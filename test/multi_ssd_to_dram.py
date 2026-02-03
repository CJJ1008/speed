#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import mmap
import zlib
import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

ALIGN = 4096               # O_DIRECT 常见对齐要求
CHECK_SAMPLE = 64 * 1024   # 每次只抽样 64KB 做校验，避免 CPU 成瓶颈


def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    i = 0
    while x >= 1024 and i < len(units) - 1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"


def round_down(n: int, align: int = ALIGN) -> int:
    return (n // align) * align


def get_mount_device(path: str) -> str:
    """用 df -P 获取目录所在块设备（确认是不是不同 NVMe）"""
    try:
        out = subprocess.check_output(["df", "-P", path], text=True).strip().splitlines()
        if len(out) >= 2:
            return out[-1].split()[0]
    except Exception:
        pass
    return "UNKNOWN"


def try_drop_caches() -> bool:
    """尽量清 page cache（需要 root 且容器允许）"""
    try:
        subprocess.run(["sync"], check=False)
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        return True
    except Exception:
        return False


def open_for_direct(path: str, flags: int, direct: bool):
    """返回 (fd, used_direct)"""
    if direct and hasattr(os, "O_DIRECT"):
        try:
            fd = os.open(path, flags | os.O_DIRECT, 0o644)
            return fd, True
        except OSError:
            pass
    fd = os.open(path, flags, 0o644)
    return fd, False


def safe_unlink(path: str):
    try:
        os.remove(path)
    except Exception:
        pass


def write_full_file(path: str, size_bytes: int, chunk_size: int, direct: bool, require_direct: bool) -> bool:
    """
    生成“真落盘”的测试文件：写满数据 + fsync
    返回 used_direct
    """
    safe_unlink(path)

    fd, used_direct = open_for_direct(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, direct=direct)
    if require_direct and not used_direct:
        os.close(fd)
        raise RuntimeError(f"[prepare] O_DIRECT 打不开: {path}")

    # mmap(-1, chunk_size) 得到页对齐 buffer
    buf = mmap.mmap(-1, chunk_size, access=mmap.ACCESS_WRITE)
    mv = memoryview(buf)

    # 填一次随机块，重复写（内容不重要，关键是真写盘）
    mv[:] = os.urandom(chunk_size)

    written = 0
    try:
        while written < size_bytes:
            to_write = min(chunk_size, size_bytes - written)
            if used_direct and (to_write % ALIGN != 0):
                to_write = round_down(to_write, ALIGN)
                if to_write == 0:
                    break

            view = mv[:to_write]
            os.writev(fd, [view])
            view.release()
            del view

            written += to_write

        os.fsync(fd)
    finally:
        mv.release()
        buf.close()
        os.close(fd)

    return used_direct


def read_full_file(path: str, size_bytes: int, chunk_size: int, direct: bool, require_direct: bool) -> dict:
    """
    顺序读全文件：读到 mmap buffer，不保存；抽样做 adler32 校验
    返回 {time, checksum, used_direct, bytes_read}
    """
    fd, used_direct = open_for_direct(path, os.O_RDONLY, direct=direct)
    if require_direct and not used_direct:
        os.close(fd)
        raise RuntimeError(f"[read] O_DIRECT 打不开: {path}")

    buf = mmap.mmap(-1, chunk_size, access=mmap.ACCESS_WRITE)
    mv = memoryview(buf)

    checksum = 1
    remaining = size_bytes
    bytes_read = 0

    start = time.perf_counter()
    try:
        while remaining > 0:
            to_read = min(chunk_size, remaining)
            if used_direct and (to_read % ALIGN != 0):
                to_read = round_down(to_read, ALIGN)
                if to_read == 0:
                    break

            view = mv[:to_read]
            n = os.readv(fd, [view])
            view.release()
            del view

            if n <= 0:
                break

            bytes_read += n

            # 抽样前 64KB：用 bytes(...) 复制出来，确保不留任何指向 mmap 的 view
            k = min(n, CHECK_SAMPLE)
            sample_bytes = bytes(mv[:k])
            checksum = zlib.adler32(sample_bytes, checksum)

            remaining -= n
    finally:
        end = time.perf_counter()
        mv.release()
        buf.close()
        os.close(fd)

    return {"time": end - start, "checksum": checksum, "used_direct": used_direct, "bytes_read": bytes_read}


def build_file_paths(dirs: list[str], prefix: str) -> list[str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return [os.path.join(d, f"{prefix}_part{i}_{ts}.bin") for i, d in enumerate(dirs)]


def main():
    ap = argparse.ArgumentParser(description="多盘并发 SSD->DRAM 读带宽测试（分片/多NVMe并发，带宽叠加）")
    ap.add_argument("--dirs", nargs="+", required=True,
                    help="每块 NVMe 对应一个已挂载目录（例如 /mnt/nvme0 /mnt/nvme1 ...）")
    ap.add_argument("--min-total-gb", type=float, default=1.0, help="最小总数据量（GB，跨所有盘求和）")
    ap.add_argument("--max-total-gb", type=float, default=8.0, help="最大总数据量（GB，跨所有盘求和）")
    ap.add_argument("--rounds", type=int, default=3, help="每个大小重复读次数取平均")
    ap.add_argument("--chunk-mb", type=int, default=16, help="读写块大小（MB，建议 8/16/32）")
    ap.add_argument("--no-direct", action="store_true", help="禁用 O_DIRECT（不推荐）")
    ap.add_argument("--require-direct", action="store_true", help="必须使用 O_DIRECT，否则报错退出")
    ap.add_argument("--drop-caches", action="store_true", help="每轮读前尝试 drop_caches（一般 O_DIRECT 不需要）")
    ap.add_argument("--keep-files", action="store_true", help="保留测试文件（默认删除）")
    args = ap.parse_args()

    dirs = [os.path.abspath(d) for d in args.dirs]
    for d in dirs:
        os.makedirs(d, exist_ok=True)

    n = len(dirs)
    direct = not args.no_direct
    chunk_size = args.chunk_mb * 1024 * 1024

    if chunk_size % ALIGN != 0:
        raise ValueError("chunk-mb 必须使 chunk_size 为 4096 的倍数（例如 8/16/32MB）")

    print("===== 多盘 SSD->DRAM 读带宽测试 =====")
    print(f"目标盘数: {n}")
    for d in dirs:
        print(f"  {d}  ->  {get_mount_device(d)}")
    print(f"O_DIRECT: {'ON' if direct else 'OFF'} | require_direct: {args.require_direct}")
    print(f"chunk: {args.chunk_mb} MB | rounds: {args.rounds}")
    print(f"total size: {args.min_total_gb} GB -> {args.max_total_gb} GB (doubling)")
    print(f"drop_caches_each_round: {'ON' if args.drop_caches else 'OFF'}")
    print("====================================\n")

    log_name = f"multi_ssd_to_dram_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    with open(log_name, "w", encoding="utf-8") as lf:
        lf.write(f"multi SSD->DRAM read test  {datetime.now().isoformat()}\n")
        for d in dirs:
            lf.write(f"dir: {d} -> {get_mount_device(d)}\n")
        lf.write("=" * 140 + "\n")
        lf.write("total_size\tper_disk_size\tavg_wall_s\tagg_GBps\tagg_MBps\tper_disk_MBps_list\tdirect_used\tchecksum_sum\tbytes_read\n")

    total_min = int(args.min_total_gb * (1024**3))
    total_max = int(args.max_total_gb * (1024**3))
    total = total_min

    files = []
    try:
        while total <= total_max:
            per = round_down(total // n, ALIGN)
            real_total = per * n
            if per <= 0:
                raise RuntimeError("总大小太小，分片后 per_disk_size=0")

            print(f"== 准备数据：total={human_size(real_total)}  per_disk={human_size(per)} ==")
            files = build_file_paths(dirs, prefix="read_test")

            # 并发写满文件（准备阶段）
            with ThreadPoolExecutor(max_workers=n) as ex:
                futs = [ex.submit(write_full_file, files[i], per, chunk_size, direct, args.require_direct) for i in range(n)]
                used_direct_list = [f.result() for f in as_completed(futs)]

            used_direct_prepare = all(used_direct_list) if used_direct_list else False
            print(f"准备完成（写满并 fsync）。O_DIRECT_used_in_prepare={used_direct_prepare}\n")

            # 并发读测速：按轮统计 wall time（带宽叠加）
            wall_sum = 0.0
            last_per_disk_mb = None
            direct_used = True
            checksum_sum = 0
            bytes_read_sum = 0

            for r in range(args.rounds):
                if args.drop_caches:
                    ok = try_drop_caches()
                    print(f"[round {r+1}] drop_caches: {'OK' if ok else 'FAIL/IGNORED'}")

                start_round = time.perf_counter()
                per_disk_stats = [None] * n

                with ThreadPoolExecutor(max_workers=n) as ex:
                    fut_map = {}
                    for i in range(n):
                        fut = ex.submit(read_full_file, files[i], per, chunk_size, direct, args.require_direct)
                        fut_map[fut] = i

                    for fut in as_completed(fut_map):
                        i = fut_map[fut]
                        per_disk_stats[i] = fut.result()

                end_round = time.perf_counter()
                wall = end_round - start_round
                wall_sum += wall

                per_disk_mb = []
                checks = 0
                bytes_read = 0
                for st in per_disk_stats:
                    per_disk_mb.append((per / 1024 / 1024) / st["time"])
                    checks += int(st["checksum"])
                    bytes_read += int(st["bytes_read"])
                    direct_used = direct_used and bool(st["used_direct"])

                last_per_disk_mb = per_disk_mb
                checksum_sum = checks
                bytes_read_sum = bytes_read

                agg_mb = (real_total / 1024 / 1024) / wall
                agg_gb = (real_total / 1024 / 1024 / 1024) / wall

                print(f"[round {r+1}] wall={wall:.4f}s  AGG={agg_mb:.2f} MB/s ({agg_gb:.4f} GB/s) "
                      f"| per-disk MB/s: " + ", ".join(f"{x:.1f}" for x in per_disk_mb))

            avg_wall = wall_sum / args.rounds
            agg_mb_avg = (real_total / 1024 / 1024) / avg_wall
            agg_gb_avg = (real_total / 1024 / 1024 / 1024) / avg_wall

            print(f"\n✅ RESULT total={human_size(real_total)} per_disk={human_size(per)} "
                  f"| avg_wall={avg_wall:.4f}s | AGG={agg_mb_avg:.2f} MB/s ({agg_gb_avg:.4f} GB/s) "
                  f"| direct_used={direct_used}\n")

            with open(log_name, "a", encoding="utf-8") as lf:
                lf.write(
                    f"{human_size(real_total)}\t{human_size(per)}\t{avg_wall:.6f}\t{agg_gb_avg:.6f}\t{agg_mb_avg:.2f}\t"
                    f"{','.join(f'{x:.1f}' for x in (last_per_disk_mb or []))}\t{direct_used}\t{checksum_sum}\t{bytes_read_sum}\n"
                )

            if not args.keep_files:
                for p in files:
                    safe_unlink(p)
                print("🗑️ 已删除本轮测试文件（释放空间）\n")

            total *= 2

    except Exception as e:
        print(f"\n❌ 发生异常: {repr(e)}")
        print("提示：如果脚本异常退出，可能留下 read_test_part*.bin，可以手动 rm 掉。")
        raise

    finally:
        if (not args.keep_files) and files:
            for p in files:
                safe_unlink(p)
        print(f"日志文件: {log_name}")


if __name__ == "__main__":
    main()

