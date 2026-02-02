#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import zlib
import mmap
import argparse
import subprocess
from datetime import datetime

# =========================
# 默认配置（可用命令行覆盖）
# =========================
DEFAULT_TEST_FILE = "/jakovchen/speed/test/ssd_test_temp.bin"
DEFAULT_MIN_MB = 256
DEFAULT_MAX_MB = 1024
DEFAULT_ROUNDS = 3
DEFAULT_CHUNK_MB = 8
DEFAULT_DIRECT = True
DEFAULT_DROP_CACHES = True
DEFAULT_DELETE_TEMP = True


def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    i = 0
    while x >= 1024 and i < len(units) - 1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"


def try_drop_caches() -> bool:
    """
    尽量清掉 page cache，避免读到缓存（需要 root 且容器允许写 /proc/sys/vm/drop_caches）
    """
    try:
        subprocess.run(["sync"], check=False)
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        return True
    except Exception:
        return False


def ensure_parent_dir(path: str):
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)


def create_test_file_fast(path: str, size_bytes: int):
    """
    生成测试文件：
    - 优先用 posix_fallocate 快速预分配（避免生成随机数据耗时）
    - 再写一点点数据（防止某些环境对“全空洞”做奇怪优化）
    - fsync 确保元数据落盘
    """
    ensure_parent_dir(path)
    if os.path.exists(path):
        os.remove(path)

    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        # 预分配（更快）
        if hasattr(os, "posix_fallocate"):
            os.posix_fallocate(fd, 0, size_bytes)
        else:
            # 兜底：truncate
            os.ftruncate(fd, size_bytes)

        # 写入少量数据做“扰动”（不影响吞吐测试）
        # 注意：写入位置和长度尽量对齐 4KB
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, os.urandom(4096))
        if size_bytes >= 8192:
            os.lseek(fd, size_bytes - 4096, os.SEEK_SET)
            os.write(fd, os.urandom(4096))

        os.fsync(fd)
    finally:
        os.close(fd)


def open_for_direct_read(path: str, direct: bool):
    """
    返回 (fd, actually_direct)
    direct=True 时尝试 O_DIRECT，失败则退回普通读
    """
    flags = os.O_RDONLY
    if direct and hasattr(os, "O_DIRECT"):
        try:
            fd = os.open(path, flags | os.O_DIRECT)
            return fd, True
        except OSError:
            pass
    fd = os.open(path, flags)
    return fd, False


def direct_seq_read(fd: int, total_size: int, chunk_size: int) -> int:
    """
    用 readv + page-aligned mmap buffer 做顺序读，并用 adler32（C实现）做轻量校验，避免 CPU 成瓶颈。
    返回 checksum（用于防止编译器/解释器“优化掉读取”）
    """
    # mmap 是页对齐的（通常 4KB 对齐），适合 O_DIRECT
    buf = mmap.mmap(-1, chunk_size, access=mmap.ACCESS_WRITE)
    mv = memoryview(buf)

    checksum = 1
    remaining = total_size

    try:
        while remaining > 0:
            # O_DIRECT 通常要求 read 长度是 4KB 倍数；这里 chunk_size 本身就是对齐的
            to_read = chunk_size if remaining >= chunk_size else remaining

            # 如果最后一块不是对齐大小，O_DIRECT 可能 EINVAL。
            # 但我们生成的测试文件大小默认是 MB 级，天然 4KB 对齐，基本不会触发。
            # 这里仍做保守处理：如果不是对齐，就把 to_read 向下取整到 4KB。
            if hasattr(os, "O_DIRECT"):
                align = 4096
                if to_read % align != 0:
                    to_read = (to_read // align) * align
                    if to_read == 0:
                        break

            # os.readv 读到 mv（不会新建 bytes，减少开销）
            n = os.readv(fd, [mv[:to_read]])
            if n <= 0:
                break

            checksum = zlib.adler32(mv[:n], checksum)
            remaining -= n
    finally:
        mv.release()
        buf.close()

    return checksum


def buffered_seq_read(fd: int, total_size: int, chunk_size: int) -> int:
    """
    普通读路径（非 O_DIRECT 兜底），依然用大块 + adler32，减少 Python 开销
    """
    checksum = 1
    remaining = total_size
    while remaining > 0:
        to_read = chunk_size if remaining >= chunk_size else remaining
        data = os.read(fd, to_read)
        if not data:
            break
        checksum = zlib.adler32(data, checksum)
        remaining -= len(data)
    return checksum


def test_read_speed(path: str, chunk_mb: int, direct: bool, drop_caches: bool):
    size_bytes = os.path.getsize(path)
    chunk_size = chunk_mb * 1024 * 1024

    dropped = False
    if drop_caches:
        dropped = try_drop_caches()

    fd, actually_direct = open_for_direct_read(path, direct=direct)
    try:
        start = time.perf_counter()
        if actually_direct:
            checksum = direct_seq_read(fd, size_bytes, chunk_size)
        else:
            checksum = buffered_seq_read(fd, size_bytes, chunk_size)
        end = time.perf_counter()
    finally:
        os.close(fd)

    elapsed = end - start
    return elapsed, size_bytes, checksum, actually_direct, dropped


def main():
    ap = argparse.ArgumentParser(description="SSD -> DRAM 读取吞吐测试（尽量接近真实 DMA 路径）")
    ap.add_argument("--file", default=DEFAULT_TEST_FILE, help="测试文件路径")
    ap.add_argument("--min-mb", type=int, default=DEFAULT_MIN_MB, help="最小测试大小（MB）")
    ap.add_argument("--max-mb", type=int, default=DEFAULT_MAX_MB, help="最大测试大小（MB）")
    ap.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS, help="每个大小重复次数取平均")
    ap.add_argument("--chunk-mb", type=int, default=DEFAULT_CHUNK_MB, help="每次读取块大小（MB，建议 >=8）")
    ap.add_argument("--no-direct", action="store_true", help="禁用 O_DIRECT（不推荐，可能读到缓存）")
    ap.add_argument("--no-drop-caches", action="store_true", help="不清 page cache")
    ap.add_argument("--keep-temp", action="store_true", help="保留临时文件（默认删除）")
    args = ap.parse_args()

    direct = not args.no_direct
    drop_caches = not args.no_drop_caches
    delete_temp = not args.keep_temp

    # 生成日志
    log_name = f"ssd_read_speed_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    print(f"===== SSD -> DRAM 读取测速开始 =====")
    print(f"测试文件: {args.file}")
    print(f"范围: {args.min_mb}MB -> {args.max_mb}MB | rounds={args.rounds} | chunk={args.chunk_mb}MB")
    print(f"O_DIRECT: {'ON' if direct else 'OFF'} | drop_caches: {'ON' if drop_caches else 'OFF'}")
    print(f"日志文件: {log_name}\n")

    with open(log_name, "w", encoding="utf-8") as lf:
        lf.write(f"SSD -> DRAM Read Test Log  {datetime.now().isoformat()}\n")
        lf.write("=" * 100 + "\n")
        lf.write("size\tavg_s\tMBps\tGBps\tdirect_used\tdrop_caches_ok\n")
        lf.write("=" * 100 + "\n")

    cur = args.min_mb * 1024 * 1024
    max_bytes = args.max_mb * 1024 * 1024

    try:
        while cur <= max_bytes:
            print(f"准备测试文件大小: {human_size(cur)} ...")
            create_test_file_fast(args.file, cur)

            total_t = 0.0
            direct_used = False
            drop_ok = False
            last_checksum = None

            for i in range(args.rounds):
                t, sz, checksum, used_direct, dropped = test_read_speed(
                    args.file,
                    chunk_mb=args.chunk_mb,
                    direct=direct,
                    drop_caches=drop_caches,
                )
                total_t += t
                direct_used = used_direct
                drop_ok = dropped
                last_checksum = checksum
                print(f"  第{i+1}次: {t:.4f}s  (checksum={checksum})")

            avg_t = total_t / args.rounds
            mbps = (cur / 1024 / 1024) / avg_t
            gbps = (cur / 1024 / 1024 / 1024) / avg_t

            print(f"✅ {human_size(cur)} | avg={avg_t:.4f}s | {mbps:.2f} MB/s ({gbps:.4f} GB/s)")
            print(f"   O_DIRECT_used={direct_used} | drop_caches_ok={drop_ok} | last_checksum={last_checksum}\n")

            with open(log_name, "a", encoding="utf-8") as lf:
                lf.write(f"{human_size(cur)}\t{avg_t:.6f}\t{mbps:.2f}\t{gbps:.6f}\t{direct_used}\t{drop_ok}\n")

            cur *= 2

    finally:
        if delete_temp and os.path.exists(args.file):
            os.remove(args.file)
            print(f"🗑️ 已删除临时测试文件: {args.file}")

    print(f"\n===== SSD -> DRAM 读取测速结束 =====")
    print(f"日志: {log_name}")


if __name__ == "__main__":
    # O_DIRECT + readv 依赖较新的 Python / Linux；版本太老就会表现异常
    import sys
    if sys.version_info < (3, 8):
        print("❌ 建议使用 Python 3.8+（最好 3.9+）")
        sys.exit(1)
    main()

