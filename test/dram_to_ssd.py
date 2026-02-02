#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import mmap
import argparse
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
DEFAULT_DELETE_TEMP = True


def human_size(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    i = 0
    while x >= 1024 and i < len(units) - 1:
        x /= 1024.0
        i += 1
    return f"{x:.2f} {units[i]}"


def ensure_parent_dir(path: str):
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)


def open_for_direct_write(path: str, direct: bool):
    """
    返回 (fd, actually_direct)
    direct=True 时尝试 O_DIRECT，失败则退回普通写
    """
    ensure_parent_dir(path)
    if os.path.exists(path):
        os.remove(path)

    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if direct and hasattr(os, "O_DIRECT"):
        try:
            fd = os.open(path, flags | os.O_DIRECT, 0o644)
            return fd, True
        except OSError:
            pass

    fd = os.open(path, flags, 0o644)
    return fd, False


def fill_random_to_mmap(buf: mmap.mmap, size_bytes: int, chunk_size: int):
    """
    用 os.urandom 分块填充 mmap（生成时间不计入写入测速）
    """
    mv = memoryview(buf)
    off = 0
    try:
        while off < size_bytes:
            end = min(off + chunk_size, size_bytes)
            mv[off:end] = os.urandom(end - off)
            off = end
    finally:
        mv.release()


def direct_seq_write(fd: int, buf: mmap.mmap, size_bytes: int, chunk_size: int):
    """
    O_DIRECT 写：用 writev 写入 mmap buffer 的分片，尽量减少 Python 拷贝
    """
    mv = memoryview(buf)
    off = 0
    try:
        while off < size_bytes:
            end = min(off + chunk_size, size_bytes)

            # O_DIRECT 通常要求写入长度 4KB 对齐；默认 size_bytes 是 MB 级，天然对齐
            # 仍做保守处理：如果最后不是 4KB 对齐，向下取整
            if hasattr(os, "O_DIRECT"):
                align = 4096
                length = end - off
                if length % align != 0:
                    end = off + (length // align) * align
                    if end == off:
                        break

            os.writev(fd, [mv[off:end]])
            off = end
    finally:
        mv.release()


def buffered_seq_write(fd: int, buf: mmap.mmap, size_bytes: int, chunk_size: int):
    """
    普通写（非 O_DIRECT 兜底）
    """
    mv = memoryview(buf)
    off = 0
    try:
        while off < size_bytes:
            end = min(off + chunk_size, size_bytes)
            os.write(fd, mv[off:end])
            off = end
    finally:
        mv.release()


def test_write_speed(path: str, size_bytes: int, chunk_mb: int, direct: bool):
    chunk_size = chunk_mb * 1024 * 1024

    # mmap 是页对齐的，适合 O_DIRECT
    buf = mmap.mmap(-1, size_bytes, access=mmap.ACCESS_WRITE)
    try:
        # 填充随机数据（不计入写入耗时）
        fill_random_to_mmap(buf, size_bytes, chunk_size)

        fd, actually_direct = open_for_direct_write(path, direct=direct)
        try:
            start = time.perf_counter()
            if actually_direct:
                direct_seq_write(fd, buf, size_bytes, chunk_size)
            else:
                buffered_seq_write(fd, buf, size_bytes, chunk_size)

            os.fsync(fd)  # 强制落盘
            end = time.perf_counter()
        finally:
            os.close(fd)

        elapsed = end - start
        return elapsed, actually_direct
    finally:
        buf.close()


def main():
    ap = argparse.ArgumentParser(description="DRAM -> SSD 写入吞吐测试（尽量接近真实落盘速度）")
    ap.add_argument("--file", default=DEFAULT_TEST_FILE, help="测试文件路径")
    ap.add_argument("--min-mb", type=int, default=DEFAULT_MIN_MB, help="最小测试大小（MB）")
    ap.add_argument("--max-mb", type=int, default=DEFAULT_MAX_MB, help="最大测试大小（MB）")
    ap.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS, help="每个大小重复次数取平均")
    ap.add_argument("--chunk-mb", type=int, default=DEFAULT_CHUNK_MB, help="每次写入块大小（MB，建议 >=8）")
    ap.add_argument("--no-direct", action="store_true", help="禁用 O_DIRECT（不推荐）")
    ap.add_argument("--keep-temp", action="store_true", help="保留临时文件（默认删除）")
    args = ap.parse_args()

    direct = not args.no_direct
    delete_temp = not args.keep_temp

    log_name = f"ssd_write_speed_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    print(f"===== DRAM -> SSD 写入测速开始 =====")
    print(f"测试文件: {args.file}")
    print(f"范围: {args.min_mb}MB -> {args.max_mb}MB | rounds={args.rounds} | chunk={args.chunk_mb}MB")
    print(f"O_DIRECT: {'ON' if direct else 'OFF'}")
    print(f"日志文件: {log_name}\n")

    with open(log_name, "w", encoding="utf-8") as lf:
        lf.write(f"DRAM -> SSD Write Test Log  {datetime.now().isoformat()}\n")
        lf.write("=" * 100 + "\n")
        lf.write("size\tavg_s\tMBps\tGBps\tdirect_used\n")
        lf.write("=" * 100 + "\n")

    cur = args.min_mb * 1024 * 1024
    max_bytes = args.max_mb * 1024 * 1024

    try:
        while cur <= max_bytes:
            print(f"准备写入大小: {human_size(cur)} ...")

            total_t = 0.0
            direct_used = False

            for i in range(args.rounds):
                t, used_direct = test_write_speed(
                    args.file,
                    size_bytes=cur,
                    chunk_mb=args.chunk_mb,
                    direct=direct,
                )
                total_t += t
                direct_used = used_direct
                print(f"  第{i+1}次: {t:.4f}s")

            avg_t = total_t / args.rounds
            mbps = (cur / 1024 / 1024) / avg_t
            gbps = (cur / 1024 / 1024 / 1024) / avg_t

            print(f"✅ {human_size(cur)} | avg={avg_t:.4f}s | {mbps:.2f} MB/s ({gbps:.4f} GB/s) | O_DIRECT_used={direct_used}\n")

            with open(log_name, "a", encoding="utf-8") as lf:
                lf.write(f"{human_size(cur)}\t{avg_t:.6f}\t{mbps:.2f}\t{gbps:.6f}\t{direct_used}\n")

            cur *= 2

    finally:
        if delete_temp and os.path.exists(args.file):
            os.remove(args.file)
            print(f"🗑️ 已删除临时测试文件: {args.file}")

    print(f"\n===== DRAM -> SSD 写入测速结束 =====")
    print(f"日志: {log_name}")


if __name__ == "__main__":
    import sys
    if sys.version_info < (3, 8):
        print("❌ 建议使用 Python 3.8+（最好 3.9+）")
        sys.exit(1)
    main()

