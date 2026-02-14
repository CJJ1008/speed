# Multi-path GDS Benchmark - Enhanced Version

## 新增功能

### 1. 文件生成功能
自动生成1GB到8GB的测试文件，用于后续benchmark测试。

### 2. 批量Benchmark套件
自动对1GB到8GB的所有文件运行benchmark测试，并生成汇总报告。

### 3. 美化日志输出
- 使用ANSI颜色代码增强可读性
- 分层级的标题和分隔线
- 表格化的汇总结果
- 实时进度显示
- 清晰的状态指示（✓/✗）

## 编译

```bash
CUDA=/usr/local/cuda-12.9
$CUDA/bin/nvcc -O3 -std=c++17 multipath_gds.cu -o multipath_gds \
  -DWITH_CUFILE \
  -I$CUDA/targets/x86_64-linux/include \
  -L$CUDA/targets/x86_64-linux/lib \
  -lcufile -lpthread -ldl -lrt -Wno-deprecated-gpu-targets
```

## 使用方法

### 步骤1: 生成测试文件

```bash
export LD_LIBRARY_PATH=/usr/local/cuda-12.9/targets/x86_64-linux/lib:$LD_LIBRARY_PATH
./multipath_gds --generate --output-dir /data/gds_test
```

这将生成：
- test_1GB.bin (1 GB)
- test_2GB.bin (2 GB)
- test_3GB.bin (3 GB)
- ...
- test_8GB.bin (8 GB)

### 步骤2: 运行Benchmark套件

#### GDS模式测试
```bash
./multipath_gds --benchmark-suite \
  --input-dir /data/gds_test \
  --target 2 \
  --chunk 8M \
  --iters 1 \
  --use-gds 1 \
  --bufreg 1 \
  --odirect 1 \
  --verbose 1
```

#### POSIX模式测试（对比）
```bash
./multipath_gds --benchmark-suite \
  --input-dir /data/gds_test \
  --target 2 \
  --chunk 8M \
  --iters 1 \
  --use-gds 0 \
  --odirect 1 \
  --verbose 1
```

### 步骤3: 查看结果

程序会输出：
1. **每个文件的测试结果**
   - 文件大小
   - 传输时间
   - 带宽（GiB/s）
   - 状态（成功/失败）

2. **汇总统计表格**
   ```
   File Size       Filename                 Data Moved      Time (s)    Bandwidth          Status
   ------------------------------------------------------------------------------------------------
   1.00 GB         test_1GB.bin            1.00 GB         0.123       8.13 GiB/s         ✓ OK
   2.00 GB         test_2GB.bin            2.00 GB         0.245       8.16 GiB/s         ✓ OK
   ...
   ```

3. **整体性能指标**
   - 总数据传输量
   - 总耗时
   - 平均带宽
   - 错误计数

## 单文件测试（原始模式）

仍然支持单文件测试：

```bash
./multipath_gds --file /data/gds_test/test_4GB.bin \
  --target 2 \
  --chunk 8M \
  --iters 1 \
  --use-gds 1 \
  --bufreg 1 \
  --odirect 1 \
  --verbose 1
```

## 参数说明

### 新增参数
- `--generate`: 文件生成模式
- `--benchmark-suite`: 批量测试模式
- `--output-dir <path>`: 生成文件的输出目录（默认：/data/gds_test）
- `--input-dir <path>`: benchmark输入文件目录（默认：/data/gds_test）

### 原有参数
- `--file <path>`: 单文件路径
- `--target <GPU>`: 目标GPU ID
- `--chunk <size>`: 块大小（如8M, 16M）
- `--iters <N>`: 迭代次数
- `--use-gds <0|1>`: 使用GDS (1) 或 POSIX (0)
- `--odirect <0|1>`: 启用O_DIRECT
- `--bufreg <0|1>`: 启用buffer registration
- `--verbose <0|1>`: 详细输出
- `--helpers <GPU,GPU,...>`: 指定helper GPU列表

## 输出示例

```
================================================================================
  BENCHMARK SUITE MODE
================================================================================

Configuration:
  Input directory: /data/gds_test
  Target GPU: 2
  Chunk size: 8.00 MB
  Iterations: 1
  Mode: GDS (cuFileRead)
  O_DIRECT: Yes
  Buffer registration: Yes

>>> Testing: test_1GB.bin (1.00 GB)
  [GPU 0 HELPER] range: 0.00 B - 128.00 MB (GDS)
  [GPU 1 HELPER] range: 128.00 MB - 256.00 MB (GDS)
  [GPU 2 TARGET] range: 256.00 MB - 384.00 MB (GDS)
  ...
  ✓ Bandwidth: 8.13 GiB/s (time: 0.123s)

================================================================================
  BENCHMARK SUMMARY
================================================================================

File Size       Filename                 Data Moved      Time (s)    Bandwidth          Status
------------------------------------------------------------------------------------------------
1.00 GB         test_1GB.bin            1.00 GB         0.123       8.13 GiB/s         ✓ OK
2.00 GB         test_2GB.bin            2.00 GB         0.245       8.16 GiB/s         ✓ OK
3.00 GB         test_3GB.bin            3.00 GB         0.368       8.15 GiB/s         ✓ OK
4.00 GB         test_4GB.bin            4.00 GB         0.491       8.15 GiB/s         ✓ OK
5.00 GB         test_5GB.bin            5.00 GB         0.614       8.14 GiB/s         ✓ OK
6.00 GB         test_6GB.bin            6.00 GB         0.737       8.14 GiB/s         ✓ OK
7.00 GB         test_7GB.bin            7.00 GB         0.860       8.14 GiB/s         ✓ OK
8.00 GB         test_8GB.bin            8.00 GB         0.983       8.14 GiB/s         ✓ OK
------------------------------------------------------------------------------------------------

Overall Statistics:
  Total data moved: 36.00 GB
  Total time: 4.42 seconds
  Average bandwidth: 8.14 GiB/s
  Total errors: 0

================================================================================
```

## 性能优化建议

1. **调整chunk大小**：根据你的存储系统特性，尝试4M, 8M, 16M等不同大小
2. **使用O_DIRECT**：减少page cache开销
3. **启用buffer registration**：对于GDS模式，可以提升性能
4. **多次迭代**：使用`--iters 3`进行多次测试，获得更稳定的结果

## 故障排查

如果遇到问题：
1. 确认cuFile库路径正确设置在LD_LIBRARY_PATH中
2. 确认有足够的磁盘空间（至少36GB用于测试文件）
3. 确认GPU间支持P2P访问
4. 查看详细日志（--verbose 1）定位具体问题
