# NVMe 多盘并发 SSD↔DRAM 带宽基准测试（Shard / Multi-NVMe Aggregation）

本工程用于在 **Linux** 上测量 **SSD(NVMe) ↔ CPU DRAM** 的“真实 I/O 吞吐”，并支持 **分片 + 多 NVMe 并发** 来叠加带宽（类似 RAID0/条带化思想，但在应用层并行实现），面向大模型场景下的 checkpoint / 权重 / KV 等大文件读写带宽评估与优化。

核心特点：
- 支持 **多盘并行读（SSD→DRAM）** 与 **多盘并行写（DRAM→SSD）**
- 使用 `O_DIRECT`（可选强制）尽量绕过 page cache，避免“缓存命中导致虚高”
- 总数据量按盘数 **自动分片**（每盘一个 shard 文件），并行执行，统计 **聚合带宽**
- 每个测试规模可重复多轮取平均，并输出日志

> ⚠️ 注意：本工程不会对磁盘做任何格式化/分区操作，但会在指定目录下创建大文件用于测试。请确保测试目录是“专用测试路径”，避免误删重要数据。

---

## 目录结构

- `multi_ssd_to_dram.py`：多盘并发读带宽（SSD→DRAM）
- `multi_dram_to_ssd.py`：多盘并发写带宽（DRAM→SSD）
- `*.log`：运行生成的日志文件（自动命名）

---

## 环境要求

- Linux（推荐 Ubuntu/CentOS 等常见发行版）
- Python 3.9+（工程兼容 3.12，已规避 mmap + memoryview 的关闭报错）
- NVMe SSD（或任何块设备，NVMe 最常见）
- 文件系统：ext4/xfs 更常见（O_DIRECT 支持更稳定）
- 权限：使用 `--require-direct` 时，底层必须支持 `O_DIRECT`；容器内可能需要正确的 bind-mount 路径

---

## 原理简述（为什么能“叠加带宽”）

单盘顺序读写通常上限是该盘的带宽（例如 3GB/s）。
当有多块 NVMe 时，通过：
1) 将总数据切成 N 份（每盘一个 shard）
2) 对每盘同时发起顺序读/写（并行 I/O）
3) 以“轮”的 wall time 统计总吞吐  
就能得到近似的聚合带宽：`BW_total ≈ sum(BW_disk_i)`（实际会受 PCIe 拓扑、NUMA、CPU、文件系统等限制）。

---

## 快速开始（宿主机 + Docker 常驻容器）

下面流程适合“多 NVMe + 容器测试”。

### 1) 宿主机查看 NVMe/挂载情况

```bash
lsblk -d -o NAME,TYPE,ROTA,SIZE,MODEL,TRAN
lsblk -f | grep -E '^nvme|^NAME'
findmnt -t ext4,xfs -o TARGET,SOURCE,FSTYPE,SIZE,OPTIONS

