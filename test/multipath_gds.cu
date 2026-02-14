// multipath_gds_enhanced.cu
// Enhanced Multi-path GDS benchmark with file generation and batch testing
//
// Build (WITH_CUFILE):
//   CUDA=/usr/local/cuda-12.9
//   $CUDA/bin/nvcc -O3 -std=c++17 multipath_gds_enhanced.cu -o multipath_gds_enhanced \
//     -DWITH_CUFILE \
//     -I$CUDA/targets/x86_64-linux/include \
//     -L$CUDA/targets/x86_64-linux/lib \
//     -lcufile -lpthread -ldl -lrt -Wno-deprecated-gpu-targets
//
// Run examples:
//   # Generate test files
//   ./multipath_gds_enhanced --generate --output-dir /data/gds_test
//
//   # Run benchmark suite
//   ./multipath_gds_enhanced --benchmark-suite --input-dir /data/gds_test --target 2 --use-gds 1
//
//   # Single file test (original mode)
//   ./multipath_gds_enhanced --file /data/gds_test/test_4GB.bin --target 2 --chunk 8M --iters 1 --use-gds 1

#include <cuda_runtime.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

#ifdef WITH_CUFILE
  #include <cufile.h>
#endif

// ANSI color codes
#define COLOR_RESET   "\033[0m"
#define COLOR_BOLD    "\033[1m"
#define COLOR_RED     "\033[31m"
#define COLOR_GREEN   "\033[32m"
#define COLOR_YELLOW  "\033[33m"
#define COLOR_BLUE    "\033[34m"
#define COLOR_MAGENTA "\033[35m"
#define COLOR_CYAN    "\033[36m"
#define COLOR_WHITE   "\033[37m"

static inline void die(const std::string& msg) {
  std::cerr << COLOR_RED << "[FATAL] " << msg << COLOR_RESET << std::endl;
  std::exit(1);
}
static inline std::string cudaErrStr(cudaError_t e) { return std::string(cudaGetErrorString(e)); }

static inline size_t align_up(size_t x, size_t a) { return (x + a - 1) / a * a; }
static inline size_t align_down(size_t x, size_t a) { return (x / a) * a; }

static size_t parse_size(const std::string& s) {
  if (s.empty()) die("empty size string");
  double val = 0.0;
  char unit = 0;
  int m = std::sscanf(s.c_str(), "%lf%c", &val, &unit);
  if (m <= 0) die("bad size: " + s);
  uint64_t mul = 1;
  if (m == 2) {
    char u = (char)std::tolower(unit);
    if (u == 'k') mul = 1024ULL;
    else if (u == 'm') mul = 1024ULL * 1024ULL;
    else if (u == 'g') mul = 1024ULL * 1024ULL * 1024ULL;
    else die("unknown suffix in size: " + s);
  }
  if (val < 0) die("negative size: " + s);
  return (size_t)(val * (double)mul);
}

static std::string format_size(size_t bytes) {
  std::ostringstream oss;
  if (bytes >= 1024ULL * 1024 * 1024) {
    oss << std::fixed << std::setprecision(2) << (double)bytes / (1024.0 * 1024.0 * 1024.0) << " GB";
  } else if (bytes >= 1024ULL * 1024) {
    oss << std::fixed << std::setprecision(2) << (double)bytes / (1024.0 * 1024.0) << " MB";
  } else if (bytes >= 1024ULL) {
    oss << std::fixed << std::setprecision(2) << (double)bytes / 1024.0 << " KB";
  } else {
    oss << bytes << " B";
  }
  return oss.str();
}

static std::string format_bandwidth(double gbps) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2) << gbps << " GiB/s";
  return oss.str();
}

static void print_separator(char c = '=', int width = 100) {
  std::cout << COLOR_CYAN << std::string(width, c) << COLOR_RESET << std::endl;
}

static void print_header(const std::string& title) {
  print_separator('=');
  std::cout << COLOR_BOLD << COLOR_YELLOW << "  " << title << COLOR_RESET << std::endl;
  print_separator('=');
}

static void print_subheader(const std::string& title) {
  std::cout << COLOR_BOLD << COLOR_BLUE << ">>> " << title << COLOR_RESET << std::endl;
}

struct Args {
  std::string file;
  size_t offset = 0;
  size_t bytes  = 0;
  size_t chunk  = 8ull * 1024 * 1024;
  int target    = 0;
  std::vector<int> helpers;
  int iters     = 1;

  int use_gds   = 0;
  int odirect   = 0;
  int bufreg    = 0;
  int verbose   = 1;
  
  // New options
  bool generate = false;
  bool benchmark_suite = false;
  std::string output_dir = "/data/gds_test";
  std::string input_dir = "/data/gds_test";
};

static Args parse_args(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; i++) {
    std::string k = argv[i];
    auto need = [&](const char* name) -> std::string {
      if (i + 1 >= argc) die(std::string("missing value for ") + name);
      return std::string(argv[++i]);
    };

    if (k == "--file") a.file = need("--file");
    else if (k == "--offset") a.offset = parse_size(need("--offset"));
    else if (k == "--bytes")  a.bytes  = parse_size(need("--bytes"));
    else if (k == "--chunk")  a.chunk  = parse_size(need("--chunk"));
    else if (k == "--target") a.target = std::stoi(need("--target"));
    else if (k == "--iters")  a.iters  = std::stoi(need("--iters"));
    else if (k == "--use-gds") a.use_gds = std::stoi(need("--use-gds"));
    else if (k == "--odirect") a.odirect = std::stoi(need("--odirect"));
    else if (k == "--bufreg")  a.bufreg  = std::stoi(need("--bufreg"));
    else if (k == "--verbose") a.verbose = std::stoi(need("--verbose"));
    else if (k == "--generate") a.generate = true;
    else if (k == "--benchmark-suite") a.benchmark_suite = true;
    else if (k == "--output-dir") a.output_dir = need("--output-dir");
    else if (k == "--input-dir") a.input_dir = need("--input-dir");
    else if (k == "--helpers") {
      std::string v = need("--helpers");
      a.helpers.clear();
      size_t pos = 0;
      while (pos < v.size()) {
        size_t comma = v.find(',', pos);
        std::string tok = (comma == std::string::npos) ? v.substr(pos) : v.substr(pos, comma - pos);
        if (!tok.empty()) a.helpers.push_back(std::stoi(tok));
        if (comma == std::string::npos) break;
        pos = comma + 1;
      }
    } else {
      die("unknown arg: " + k);
    }
  }
  
  if (!a.generate && !a.benchmark_suite && a.file.empty()) {
    die("need --file <path> OR --generate OR --benchmark-suite");
  }
  if (a.chunk == 0) die("--chunk must be > 0");
  if (a.iters <= 0) die("--iters must be >= 1");
  if (a.use_gds != 0 && a.use_gds != 1) die("--use-gds must be 0/1");
  if (a.odirect != 0 && a.odirect != 1) die("--odirect must be 0/1");
  if (a.bufreg  != 0 && a.bufreg  != 1) die("--bufreg must be 0/1");
  return a;
}

struct ThreadStats {
  double seconds = 0.0;
  uint64_t bytes = 0;
  int errors = 0;
};

static void print0(std::mutex& mu, const std::string& s) {
  std::lock_guard<std::mutex> lk(mu);
  std::cerr << s << std::endl;
}

struct ThreadCtx {
  std::string file;
  int reader_dev = -1;
  int target_dev = -1;

  size_t global_offset = 0;
  size_t file_off = 0;
  size_t bytes = 0;

  size_t chunk = 0;
  int iters = 1;
  int use_gds = 0;
  int odirect = 0;
  int bufreg = 0;
  int verbose = 1;

  void* target_buf = nullptr;
  ThreadStats* stats = nullptr;
  std::mutex* print_mu = nullptr;
};

static void worker(ThreadCtx ctx) {
  auto t0 = std::chrono::high_resolution_clock::now();
  uint64_t moved = 0;

  const bool is_target_reader = (ctx.reader_dev == ctx.target_dev);

  int flags = O_RDONLY;
  if (ctx.odirect) flags |= O_DIRECT;

  int fd = ::open(ctx.file.c_str(), flags);
  if (fd < 0) {
    ctx.stats->errors++;
    print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] open failed: " + std::string(strerror(errno)) + COLOR_RESET);
    return;
  }

#ifdef WITH_CUFILE
  CUfileHandle_t cf_handle = nullptr;
  bool gds_ready = false;
  if (ctx.use_gds) {
    CUfileDescr_t desc;
    std::memset(&desc, 0, sizeof(desc));
    desc.handle.fd = fd;
    desc.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
    CUfileError_t st = cuFileHandleRegister(&cf_handle, &desc);
    if (st.err != CU_FILE_SUCCESS) {
      ctx.stats->errors++;
      print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] cuFileHandleRegister failed err=" + std::to_string(st.err) + COLOR_RESET);
      ::close(fd);
      return;
    }
    gds_ready = true;
  }
#else
  if (ctx.use_gds) {
    ctx.stats->errors++;
    print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] built without WITH_CUFILE" + COLOR_RESET);
    ::close(fd);
    return;
  }
#endif

  cudaError_t ce;

  size_t dst_off0 = ctx.file_off - ctx.global_offset;
  void* target_slice_base = (void*)((char*)ctx.target_buf + dst_off0);

  void* helper_buf = nullptr;

  if (!is_target_reader) {
    ce = cudaSetDevice(ctx.reader_dev);
    if (ce != cudaSuccess) die("cudaSetDevice(reader) failed: " + cudaErrStr(ce));
    ce = cudaMalloc(&helper_buf, ctx.chunk);
    if (ce != cudaSuccess) die("cudaMalloc(helper_buf) failed: " + cudaErrStr(ce));
  }

#ifdef WITH_CUFILE
  if (ctx.use_gds && ctx.bufreg) {
    if (is_target_reader) {
      ce = cudaSetDevice(ctx.target_dev);
      if (ce != cudaSuccess) die("cudaSetDevice(target) failed: " + cudaErrStr(ce));
      CUfileError_t st = cuFileBufRegister(target_slice_base, ctx.bytes, 0);
      if (st.err != CU_FILE_SUCCESS) {
        ctx.stats->errors++;
        print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] cuFileBufRegister failed" + COLOR_RESET);
        if (helper_buf) { cudaSetDevice(ctx.reader_dev); cudaFree(helper_buf); }
        cuFileHandleDeregister(cf_handle);
        ::close(fd);
        return;
      }
    } else {
      ce = cudaSetDevice(ctx.reader_dev);
      if (ce != cudaSuccess) die("cudaSetDevice(reader) failed: " + cudaErrStr(ce));
      CUfileError_t st = cuFileBufRegister(helper_buf, ctx.chunk, 0);
      if (st.err != CU_FILE_SUCCESS) {
        ctx.stats->errors++;
        print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] cuFileBufRegister failed" + COLOR_RESET);
        cudaFree(helper_buf);
        cuFileHandleDeregister(cf_handle);
        ::close(fd);
        return;
      }
    }
  }
#endif

  void* host_buf = nullptr;
  if (!ctx.use_gds) {
    size_t al = 4096;
    if (posix_memalign(&host_buf, al, align_up(ctx.chunk, al)) != 0) die("posix_memalign(host_buf) failed");
  }

  cudaStream_t tstream = nullptr;
  if (!is_target_reader) {
    ce = cudaSetDevice(ctx.target_dev);
    if (ce != cudaSuccess) die("cudaSetDevice(target) failed: " + cudaErrStr(ce));
    ce = cudaStreamCreateWithFlags(&tstream, cudaStreamNonBlocking);
    if (ce != cudaSuccess) die("cudaStreamCreate failed: " + cudaErrStr(ce));
  }

  if (ctx.verbose) {
    std::string role = is_target_reader ? COLOR_GREEN "TARGET" COLOR_RESET : COLOR_YELLOW "HELPER" COLOR_RESET;
    print0(*ctx.print_mu,
           "  [GPU " + std::to_string(ctx.reader_dev) + " " + role + "] range: " +
           format_size(ctx.file_off) + " - " + format_size(ctx.file_off + ctx.bytes) +
           " (" + (ctx.use_gds ? COLOR_CYAN "GDS" COLOR_RESET : COLOR_MAGENTA "POSIX" COLOR_RESET) + ")");
  }

  for (int it = 0; it < ctx.iters; it++) {
    size_t off = ctx.file_off;
    size_t rem = ctx.bytes;

    while (rem > 0) {
      size_t n = std::min(ctx.chunk, rem);

      if (ctx.use_gds) {
#ifdef WITH_CUFILE
        if (is_target_reader) {
          ce = cudaSetDevice(ctx.target_dev);
          if (ce != cudaSuccess) die("cudaSetDevice(target) failed: " + cudaErrStr(ce));
          void* dst_ptr = (void*)((char*)ctx.target_buf + (off - ctx.global_offset));
          ssize_t r = cuFileRead(cf_handle, dst_ptr, n, (off_t)off, 0);
          if (r < 0 || (size_t)r != n) {
            ctx.stats->errors++;
            print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] cuFileRead failed" + COLOR_RESET);
            break;
          }
        } else {
          ce = cudaSetDevice(ctx.reader_dev);
          if (ce != cudaSuccess) die("cudaSetDevice(reader) failed: " + cudaErrStr(ce));
          ssize_t r = cuFileRead(cf_handle, helper_buf, n, (off_t)off, 0);
          if (r < 0 || (size_t)r != n) {
            ctx.stats->errors++;
            print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] cuFileRead failed" + COLOR_RESET);
            break;
          }
        }
#endif
      } else {
        ssize_t r = pread(fd, host_buf, n, (off_t)off);
        if (r < 0 || (size_t)r != n) {
          ctx.stats->errors++;
          print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] pread failed" + COLOR_RESET);
          break;
        }

        if (is_target_reader) {
          ce = cudaSetDevice(ctx.target_dev);
          if (ce != cudaSuccess) die("cudaSetDevice(target) failed: " + cudaErrStr(ce));
          void* dst_ptr = (void*)((char*)ctx.target_buf + (off - ctx.global_offset));
          ce = cudaMemcpy(dst_ptr, host_buf, n, cudaMemcpyHostToDevice);
          if (ce != cudaSuccess) die("cudaMemcpy(H2D target) failed: " + cudaErrStr(ce));
        } else {
          ce = cudaSetDevice(ctx.reader_dev);
          if (ce != cudaSuccess) die("cudaSetDevice(reader) failed: " + cudaErrStr(ce));
          ce = cudaMemcpy(helper_buf, host_buf, n, cudaMemcpyHostToDevice);
          if (ce != cudaSuccess) die("cudaMemcpy(H2D helper) failed: " + cudaErrStr(ce));
        }
      }

      if (!is_target_reader) {
        ce = cudaSetDevice(ctx.target_dev);
        if (ce != cudaSuccess) die("cudaSetDevice(target) failed: " + cudaErrStr(ce));
        void* dst_ptr = (void*)((char*)ctx.target_buf + (off - ctx.global_offset));

        cudaError_t pe = cudaMemcpyPeerAsync(dst_ptr, ctx.target_dev, helper_buf, ctx.reader_dev, n, tstream);
        if (pe != cudaSuccess) {
          ctx.stats->errors++;
          print0(*ctx.print_mu, COLOR_RED "[reader " + std::to_string(ctx.reader_dev) + "] cudaMemcpyPeerAsync failed" + COLOR_RESET);
          break;
        }
        ce = cudaStreamSynchronize(tstream);
        if (ce != cudaSuccess) die("cudaStreamSynchronize failed: " + cudaErrStr(ce));
      }

      moved += n;
      off += n;
      rem -= n;
    }
  }

  if (tstream) cudaStreamDestroy(tstream);
  if (host_buf) free(host_buf);

#ifdef WITH_CUFILE
  if (ctx.use_gds && gds_ready) {
    if (ctx.bufreg) {
      if (is_target_reader) cuFileBufDeregister(target_slice_base);
      else cuFileBufDeregister(helper_buf);
    }
    cuFileHandleDeregister(cf_handle);
  }
#endif

  if (helper_buf) {
    cudaSetDevice(ctx.reader_dev);
    cudaFree(helper_buf);
  }

  ::close(fd);

  auto t1 = std::chrono::high_resolution_clock::now();
  ctx.stats->seconds = std::chrono::duration<double>(t1 - t0).count();
  ctx.stats->bytes = moved;

  if (ctx.verbose) {
    double gb = (double)moved / (1024.0 * 1024.0 * 1024.0);
    double gbps = (ctx.stats->seconds > 0) ? (gb / ctx.stats->seconds) : 0.0;
    std::string role = is_target_reader ? COLOR_GREEN "TARGET" COLOR_RESET : COLOR_YELLOW "HELPER" COLOR_RESET;
    print0(*ctx.print_mu,
           "  [GPU " + std::to_string(ctx.reader_dev) + " " + role + "] done: " +
           format_size(moved) + " in " + std::to_string(ctx.stats->seconds) + "s → " +
           COLOR_BOLD + format_bandwidth(gbps) + COLOR_RESET);
  }
}

static std::vector<int> unique_keep_order(const std::vector<int>& v) {
  std::vector<int> out;
  out.reserve(v.size());
  for (int x : v) {
    bool seen = false;
    for (int y : out) if (y == x) { seen = true; break; }
    if (!seen) out.push_back(x);
  }
  return out;
}

// Generate test file with random data
static bool generate_test_file(const std::string& path, size_t size_bytes) {
  print_subheader("Generating file: " + path + " (" + format_size(size_bytes) + ")");
  
  auto t0 = std::chrono::high_resolution_clock::now();
  
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    std::cerr << COLOR_RED << "  Failed to create file: " << strerror(errno) << COLOR_RESET << std::endl;
    return false;
  }

  const size_t CHUNK = 64 * 1024 * 1024; // 64MB chunks
  std::vector<char> buffer(CHUNK);
  
  // Fill with pseudo-random data
  for (size_t i = 0; i < buffer.size(); i++) {
    buffer[i] = (char)(i % 256);
  }

  size_t written = 0;
  while (written < size_bytes) {
    size_t to_write = std::min(CHUNK, size_bytes - written);
    ssize_t ret = ::write(fd, buffer.data(), to_write);
    if (ret < 0) {
      std::cerr << COLOR_RED << "  Write failed: " << strerror(errno) << COLOR_RESET << std::endl;
      ::close(fd);
      return false;
    }
    written += ret;
    
    if (written % (512 * 1024 * 1024) == 0) {
      std::cout << "  Progress: " << format_size(written) << " / " << format_size(size_bytes) << "\r" << std::flush;
    }
  }
  
  ::close(fd);
  
  auto t1 = std::chrono::high_resolution_clock::now();
  double elapsed = std::chrono::duration<double>(t1 - t0).count();
  double gbps = (double)size_bytes / (1024.0 * 1024.0 * 1024.0) / elapsed;
  
  std::cout << COLOR_GREEN << "  ✓ Generated " << format_size(size_bytes) 
            << " in " << std::fixed << std::setprecision(2) << elapsed << "s ("
            << format_bandwidth(gbps) << ")" << COLOR_RESET << std::endl;
  
  return true;
}

struct BenchmarkResult {
  size_t file_size;
  std::string filename;
  double wall_time;
  uint64_t bytes_moved;
  double bandwidth_gbps;
  int errors;
};

// Run single benchmark
static BenchmarkResult run_single_benchmark(const Args& args, const std::string& filepath, size_t file_size) {
  BenchmarkResult result;
  result.file_size = file_size;
  result.filename = filepath.substr(filepath.find_last_of('/') + 1);
  result.errors = 0;
  
  struct stat st;
  if (stat(filepath.c_str(), &st) != 0) {
    result.errors = 1;
    return result;
  }
  
  size_t offset = args.offset;
  size_t avail = file_size - offset;
  size_t total = (args.bytes == 0) ? avail : std::min(args.bytes, avail);
  
  const size_t BLK = 4096;
  if (args.odirect) {
    total = align_down(total, BLK);
  }
  
  cudaError_t ce = cudaSetDevice(args.target);
  if (ce != cudaSuccess) {
    result.errors = 1;
    return result;
  }
  
  void* target_buf = nullptr;
  ce = cudaMalloc(&target_buf, total);
  if (ce != cudaSuccess) {
    result.errors = 1;
    return result;
  }
  
  std::vector<int> helpers = args.helpers;
  if (helpers.empty()) {
    int ngpu = 0;
    cudaGetDeviceCount(&ngpu);
    for (int d = 0; d < ngpu; d++) helpers.push_back(d);
  }
  helpers = unique_keep_order(helpers);
  
  // Enable peer access
  for (int h : helpers) {
    if (h == args.target) continue;
    cudaSetDevice(args.target);
    cudaDeviceEnablePeerAccess(h, 0);
    cudaSetDevice(h);
    cudaDeviceEnablePeerAccess(args.target, 0);
  }
  
  size_t nreaders = helpers.size();
  size_t stripe = (total + nreaders - 1) / nreaders;
  if (args.odirect || args.use_gds) stripe = align_up(stripe, BLK);
  
  std::mutex print_mu;
  std::vector<ThreadStats> stats(nreaders);
  std::vector<std::thread> ths;
  
  auto wall0 = std::chrono::high_resolution_clock::now();
  
  for (size_t i = 0; i < nreaders; i++) {
    size_t start = offset + i * stripe;
    size_t end = std::min(offset + total, start + stripe);
    size_t bytes = (end > start) ? (end - start) : 0;
    if (args.odirect) bytes = align_down(bytes, BLK);
    if (bytes == 0) continue;
    
    ThreadCtx ctx;
    ctx.file = filepath;
    ctx.reader_dev = helpers[i];
    ctx.target_dev = args.target;
    ctx.global_offset = offset;
    ctx.file_off = start;
    ctx.bytes = bytes;
    ctx.chunk = args.chunk;
    ctx.iters = args.iters;
    ctx.use_gds = args.use_gds;
    ctx.odirect = args.odirect;
    ctx.bufreg = args.bufreg;
    ctx.verbose = args.verbose;
    ctx.target_buf = target_buf;
    ctx.stats = &stats[i];
    ctx.print_mu = &print_mu;
    
    ths.emplace_back(worker, ctx);
  }
  
  for (auto& t : ths) t.join();
  
  auto wall1 = std::chrono::high_resolution_clock::now();
  result.wall_time = std::chrono::duration<double>(wall1 - wall0).count();
  
  result.bytes_moved = 0;
  for (auto& s : stats) {
    result.bytes_moved += s.bytes;
    result.errors += s.errors;
  }
  
  double gb = (double)result.bytes_moved / (1024.0 * 1024.0 * 1024.0);
  result.bandwidth_gbps = (result.wall_time > 0) ? (gb / result.wall_time) : 0.0;
  
  cudaSetDevice(args.target);
  cudaFree(target_buf);
  
  return result;
}

int main(int argc, char** argv) {
  Args args = parse_args(argc, argv);
  
  // File generation mode
  if (args.generate) {
    print_header("FILE GENERATION MODE");
    
    std::cout << COLOR_BOLD << "Output directory: " << COLOR_RESET << args.output_dir << std::endl;
    std::cout << std::endl;
    
    // Create directory if needed
    mkdir(args.output_dir.c_str(), 0755);
    
    // Generate 1GB to 8GB files
    bool all_success = true;
    for (int gb = 1; gb <= 8; gb++) {
      std::string filename = "test_" + std::to_string(gb) + "GB.bin";
      std::string filepath = args.output_dir + "/" + filename;
      size_t size = (size_t)gb * 1024 * 1024 * 1024;
      
      if (!generate_test_file(filepath, size)) {
        all_success = false;
      }
      std::cout << std::endl;
    }
    
    print_separator();
    if (all_success) {
      std::cout << COLOR_GREEN << COLOR_BOLD << "✓ All test files generated successfully!" << COLOR_RESET << std::endl;
    } else {
      std::cout << COLOR_RED << COLOR_BOLD << "✗ Some files failed to generate" << COLOR_RESET << std::endl;
    }
    print_separator();
    
    return all_success ? 0 : 1;
  }
  
  // Benchmark suite mode
  if (args.benchmark_suite) {
    print_header("BENCHMARK SUITE MODE");
    
    int ngpu = 0;
    cudaError_t ce = cudaGetDeviceCount(&ngpu);
    if (ce != cudaSuccess) die("cudaGetDeviceCount failed: " + cudaErrStr(ce));
    if (ngpu <= 0) die("no CUDA device found");
    if (args.target < 0 || args.target >= ngpu) die("bad --target");
    
#ifdef WITH_CUFILE
    if (args.use_gds) {
      CUfileError_t st0 = cuFileDriverOpen();
      if (st0.err != CU_FILE_SUCCESS) {
        die("cuFileDriverOpen failed err=" + std::to_string(st0.err));
      }
    }
#endif
    
    std::cout << COLOR_BOLD << "Configuration:" << COLOR_RESET << std::endl;
    std::cout << "  Input directory: " << args.input_dir << std::endl;
    std::cout << "  Target GPU: " << args.target << std::endl;
    std::cout << "  Chunk size: " << format_size(args.chunk) << std::endl;
    std::cout << "  Iterations: " << args.iters << std::endl;
    std::cout << "  Mode: " << (args.use_gds ? COLOR_CYAN "GDS (cuFileRead)" COLOR_RESET : COLOR_MAGENTA "POSIX (pread+H2D)" COLOR_RESET) << std::endl;
    std::cout << "  O_DIRECT: " << (args.odirect ? "Yes" : "No") << std::endl;
    std::cout << "  Buffer registration: " << (args.bufreg ? "Yes" : "No") << std::endl;
    std::cout << std::endl;
    
    std::vector<BenchmarkResult> results;
    auto suite_start = std::chrono::high_resolution_clock::now();
    
    // Run benchmarks for 1GB to 8GB
    for (int gb = 1; gb <= 8; gb++) {
      std::string filename = "test_" + std::to_string(gb) + "GB.bin";
      std::string filepath = args.input_dir + "/" + filename;
      size_t file_size = (size_t)gb * 1024 * 1024 * 1024;
      
      print_subheader("Testing: " + filename + " (" + format_size(file_size) + ")");
      
      BenchmarkResult res = run_single_benchmark(args, filepath, file_size);
      results.push_back(res);
      
      if (res.errors == 0) {
        std::cout << COLOR_GREEN << "  ✓ Bandwidth: " << COLOR_BOLD 
                  << format_bandwidth(res.bandwidth_gbps) << COLOR_RESET 
                  << " (time: " << std::fixed << std::setprecision(3) << res.wall_time << "s)" << std::endl;
      } else {
        std::cout << COLOR_RED << "  ✗ FAILED with " << res.errors << " errors" << COLOR_RESET << std::endl;
      }
      std::cout << std::endl;
    }
    
    auto suite_end = std::chrono::high_resolution_clock::now();
    double suite_time = std::chrono::duration<double>(suite_end - suite_start).count();
    
    // Summary table
    print_separator('=');
    std::cout << COLOR_BOLD << COLOR_YELLOW << "  BENCHMARK SUMMARY" << COLOR_RESET << std::endl;
    print_separator('=');
    std::cout << std::endl;
    
    std::cout << std::left 
              << std::setw(15) << "File Size"
              << std::setw(25) << "Filename"
              << std::setw(15) << "Data Moved"
              << std::setw(12) << "Time (s)"
              << std::setw(18) << "Bandwidth"
              << std::setw(10) << "Status"
              << std::endl;
    print_separator('-');
    
    uint64_t total_bytes = 0;
    int total_errors = 0;
    
    for (const auto& res : results) {
      std::cout << std::left 
                << std::setw(15) << format_size(res.file_size)
                << std::setw(25) << res.filename
                << std::setw(15) << format_size(res.bytes_moved)
                << std::setw(12) << std::fixed << std::setprecision(3) << res.wall_time
                << std::setw(18) << format_bandwidth(res.bandwidth_gbps);
      
      if (res.errors == 0) {
        std::cout << COLOR_GREEN << "✓ OK" << COLOR_RESET;
      } else {
        std::cout << COLOR_RED << "✗ FAIL" << COLOR_RESET;
      }
      std::cout << std::endl;
      
      total_bytes += res.bytes_moved;
      total_errors += res.errors;
    }
    
    print_separator('-');
    
    // Overall statistics
    double total_gb = (double)total_bytes / (1024.0 * 1024.0 * 1024.0);
    double overall_gbps = (suite_time > 0) ? (total_gb / suite_time) : 0.0;
    
    std::cout << std::endl;
    std::cout << COLOR_BOLD << "Overall Statistics:" << COLOR_RESET << std::endl;
    std::cout << "  Total data moved: " << COLOR_BOLD << format_size(total_bytes) << COLOR_RESET << std::endl;
    std::cout << "  Total time: " << COLOR_BOLD << std::fixed << std::setprecision(2) << suite_time << " seconds" << COLOR_RESET << std::endl;
    std::cout << "  Average bandwidth: " << COLOR_BOLD << COLOR_GREEN << format_bandwidth(overall_gbps) << COLOR_RESET << std::endl;
    std::cout << "  Total errors: " << (total_errors == 0 ? COLOR_GREEN : COLOR_RED) << total_errors << COLOR_RESET << std::endl;
    std::cout << std::endl;
    
    print_separator('=');
    
#ifdef WITH_CUFILE
    if (args.use_gds) cuFileDriverClose();
#endif
    
    return total_errors == 0 ? 0 : 2;
  }
  
  // Single file mode (original behavior)
  print_header("SINGLE FILE BENCHMARK");
  
  struct stat st;
  if (stat(args.file.c_str(), &st) != 0) die("stat(file) failed: " + std::string(strerror(errno)));
  size_t file_size = (size_t)st.st_size;
  if (args.offset > file_size) die("--offset beyond file size");
  size_t avail = file_size - args.offset;
  size_t total = (args.bytes == 0) ? avail : std::min(args.bytes, avail);
  if (total == 0) die("nothing to read (total=0)");

  int ngpu = 0;
  cudaError_t ce = cudaGetDeviceCount(&ngpu);
  if (ce != cudaSuccess) die("cudaGetDeviceCount failed: " + cudaErrStr(ce));
  if (ngpu <= 0) die("no CUDA device found");
  if (args.target < 0 || args.target >= ngpu) die("bad --target");

  if (args.helpers.empty()) {
    for (int d = 0; d < ngpu; d++) args.helpers.push_back(d);
  }
  args.helpers = unique_keep_order(args.helpers);

  for (int h : args.helpers) {
    if (h < 0 || h >= ngpu) die("bad helper id in --helpers: " + std::to_string(h));
  }

  const size_t BLK = 4096;
  if (args.odirect) {
    if (args.offset % BLK != 0) die("O_DIRECT requires --offset 4KB aligned");
    if (args.chunk % BLK != 0) die("O_DIRECT requires --chunk multiple of 4KB");
    size_t new_total = align_down(total, BLK);
    if (new_total != total) {
      std::cerr << COLOR_YELLOW << "[WARN] O_DIRECT total rounded down " << total << " -> " << new_total << COLOR_RESET << "\n";
      total = new_total;
      if (total == 0) die("O_DIRECT alignment made total=0");
    }
  }

#ifdef WITH_CUFILE
  if (args.use_gds) {
    CUfileError_t st0 = cuFileDriverOpen();
    if (st0.err != CU_FILE_SUCCESS) {
      die("cuFileDriverOpen failed err=" + std::to_string(st0.err));
    }
  }
#else
  if (args.use_gds) die("need build with -DWITH_CUFILE");
#endif

  ce = cudaSetDevice(args.target);
  if (ce != cudaSuccess) die("cudaSetDevice(target) failed: " + cudaErrStr(ce));
  void* target_buf = nullptr;
  ce = cudaMalloc(&target_buf, total);
  if (ce != cudaSuccess) die("cudaMalloc(target_buf) failed: " + cudaErrStr(ce));

  for (int h : args.helpers) {
    if (h == args.target) continue;
    int can1 = 0, can2 = 0;
    cudaDeviceCanAccessPeer(&can1, args.target, h);
    cudaDeviceCanAccessPeer(&can2, h, args.target);
    if (!can1 || !can2) {
      std::cerr << COLOR_YELLOW << "[WARN] P2P not supported target(" << args.target << ") <-> gpu(" << h << ")" << COLOR_RESET << "\n";
      continue;
    }
    cudaSetDevice(args.target);
    cudaError_t e1 = cudaDeviceEnablePeerAccess(h, 0);
    if (e1 != cudaSuccess && e1 != cudaErrorPeerAccessAlreadyEnabled)
      std::cerr << COLOR_YELLOW << "[WARN] enable peer failed" << COLOR_RESET << "\n";

    cudaSetDevice(h);
    cudaError_t e2 = cudaDeviceEnablePeerAccess(args.target, 0);
    if (e2 != cudaSuccess && e2 != cudaErrorPeerAccessAlreadyEnabled)
      std::cerr << COLOR_YELLOW << "[WARN] enable peer failed" << COLOR_RESET << "\n";
  }

  size_t nreaders = args.helpers.size();
  size_t stripe = (total + nreaders - 1) / nreaders;
  if (args.odirect || args.use_gds) stripe = align_up(stripe, BLK);

  std::mutex print_mu;
  std::vector<ThreadStats> stats(nreaders);
  std::vector<std::thread> ths;

  std::cout << COLOR_BOLD << "Configuration:" << COLOR_RESET << std::endl;
  std::cout << "  File: " << args.file << std::endl;
  std::cout << "  File size: " << format_size(file_size) << std::endl;
  std::cout << "  Offset: " << format_size(args.offset) << std::endl;
  std::cout << "  Total to read: " << format_size(total) << std::endl;
  std::cout << "  Chunk size: " << format_size(args.chunk) << std::endl;
  std::cout << "  Target GPU: " << args.target << std::endl;
  std::cout << "  Reader GPUs: ";
  for (size_t i = 0; i < nreaders; i++) {
    std::cout << args.helpers[i] << (i + 1 == nreaders ? "" : ", ");
  }
  std::cout << std::endl;
  std::cout << "  Stripe size: " << format_size(stripe) << std::endl;
  std::cout << "  Iterations: " << args.iters << std::endl;
  std::cout << "  Mode: " << (args.use_gds ? COLOR_CYAN "GDS" COLOR_RESET : COLOR_MAGENTA "POSIX" COLOR_RESET) << std::endl;
  std::cout << std::endl;

  print_subheader("Starting benchmark...");
  auto wall0 = std::chrono::high_resolution_clock::now();

  for (size_t i = 0; i < nreaders; i++) {
    size_t start = args.offset + i * stripe;
    size_t end = std::min(args.offset + total, start + stripe);
    size_t bytes = (end > start) ? (end - start) : 0;

    if (args.odirect) bytes = align_down(bytes, BLK);
    if (bytes == 0) continue;

    ThreadCtx ctx;
    ctx.file = args.file;
    ctx.reader_dev = args.helpers[i];
    ctx.target_dev = args.target;
    ctx.global_offset = args.offset;
    ctx.file_off = start;
    ctx.bytes = bytes;
    ctx.chunk = args.chunk;
    ctx.iters = args.iters;
    ctx.use_gds = args.use_gds;
    ctx.odirect = args.odirect;
    ctx.bufreg = args.bufreg;
    ctx.verbose = args.verbose;
    ctx.target_buf = target_buf;
    ctx.stats = &stats[i];
    ctx.print_mu = &print_mu;

    ths.emplace_back(worker, ctx);
  }

  for (auto& t : ths) t.join();

  auto wall1 = std::chrono::high_resolution_clock::now();
  double wall_sec = std::chrono::duration<double>(wall1 - wall0).count();

  uint64_t moved = 0;
  int errors = 0;
  for (auto& s : stats) { moved += s.bytes; errors += s.errors; }

  double gb = (double)moved / (1024.0 * 1024.0 * 1024.0);
  double gbps = (wall_sec > 0) ? (gb / wall_sec) : 0.0;
  
  std::cout << std::endl;
  print_separator('=');
  std::cout << COLOR_BOLD << COLOR_YELLOW << "  RESULTS" << COLOR_RESET << std::endl;
  print_separator('=');
  std::cout << "  Total moved: " << COLOR_BOLD << format_size(moved) << COLOR_RESET << std::endl;
  std::cout << "  Wall time: " << COLOR_BOLD << std::fixed << std::setprecision(3) << wall_sec << " seconds" << COLOR_RESET << std::endl;
  std::cout << "  Bandwidth: " << COLOR_BOLD << COLOR_GREEN << format_bandwidth(gbps) << COLOR_RESET << std::endl;
  std::cout << "  Errors: " << (errors == 0 ? COLOR_GREEN : COLOR_RED) << errors << COLOR_RESET << std::endl;
  print_separator('=');

#ifdef WITH_CUFILE
  if (args.use_gds) cuFileDriverClose();
#endif

  cudaSetDevice(args.target);
  cudaFree(target_buf);
  return errors == 0 ? 0 : 2;
}
