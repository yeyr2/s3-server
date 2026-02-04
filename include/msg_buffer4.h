#ifndef __X_MSG_BUFFER_H__
#define __X_MSG_BUFFER_H__

/*
设计文档与核心特性 (Core Features):

1. 静态批量分配 (Bulk Allocation):
   初始化时一次性申请所有描述符和数据块。这消除了运行时的系统调用抖动，并保证物理地址 4K 对齐，
   完美适配 Direct IO。

2. 三级加速架构 (Tiered Acceleration):
   - L1 (TLC): 线程本地栈，无锁，OPS > 1000万。
   - L2 (Remote Inbox): 跨线程定向回流信箱，原子 CAS 链表，解决生产/消费模型下的锁竞争。
   - L3 (Global Pool): 全局后备池，通过原子水位计 (global_free_count_) 实现自适应流控。

3. 解耦视图架构 (Decoupled Architecture):
   - x_buf_unit_t (物理层): 负责生命周期、引用计数。
   - x_msg_t (逻辑层): 通过 segment (offset/length) 形成对物理内存的“视图”，支持零拷贝切片。

4. 确定性保护 (Fail-Fast) 与流控:
   - 支持非阻塞 get()：池耗尽返回 nullptr，触发上层流控（如停止 epoll 接收）。
   - 原子状态机：利用 FREE/BUSY 状态拦截并发双重释放 (Double-Free)。

5. 自适应回拢 (Adaptive Reclamation):
   - 当全局水位 < 5% 时，强制所有释放动作直达全局池，防止局部线程囤积导致系统性饥饿。
*/

#include <atomic>
#include <vector>
#include <mutex>
#include <cstdint>
#include <algorithm>
#include <cstring>
#include <sys/uio.h> 

#define X_LIKELY(x)   __builtin_expect(!!(x), 1)
#define X_UNLIKELY(x) __builtin_expect(!!(x), 0)

enum class UnitState : uint32_t {
    FREE = 0xDEADBEEF,
    BUSY = 0x5A5A5A5A  
};

class x_buf_pool_t;
struct x_thread_cache_t;

// ============================================================================
// 1. 缓冲单元描述符 (x_buf_unit_t)
// ============================================================================
struct alignas(64) x_buf_unit_t 
{
    std::atomic<int32_t>   ref{0};         
    std::atomic<UnitState> state{UnitState::FREE};
    
    uint8_t* data_ptr{nullptr};            
    uint32_t      capacity{0};             
    x_buf_pool_t* owner_pool{nullptr};     
    uint32_t      origin_tid{0};           
    x_thread_cache_t* origin_tlc{nullptr}; 
    x_buf_unit_t* next_inbox{nullptr};     

    void add_ref();  
    void release(); 
};

// ============================================================================
// 2. RAII 智能指针 (x_buf_ptr)
// ============================================================================
class x_buf_ptr {
public:
    explicit x_buf_ptr(x_buf_unit_t* unit = nullptr);
    x_buf_ptr(const x_buf_ptr& other);
    x_buf_ptr(x_buf_ptr&& other) noexcept;
    x_buf_ptr& operator=(x_buf_ptr other);
    ~x_buf_ptr();
    x_buf_unit_t* operator->() const { return unit_; }
    x_buf_unit_t* get() const { return unit_; }
    explicit operator bool() const { return unit_ != nullptr; }
private:
    x_buf_unit_t* unit_;
};

// ============================================================================
// 3. 线程本地缓存 (TLC)
// ============================================================================
struct alignas(64) x_thread_cache_t {
    static constexpr size_t L1_CAPACITY = 128; 
    x_buf_unit_t* stack[L1_CAPACITY];          
    size_t count{0};
    std::atomic<x_buf_unit_t*> remote_inbox{nullptr}; // L2 归还信箱

    bool empty() const { return count == 0; }  // 新增：修复tlc.empty()未定义
};

// ============================================================================
// 4. 缓冲池类 (x_buf_pool_t)
// ============================================================================
class x_buf_pool_t {
public:
    x_buf_pool_t(uint32_t payload_size, uint32_t count);
    ~x_buf_pool_t();

    x_buf_ptr get();             
    void release(x_buf_unit_t* unit); // 原 put()，更名为 release

    static uint32_t get_curr_tid();
    static x_thread_cache_t& get_tlc(); 

    // 统计功能
    size_t get_tlc_count() const { return get_tlc().count; }  // 添加const
    int32_t get_global_count() const { return global_free_count_.load(std::memory_order_relaxed); }  // 添加const
    uint32_t get_total_count() const { return total_count_; }

private:
    uint32_t payload_size_;
    uint32_t total_count_;
    std::atomic<int32_t> global_free_count_{0}; 
    
    x_buf_unit_t* all_units_base_{nullptr};
    void* all_data_base_{nullptr};
    
    std::vector<x_buf_unit_t*> global_free_list_; 
    std::mutex global_lock_;                      
};

// ============================================================================
// 5. 消息视图容器 (x_msg_t)
// ============================================================================
class x_msg_t {
public:
    struct segment { x_buf_unit_t* unit; uint32_t offset; uint32_t length; };
    x_msg_t() = default;
    ~x_msg_t();
    void clear();
    void append_unit(x_buf_unit_t* unit, uint32_t offset, uint32_t length);
    
    // 从外部buffer 拷贝数据到 msg，并自动扩展msg
    bool copy_in(x_buf_pool_t& pool, const void* src, uint32_t len);
        
    // 导出到连续内存
    uint32_t copy_out(char* dst, uint32_t max_len) const;

    uint32_t total_length() const { return total_len_; }
    size_t get_iovec(struct iovec* iov, size_t max_iov) const;

private:
    std::vector<segment> segments_;
    uint32_t total_len_{0};
};

#endif