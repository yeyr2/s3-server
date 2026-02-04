#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/syscall.h>
#include "msg_buffer4.h"

static void x_buf_panic(const char* file, int line, const char* msg) {
    fprintf(stderr, "\n[FATAL] [%s:%d] %s\n", file, line, msg);
    abort();
}
#define X_PANIC(msg) x_buf_panic(__FILE__, __LINE__, msg)

// --- x_buf_unit_t ---
void x_buf_unit_t::add_ref() {
    if (X_UNLIKELY(state.load(std::memory_order_relaxed) != UnitState::BUSY)) {
        X_PANIC("RETAIN_ON_FREE_UNIT");
    }
    ref.fetch_add(1, std::memory_order_relaxed);
}

void x_buf_unit_t::release() {
    if (ref.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        UnitState expected = UnitState::BUSY;
        if (X_UNLIKELY(!state.compare_exchange_strong(expected, UnitState::FREE, std::memory_order_acq_rel))) {
            X_PANIC("DOUBLE_FREE_DETECTED");
        }
        owner_pool->release(this); 
    }
}

// --- x_buf_ptr ---
x_buf_ptr::x_buf_ptr(x_buf_unit_t* u) : unit_(u) {}
x_buf_ptr::x_buf_ptr(x_buf_ptr&& other) noexcept : unit_(other.unit_) { other.unit_ = nullptr; }
x_buf_ptr::x_buf_ptr(const x_buf_ptr& other) : unit_(other.unit_) { if (unit_) unit_->add_ref(); }
x_buf_ptr& x_buf_ptr::operator=(x_buf_ptr other) { std::swap(unit_, other.unit_); return *this; }
x_buf_ptr::~x_buf_ptr() { if (unit_) unit_->release(); }

// --- x_buf_pool_t ---
x_buf_pool_t::x_buf_pool_t(uint32_t payload_size, uint32_t count) 
    : payload_size_((payload_size + 4095) & ~4095), total_count_(count) {
    all_units_base_ = static_cast<x_buf_unit_t*>(malloc(sizeof(x_buf_unit_t) * count));
    if (!all_units_base_) X_PANIC("MALLOC_FAILED");  // 新增：检查malloc失败

    if (posix_memalign(&all_data_base_, 4096, (size_t)payload_size_ * count) != 0) {
        free(all_units_base_);  // 新增：清理
        X_PANIC("MEMALIGN_FAILED");
    }

    global_free_list_.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        x_buf_unit_t* u = &all_units_base_[i];
        // 移除多余placement new：struct已初始化
        u->owner_pool = this;
        u->data_ptr = (uint8_t*)all_data_base_ + (i * (size_t)payload_size_);
        u->capacity = payload_size_;
        u->origin_tid = 0; 
        u->origin_tlc = nullptr;
        u->next_inbox = nullptr;
        global_free_list_.push_back(u);
    }
    global_free_count_.store(count, std::memory_order_relaxed); 
}

x_buf_pool_t::~x_buf_pool_t() { 
    free(all_units_base_); 
    free(all_data_base_); 
}

uint32_t x_buf_pool_t::get_curr_tid() {
    static thread_local uint32_t tid = 0;
    if (X_UNLIKELY(tid == 0)) tid = (uint32_t)syscall(SYS_gettid);
    return tid;
}

x_thread_cache_t& x_buf_pool_t::get_tlc() {
    static thread_local x_thread_cache_t tls_tlc;
    return tls_tlc;
}

x_buf_ptr x_buf_pool_t::get() {
    x_thread_cache_t& tlc = get_tlc();
    x_buf_unit_t* unit = nullptr;

    // 1. L1 TLC 优先
    if (!tlc.empty()) {
        unit = tlc.stack[--tlc.count];
    } 
    // 2. L2 Inbox 收割 (处理跨线程回流)
    else if ((unit = tlc.remote_inbox.exchange(nullptr, std::memory_order_acquire)) != nullptr) {
        // 修复bug：先将整个链表逆转收集到stack（避免长链O(n)，但n<=批次大小）
        // 如果stack溢出，将剩余推到global
        size_t added = 0;
        x_buf_unit_t* curr = unit;
        while (curr) {
            x_buf_unit_t* next = curr->next_inbox;
            if (tlc.count < x_thread_cache_t::L1_CAPACITY) {
                tlc.stack[tlc.count++] = curr;
                ++added;
            } else {
                // 剩余推到global（需锁）
                std::lock_guard<std::mutex> lock(global_lock_);
                global_free_list_.push_back(curr);
                global_free_count_.fetch_add(1, std::memory_order_relaxed);
            }
            curr = next;
        }
        // 从stack pop一个作为unit（如果added>0）
        if (added > 0) {
            unit = tlc.stack[--tlc.count];
        } else {
            unit = nullptr;  // 罕见：所有都溢出到global
        }
    } 
    // 3. L3 全局池补充
    else {
        std::lock_guard<std::mutex> lock(global_lock_);
        if (X_UNLIKELY(global_free_list_.empty())) return x_buf_ptr(nullptr); // 流控：返回空
        
        size_t fetch_cnt = std::min(global_free_list_.size(), x_thread_cache_t::L1_CAPACITY / 2);
        for (size_t i = 0; i < fetch_cnt - 1; ++i) {
            tlc.stack[tlc.count++] = global_free_list_.back();
            global_free_list_.pop_back();
        }
        unit = global_free_list_.back();
        global_free_list_.pop_back();
        global_free_count_.fetch_sub(fetch_cnt, std::memory_order_relaxed); 
    }

    if (!unit) return x_buf_ptr(nullptr);  // 新增：如果inbox全溢出，返回空（虽罕见）

    unit->origin_tid = get_curr_tid();
    unit->origin_tlc = &tlc;
    unit->ref.store(1, std::memory_order_relaxed);
    unit->state.store(UnitState::BUSY, std::memory_order_relaxed);
    return x_buf_ptr(unit);
}

void x_buf_pool_t::release(x_buf_unit_t* unit) {
    uint32_t curr_tid = get_curr_tid();
    x_thread_cache_t& tlc = get_tlc();

    // 自适应检查：全局缺货时强制直还全局池
    if (X_UNLIKELY(global_free_count_.load(std::memory_order_relaxed) < (int32_t)(total_count_ * 0.05))) {
        std::lock_guard<std::mutex> lock(global_lock_);
        global_free_list_.push_back(unit);
        global_free_count_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    if (unit->origin_tid == curr_tid) {
        if (X_LIKELY(tlc.count < x_thread_cache_t::L1_CAPACITY)) {
            tlc.stack[tlc.count++] = unit; // 无锁 L1
        } else {
            // L1 溢出批量归还
            std::lock_guard<std::mutex> lock(global_lock_);
            size_t move_cnt = x_thread_cache_t::L1_CAPACITY / 2;
            for (size_t i = 0; i < move_cnt; ++i) global_free_list_.push_back(tlc.stack[--tlc.count]);
            global_free_list_.push_back(unit);
            global_free_count_.fetch_add(move_cnt + 1, std::memory_order_relaxed);
        }
    } else {
        // L2 无锁 Inbox 定向回流
        x_buf_unit_t* old_head = unit->origin_tlc->remote_inbox.load(std::memory_order_relaxed);
        do {
            unit->next_inbox = old_head;
        } while (!unit->origin_tlc->remote_inbox.compare_exchange_weak(old_head, unit, std::memory_order_release, std::memory_order_relaxed));
    }
}

// --- x_msg_t ---
x_msg_t::~x_msg_t() { clear(); }
void x_msg_t::clear() 
{ 
    for (auto& seg : segments_) {
        seg.unit->release(); 
    }
    segments_.clear(); 
    total_len_ = 0; 
}

/**
 * 核心功能：将数据追加到 msg 尾部（视图，不拷贝数据，只是形式上的追加，没有申请）
 * 1. 优先利用最后一个 segment 的 unit 剩余空间
 * 2. 空间不足则自动申请新 unit
 */
void x_msg_t::append_unit(x_buf_unit_t* unit, uint32_t offset, uint32_t length) {
    if (X_UNLIKELY(!unit || length == 0)) return;
    if (X_UNLIKELY(offset + length > unit->capacity)) X_PANIC("VIEW_OUT_OF_BOUNDS");
    if (X_UNLIKELY(unit->state.load(std::memory_order_relaxed) != UnitState::BUSY)) X_PANIC("APPEND_FREE_UNIT");  // 新增：检查状态
    unit->add_ref();
    segments_.push_back({unit, offset, length});
    total_len_ += length; 
}

/**
 * 核心功能：将数据追加到 msg 尾部（拷贝数据，申请新的unit）
 * WARNING： append_unit 获取的unit不能使用cpoy_in，可能覆盖其他segment的数据，尽量保证一个 unit 只对应一个 segment，负责保证修改的segment使用的是最右边的。
 * 1. 优先利用最后一个 segment 的 unit 剩余空间
 * 2. 空间不足则自动申请新 unit
 */
bool x_msg_t::copy_in(x_buf_pool_t& pool, const void* src, uint32_t len) 
{
    if (X_UNLIKELY(!src || len == 0)) 
        return true;

    const uint8_t* src_ptr = static_cast<const uint8_t*>(src);
    uint32_t remaining = len;

    // Step 1: 尝试利用最后一个单元的“余料”空间
    if (!segments_.empty()) {
        segment& last_seg = segments_.back();
        x_buf_unit_t* u = last_seg.unit;
        
        // 计算该 unit 尾部还剩多少物理空间
        uint32_t used_physical = last_seg.offset + last_seg.length;
        if (used_physical < u->capacity) {
            uint32_t avail = u->capacity - used_physical;
            uint32_t to_fill = std::min(remaining, avail);
            
            // 在原 unit 物理地址上直接追加
            memcpy(u->data_ptr + used_physical, src_ptr, to_fill);
            
            // 更新该 segment 的逻辑长度和 msg 总长度
            last_seg.length += to_fill;
            total_len_ += to_fill;
            
            src_ptr += to_fill;
            remaining -= to_fill;
        }
    }

    // Step 2: 如果还有剩余数据，申请新 unit 循环填充
    while (remaining > 0) {
        x_buf_ptr new_ptr = pool.get();
        if (X_UNLIKELY(!new_ptr)) {
            // 池耗尽，触发流控返回 false
            return false; 
        }

        uint32_t to_copy = std::min(remaining, new_ptr->capacity);
        memcpy(new_ptr->data_ptr, src_ptr, to_copy);
        
        // 挂载新 unit
        this->append_unit(new_ptr.get(), 0, to_copy);
        
        src_ptr += to_copy;
        remaining -= to_copy;
    }

    return true;
}

/**
 * copy_out: 导出数据到连续缓冲区
 */
uint32_t x_msg_t::copy_out(char* dst, uint32_t max_len) const {
    if (X_UNLIKELY(!dst || max_len == 0 || segments_.empty())) return 0;

    uint32_t bytes_to_copy = std::min(max_len, total_len_);
    uint32_t remaining = bytes_to_copy;
    char* curr_dst = dst;

    for (const auto& seg : segments_) {
        uint32_t seg_len = std::min(remaining, seg.length);
        if (seg_len > 0) {
            memcpy(curr_dst, seg.unit->data_ptr + seg.offset, seg_len);
            curr_dst += seg_len;
            remaining -= seg_len;
        }
        if (remaining == 0) break;
    }
    return bytes_to_copy - remaining;  // 修复：返回实际拷贝字节（如果segments异常）
}

// 把消息里的每一段（segment）填进 struct iovec 数组，供 writev() 等“分散写”接口使用。
// 不拷贝数据，只填指针和长度。
size_t x_msg_t::get_iovec(struct iovec* iov, size_t max_iov) const {
    size_t count = std::min(max_iov, segments_.size());
    for (size_t i = 0; i < count; ++i) {
        iov[i].iov_base = segments_[i].unit->data_ptr + segments_[i].offset;
        iov[i].iov_len  = segments_[i].length;
    }
    return count;
}