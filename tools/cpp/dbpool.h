#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <stdexcept>
// 你的 Oracle 连接封装类（确保 _ooci.h 中定义了 connection 类）
#include "/project/public/db/oracle/_ooci.h"  

// 数据库连接池（单例模式 + 安全的内存管理）
class DbConnPool {
public:
    // 禁止拷贝和移动（单例核心）
    DbConnPool(const DbConnPool&) = delete;
    DbConnPool& operator=(const DbConnPool&) = delete;
    DbConnPool(DbConnPool&&) = delete;
    DbConnPool& operator=(DbConnPool&&) = delete;

    // 获取单例实例（全局唯一）
    static DbConnPool& getInstance() {
        static DbConnPool instance; // 局部静态变量，线程安全（C++11+）
        return instance;
    }

    // 初始化连接池（仅调用一次）
    bool init(int pool_size, const char* connstr, const char* charset);

    // 从池获取连接（阻塞等待，返回智能指针，自动管理生命周期）
    std::unique_ptr<connection, std::function<void(connection*)>> get();

    // 归还连接到池（内部调用，用户无需手动调用）
    void put(std::unique_ptr<connection> conn);

    // 销毁连接池（释放所有连接）
    void destroy();

private:
    // 私有构造/析构（单例核心）
    DbConnPool() = default;
    ~DbConnPool() { destroy(); } // 析构时自动清理

    // 检查连接是否有效（需根据你的 connection 类实现，比如 ping 数据库）
    bool isConnValid(connection* conn);

    // 重建连接（当连接无效时）
    std::unique_ptr<connection> recreateConn(const char* connstr, const char* charset);

    // 成员变量
    std::mutex mtx_;                  // 互斥锁，保护池操作
    std::condition_variable cv_;      // 条件变量，实现阻塞等待
    std::queue<std::unique_ptr<connection>> pool_; // 连接池容器
    int pool_size_ = 0;               // 池大小
    const char* connstr_ = nullptr;   // 数据库连接串
    const char* charset_ = nullptr;   // 字符集
    bool is_inited_ = false;          // 初始化标记
};