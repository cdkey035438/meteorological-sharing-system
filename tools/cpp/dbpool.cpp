#include "dbpool.h"
#include <iostream>

// 初始化连接池
bool DbConnPool::init(int pool_size, const char* connstr, const char* charset) {
    if (is_inited_) {
        std::cerr << "连接池已初始化，无需重复调用！" << std::endl;
        return true;
    }
    if (pool_size <= 0 || !connstr || !charset) {
        std::cerr << "初始化参数非法：池大小>0、连接串/字符集非空！" << std::endl;
        return false;
    }

    std::lock_guard<std::mutex> lock(mtx_);
    pool_size_ = pool_size;
    connstr_ = connstr;
    charset_ = charset;

    // 批量创建连接
    int create_cnt = 0;
    for (int i = 0; i < pool_size_; ++i) {
        auto conn = std::make_unique<connection>();
        if (!conn || !conn->connecttodb(connstr_, charset_)) {
            std::cerr << "创建第" << i+1 << "个数据库连接失败！" << std::endl;
            // 回滚：销毁已创建的连接
            destroy();
            return false;
        }
        pool_.push(std::move(conn));
        create_cnt++;
    }

    is_inited_ = true;
    std::cout << "连接池初始化成功，创建了 " << create_cnt << " 个连接" << std::endl;
    return true;
}

// 获取连接（阻塞等待，返回带自定义删除器的智能指针）
std::unique_ptr<connection, std::function<void(connection*)>> DbConnPool::get() {
    if (!is_inited_) {
        throw std::runtime_error("连接池未初始化，无法获取连接！");
    }

    std::unique_lock<std::mutex> lock(mtx_);
    // 阻塞等待，直到池中有可用连接
    cv_.wait(lock, [this]() { return !pool_.empty(); });

    // 取出池顶连接
    auto conn = std::move(pool_.front());
    pool_.pop();
    lock.unlock(); // 提前解锁，减少锁持有时间

    // 检查连接是否有效，无效则重建
    if (!isConnValid(conn.get())) {
        std::cerr << "连接失效，重建新连接..." << std::endl;
        conn = recreateConn(connstr_, charset_);
        if (!conn) {
            throw std::runtime_error("连接失效且重建失败！");
        }
    }

    // 返回智能指针，删除器绑定“归还连接”逻辑（用户无需手动调用put）
    return std::unique_ptr<connection, std::function<void(connection*)>>(
        conn.release(),
        [this](connection* c) {
            if (c) {
                this->put(std::unique_ptr<connection>(c));
            }
        }
    );
}

// 归还连接到池
void DbConnPool::put(std::unique_ptr<connection> conn) {
    if (!conn || !is_inited_) {
        return;
    }

    std::lock_guard<std::mutex> lock(mtx_);
    pool_.push(std::move(conn));
    cv_.notify_one(); // 通知一个等待的线程：有新连接可用
}

// 销毁连接池（释放所有连接）
void DbConnPool::destroy() {
    std::lock_guard<std::mutex> lock(mtx_);
    int destroy_cnt = 0;
    while (!pool_.empty()) {
        auto conn = std::move(pool_.front());
        pool_.pop();
        // 若 connection 有析构函数自动关闭连接，这里无需额外操作；否则手动关闭
        // conn->close(); // 根据你的 connection 类补充关闭逻辑
        destroy_cnt++;
    }
    is_inited_ = false;
    pool_size_ = 0;
    std::cout << "连接池已销毁，释放了 " << destroy_cnt << " 个连接" << std::endl;
}

// 检查连接是否有效（需根据你的 connection 类实现）
bool DbConnPool::isConnValid(connection* conn) {
    if (!conn) return false;
    // 示例：调用 connection 的 ping 方法（需你自己实现）
    // return conn->ping();
    // 若没有 ping 方法，可简单返回 true（后续根据实际情况补充）
    return true;
}

// 重建连接
std::unique_ptr<connection> DbConnPool::recreateConn(const char* connstr, const char* charset) {
    auto conn = std::make_unique<connection>();
    if (conn && conn->connecttodb(connstr, charset)) {
        return conn;
    }
    return nullptr;
}