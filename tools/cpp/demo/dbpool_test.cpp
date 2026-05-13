#include "dbpool.h"
#include <iostream>

int main() {
    // 1. 初始化连接池（全局仅调用一次）
    auto& pool = DbConnPool::getInstance();
    if (!pool.init(5, "oracle_conn_str", "AL32UTF8")) { // 替换为你的实际连接串
        std::cerr << "连接池初始化失败！" << std::endl;
        return -1;
    }

    // 2. 获取连接（自动管理，无需手动归还）
    try {
        auto conn = pool.get(); // conn 是智能指针，自动绑定归还逻辑
        // 使用连接执行数据库操作
        // conn->execSQL("SELECT * FROM table"); // 替换为你的实际操作
        std::cout << "成功获取连接并执行操作" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "获取连接失败：" << e.what() << std::endl;
        return -1;
    }

    // 3. 程序退出时，连接池析构会自动调用 destroy() 释放所有连接
    return 0;
}