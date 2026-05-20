# 气象数据共享平台

基于 Reactor 模式与仿 Muduo 网络库的气象数据共享平台后端系统。

---

## 项目简介

本项目是一个面向气象业务场景的数据共享平台后端系统，覆盖从多源气象数据处理、标准化入库、数据同步到对外查询服务的完整后端链路。平台采用读写分离的数据架构，通过配置驱动机制实现灵活的数据接口管理，并在数据访问模块中引入基于 **Reactor 模式**、参考 **Muduo 网络库**设计思想实现的事件驱动通信组件，以提升系统在并发访问场景下的处理能力。

项目来源于实际气象数据共享业务场景，在对原始业务数据进行脱敏处理后用于教学验证与工程实践。

## 技术栈

| 技术 | 说明 |
|------|------|
| C++11 | 核心开发语言，使用智能指针、Lambda、std::function、std::thread 等现代特性 |
| Linux | 服务器运行环境，基于 epoll 实现 I/O 多路复用 |
| Oracle | 后端数据存储，通过 OCI 接口进行数据库访问 |
| XML | 数据交换中间格式，标准化数据流转 |
| Reactor 模式 | 事件驱动网络编程模型 |
| Muduo 设计思想 | 参考陈硕 Muduo 网络库的组件化架构 |
| HTTP | 数据访问接口通信协议 |

## 系统架构

平台采用分层与模块化架构设计，整体划分为五个功能层次：

```
+--------------------------------------------------+
|                  数据服务层                        |
|    webserver (V1)  |  webserver_reactor (V2)     |
|    基准版HTTP接口      |  Reactor事件驱动接口        |
+--------------------------------------------------+
|                  数据同步层                        |
|       syncinc (增量同步)  |  syncref (参考同步)       |
+--------------------------------------------------+
|                  数据入库层                        |
|              xmltodb (配置驱动入库引擎)              |
+--------------------------------------------------+
|                业务处理层 (idc)                     |
|   crtsurfdata | obtcodetodb | obtmindtodb ...      |
+--------------------------------------------------+
|              系统管理与运维层                        |
|    procctl (调度) | checkproc (守护) | 启停脚本      |
+--------------------------------------------------+
```

**数据流转**：原始气象数据 → 业务处理生成 XML → xmltodb 配置驱动入库 → 核心数据库 → 数据同步 → 业务数据库 → HTTP 接口查询服务

## 目录结构

```
meteorological-sharing-system/
├── idc/                          # 业务处理层
│   └── cpp/
│       ├── crtsurfdata.cpp       # 地面气象观测数据生成
│       ├── obtcodetodb.cpp       # 站点参数入库
│       ├── obtcodetodb2.cpp      # 站点参数入库(扩展版)
│       ├── obtmindtodb.cpp       # 分钟观测数据入库
│       ├── idcapp.cpp/h          # 业务处理公共类
│       ├── start.sh              # 启动脚本
│       └── stop.sh               # 停止脚本
│
├── public/                       # 公共基础库
│   ├── network/                  # 仿 Muduo 网络库
│   │   ├── TcpServer.cpp/h       # TCP 服务器入口
│   │   ├── EventLoop.cpp/h       # 事件循环
│   │   ├── Channel.cpp/h         # 通道(事件与回调绑定)
│   │   ├── Connection.cpp/h      # 连接对象
│   │   ├── Buffer.cpp/h          # 输入/输出缓冲区
│   │   ├── Epoll.cpp/h           # epoll I/O 多路复用
│   │   ├── Acceptor.cpp/h        # 连接接受器
│   │   ├── Socket.cpp/h          # 套接字封装
│   │   ├── InetAddress.cpp/h     # 网络地址封装
│   │   ├── ThreadPool.cpp/h      # 工作线程池
│   │   └── Timestamp.cpp/h       # 时间戳工具
│   ├── db/oracle/                # Oracle OCI 数据库封装
│   ├── _public.cpp/h             # 公共工具函数库
│   ├── _ftp.cpp/h                # FTP 工具封装
│   └── ftplib.c/h                # 开源 FTP 客户端库
│
├── tools/cpp/                    # 工具程序
│   ├── xmltodb.cpp               # 通用 XML 入库引擎
│   ├── syncinc.cpp               # 增量数据同步
│   ├── syncref.cpp               # 参考(全量)数据同步
│   ├── webserver.cpp             # 基准版 HTTP 数据访问接口 (V1, port 8080)
│   ├── webserver_reactor.cpp     # Reactor 版 HTTP 数据访问接口 (V2, port 8081)
│   ├── ftpgetfiles.cpp           # FTP 文件下载
│   ├── ftpputfiles.cpp           # FTP 文件上传
│   ├── tcpgetfiles.cpp           # TCP 自定义协议文件下载
│   ├── tcpputfiles.cpp           # TCP 自定义协议文件上传
│   ├── procctl.cpp               # 进程周期调度控制
│   ├── checkproc.cpp             # 进程守护与心跳检查
│   ├── dbpool.cpp/h              # 数据库连接池
│   ├── deletefiles.cpp           # 过期文件清理
│   ├── deletetable.cpp           # 过期数据清理
│   ├── dminingoracle.cpp         # Oracle 数据抽取
│   ├── fileserver.cpp            # 文件传输服务
│   ├── gzipfiles.cpp             # 日志文件压缩归档
│   ├── test_api.sh               # 接口测试脚本
│   └── demo/                     # 演示与测试代码
│
└── README.md                     # 本文件
```

## 核心功能模块

### 1. 业务处理层 (idc/cpp)

负责将原始气象数据整理为标准化 XML 中间文件，供后续入库使用。

- **crtsurfdata**：生成全国气象站点地面观测数据（模拟数据源）
- **obtcodetodb / obtmindtodb**：站点参数与分钟观测数据的预处理与标准化

### 2. 数据入库层 (xmltodb)

通用 XML 入库引擎，采用**配置驱动**方式实现不同 XML 文件到数据库表的统一装载。

- 通过配置表自动完成 XML 字段与数据库表字段的映射
- 支持插入/更新双模式（主键冲突时自动切换）
- 支持预处理 SQL 执行
- 新增数据类型时无需修改代码，仅需调整配置

### 3. 数据同步层 (syncinc / syncref)

实现核心数据库与业务数据库之间的数据分发，支持读写分离架构。

- **syncinc**：增量同步，适用于持续追加的业务数据
- **syncref**：参考(全量)同步，适用于参数/字典类基础数据
- 支持基于 XML 中间文件的分发再加载机制

### 4. 数据服务层 (webserver / webserver_reactor)

对外提供统一的 HTTP 数据查询接口服务。

#### V1 - 基准版 (webserver.cpp，port 8080)
- 基于 epoll 的过程式事件处理
- 支持用户认证、接口权限控制
- 配置驱动的动态 SQL 查询
- XML 标准化响应（meta + data + summary）
- 大数据量查询截断保护

#### V2 - Reactor 版 (webserver_reactor.cpp，port 8081)
- 基于 Reactor 模式的事件驱动架构
- I/O 线程负责网络收发，工作线程负责业务处理
- 参考 Muduo 网络库设计思想，使用仿 Muduo 组件
- 与 V1 保持业务逻辑一致，网络结构更清晰
- 支持小规模并发访问

**两种接口版本并行运行**，形成基准对照与功能验证环境。

### 5. 仿 Muduo 网络库 (public/network)

参考陈硕《Linux 多线程服务端编程》及 Muduo 网络库设计思想，结合平台数据访问场景裁剪实现的核心组件：

| 组件 | 职责 |
|------|------|
| **TcpServer** | 服务器入口，管理主/从事件循环、Acceptor 和线程池 |
| **EventLoop** | 事件循环核心，基于 epoll 进行事件监听与分发 |
| **Channel** | 封装文件描述符及其关注事件，绑定回调函数 |
| **Connection** | 管理连接生命周期，持有 Socket、Channel 和 Buffer |
| **Buffer** | 输入/输出缓冲区，支持 HTTP 报文分隔解析 |
| **Epoll** | epoll I/O 多路复用封装 |
| **Acceptor** | 监听端口，接受新连接 |
| **ThreadPool** | 工作线程池，处理数据库查询等业务逻辑 |

### 6. 系统管理与运维

- **procctl**：周期调度程序，定时拉起数据处理任务
- **checkproc**：进程守护检查，监控后台服务存活状态
- **start.sh / stop.sh**：统一启停脚本，按依赖顺序管理各模块

## 编译环境

- **操作系统**：Linux（CentOS 7 / Ubuntu 等）
- **编译器**：g++（支持 C++11）
- **数据库**：Oracle 11g+
- **依赖库**：Oracle OCI (libclntsh)

## 快速开始

### 1. 环境准备

```bash
# 确保已安装 Oracle 客户端并设置环境变量
export ORACLE_HOME=/opt/oracle/instantclient_11_2
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH
```

### 2. 编译

```bash
# 进入 tools 目录编译全部工具
cd tools/cpp
make all

# 编译后的二进制文件会自动复制到 ../bin/ 目录
```

### 3. 启动服务

```bash
# 进入 idc 目录，使用启停脚本
cd idc/cpp
./start.sh

# 或手动启动核心服务
procctl 5 /project/tools/bin/webserver /log/idc/webserver.log 8080
procctl 5 /project/tools/bin/webserver_reactor /log/idc/webserver_reactor.log 8081
```

### 4. 接口测试

```bash
# 使用测试脚本验证接口
cd tools/cpp
bash test_api.sh

# 或手动 curl 调用
# V1 接口 (port 8080)
curl "http://127.0.0.1:8080/api?username=liang&passwd=101018&intername=getzhobtmind3&obtid=58015&begintime=20250109120000&endtime=20250109121000"

# V2 接口 (port 8081)
curl "http://127.0.0.1:8081/api?username=liang&passwd=101018&intername=getzhobtmind3&obtid=58015&begintime=20250109120000&endtime=20250109121000"
```

### 5. 停止服务

```bash
cd idc/cpp
./stop.sh
```

## API 接口说明

### 请求格式

```
GET /api?username={用户名}&passwd={密码}&intername={接口名}&{参数1}={值1}&...
```

### 响应格式（XML）

```xml
<?xml version="1.0" encoding="utf-8"?>
<data>
  <meta>
    <col label="站号" unit="" scale="1" type="string"/>
    <col label="气温" unit="℃" scale="0.1" type="number"/>
    ...
  </meta>
  <data>
    <row>
      <obtid>58015</obtid>
      <t>235</t>
      ...
    </row>
  </data>
  <summary>
    <rowcount>10</rowcount>
    <has_more>false</has_more>
    <truncated_by_size>false</truncated_by_size>
  </summary>
</data>
```

### 接口配置

所有数据查询接口通过数据库配置表 (`T_INTERCFG`) 动态定义，包括：
- 接口名称与标识
- 查询 SQL 语句
- 输入参数定义
- 输出字段及元数据（中文名称、单位、缩放比例）

新增接口无需修改代码，仅需在配置表中插入记录。

## 数据库设计

系统数据库分为两大域：

**业务数据域**
- `T_ZHOBTCODE` — 气象站点参数表
- `T_ZHOBTMIND` — 分钟级气象观测数据表

**数据服务域**
- `T_USERINFO` — 用户信息表
- `T_INTERCFG` — 接口配置表
- `T_USERANDINTER` — 用户接口权限表
- `T_COLMETA` — 字段元数据定义表

## 项目亮点

- **完整后端链路**：覆盖"数据处理 → 入库 → 同步 → 服务"全流程，非单一接口演示
- **双版本接口对照**：基准版(V1)与 Reactor 版(V2)并行运行，便于功能验证与结构对比
- **仿 Muduo 网络库**：完整实现 EventLoop、Channel、Buffer、TcpServer、ThreadPool 等核心组件
- **配置驱动设计**：数据入库与接口服务均通过配置表驱动，具备良好的可扩展性
- **接口规范化**：统一 XML 响应结构(meta + data + summary)，支持字段元数据语义说明和大数据量截断保护
- **运维支撑**：包含进程调度、心跳检测、守护检查、启停脚本等完整运维机制

## 学习参考

- 陈硕.《Linux 多线程服务端编程：使用 muduo C++ 网络库》. 电子工业出版社, 2013.
- Schmidt D C. "Reactor: An Object Behavioral Pattern for Demultiplexing and Dispatching Handles for Synchronous Events". Pattern Languages of Program Design, 1995.

## 作者

梁航 — 广州理工学院 计算机科学与工程学院

**毕业论文题目**：《基于 Reactor 模式与仿 Muduo 网络库的气象数据共享平台设计与实现》

---

> 本项目为本科毕业设计，基于真实气象数据共享业务场景进行脱敏和教学裁剪，定位为工程设计与实现，面向教学验证环境。
