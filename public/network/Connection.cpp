#include "Connection.h"

Connection::Connection(EventLoop* loop,
                       std::unique_ptr<Socket> clientsock,
                       uint16_t in_sep,
                       uint16_t out_sep)
    :loop_(loop),
     clientsock_(std::move(clientsock)),
     clientchannel_(new Channel(loop_, clientsock_->fd())),
     inputbuffer_(in_sep),
     outputbuffer_(out_sep),
     disconnect_(false)
{
    // 为新客户端连接准备读事件，并添加到epoll中。
    clientchannel_->setreadcallback(std::bind(&Connection::onmessage,this));
    clientchannel_->setclosecallback(std::bind(&Connection::closecallback,this));
    clientchannel_->seterrorcallback(std::bind(&Connection::errorcallback,this));
    clientchannel_->setwritecallback(std::bind(&Connection::writecallback,this));
    clientchannel_->useet();                 // 客户端连上来的fd采用边缘触发。
    clientchannel_->enablereading();   // 让epoll_wait()监视clientchannel的读事件
}

Connection::~Connection()
{
    // printf("conn已析构。\n");
}

int Connection::fd() const                              // 返回客户端的fd。
{
    return clientsock_->fd();
}

std::string Connection::ip() const                   // 返回客户端的ip。
{
    return clientsock_->ip();
}

uint16_t Connection::port() const                  // 返回客户端的port。
{
    return clientsock_->port();
}

void Connection::closecallback()                    // TCP连接关闭（断开）的回调函数，供Channel回调。
{
    disconnect_=true;
    clientchannel_->remove();                         // 从事件循环中删除Channel。
    closecallback_(shared_from_this());
}

void Connection::errorcallback()                    // TCP连接错误的回调函数，供Channel回调。
{
    disconnect_=true;
    clientchannel_->remove();                  // 从事件循环中删除Channel。
    errorcallback_(shared_from_this());     // 回调TcpServer::errorconnection()。
}

// 设置关闭fd_的回调函数。
void Connection::setclosecallback(std::function<void(spConnection)> fn)    
{
    closecallback_=fn;     // 回调TcpServer::closeconnection()。
}

// 设置fd_发生了错误的回调函数。
void Connection::seterrorcallback(std::function<void(spConnection)> fn)    
{
    errorcallback_=fn;     // 回调TcpServer::errorconnection()。
}

// 设置处理报文的回调函数。
void Connection::setonmessagecallback(std::function<void(spConnection,std::string&)> fn)    
{
    onmessagecallback_=fn;       // 回调TcpServer::onmessage()。
}

// 发送数据完成后的回调函数。
void Connection::setsendcompletecallback(std::function<void(spConnection)> fn)    
{
    sendcompletecallback_=fn;
}

// 处理对端发送过来的消息。
void Connection::onmessage()
{
    char buffer[1024];
    while (true)             // 由于使用非阻塞IO，一次读取buffer大小数据，直到全部的数据读取完毕。
    {    
        bzero(&buffer, sizeof(buffer));
        ssize_t nread = read(fd(), buffer, sizeof(buffer));
        if (nread > 0)      // 成功的读取到了数据。
        {
            inputbuffer_.append(buffer,nread);      // 把读取的数据追加到接收缓冲区中。
        } 
        else if (nread == -1 && errno == EINTR) // 读取数据的时候被信号中断，继续读取。
        {  
            continue;
        } 
        else if (nread == -1 && ((errno == EAGAIN) || (errno == EWOULDBLOCK))) // 全部的数据已读取完毕。
        {
            std::string message;

            while (true)             // 从接收缓冲区中拆分出客户端的请求消息。
            {
                if (inputbuffer_.pickmessage(message)==false) break;

                // printf("message (fd=%d):%s\n",fd(),message.c_str());
                lastatime_=Timestamp::now();             // 更新Connection的时间戳。

                onmessagecallback_(shared_from_this(),message);       // 回调TcpServer::onmessage()处理客户端的请求消息。
            }
            break;
        } 
        else if (nread == 0)  // 客户端连接已断开。
        {  
            closecallback();                                  // 回调TcpServer::closecallback()。
            break;
        }
    }
}

// 发送数据，不管在任何线程中，都是调用此函数发送数据。
void Connection::send(const char *data,size_t size)          
{
    if (disconnect_==true) {  printf("客户端连接已断开了，send()直接返回。\n"); return;}

    // 因为数据要发送给其它线程处理，所以，把它包装成智能指针。
    // ★必须保留 size：HTTP 响应/二进制数据可能包含 '\0'，用 std::string(data) 会被截断。
    std::shared_ptr<std::string> message(new std::string(data, size));

    if (loop_->isinloopthread())   // 判断当前线程是否为事件循环线程（IO线程）。
    {
        // 如果当前线程是IO线程，直接调用sendinloop()发送数据。
        // printf("send() 在事件循环的线程中。\n");
        sendinloop(message);
    }
    else
    {
        // 如果当前线程不是IO线程，调用EventLoop::queueinloop()，把sendinloop()交给事件循环线程去执行。
        // printf("send() 不在事件循环的线程中。\n");
        loop_->queueinloop(std::bind(&Connection::sendinloop,this,message));
    }
}

// 发送数据，如果当前线程是IO线程，直接调用此函数，如果是工作线程，将把此函数传给IO线程去执行。
/*
void Connection::sendinloop(const char *data,size_t size)
{
    outputbuffer_.appendwithsep(data,size);    // 把需要发送的数据保存到Connection的发送缓冲区中。
    clientchannel_->enablewriting();    // 注册写事件。
}
*/
void Connection::sendinloop(std::shared_ptr<std::string> data)
{
    outputbuffer_.appendwithsep(data->data(),data->size());    // 把需要发送的数据保存到Connection的发送缓冲区中。
    clientchannel_->enablewriting();    // 注册写事件。
}

// 处理写事件的回调函数，供Channel回调。
void Connection::writecallback()                   
{
    // ★ET(边缘触发) 模式下：必须把能写的数据尽可能写完，否则可能收不到下一次写事件。
    while (outputbuffer_.size() > 0)
    {
        ssize_t writen = ::send(fd(), outputbuffer_.data(), outputbuffer_.size(), 0);
        if (writen > 0)
        {
            outputbuffer_.erase(0, (size_t)writen);
            continue;
        }

        if (writen == -1 && errno == EINTR) continue;  // 被信号中断，重试。

        if (writen == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            // 发送缓冲区已满，等待下一次可写事件。
            break;
        }

        // 其它错误：交给 errorcallback() 走统一的清理逻辑。
        errorcallback();
        return;
    }

    // 如果发送缓冲区中没有数据了，表示数据已发送完成，不再关注写事件。
    if (outputbuffer_.size() == 0)
    {
        clientchannel_->disablewriting();
        if (sendcompletecallback_) sendcompletecallback_(shared_from_this());
    }
}

 // 判断TCP连接是否超时（空闲太久）。
 bool Connection::timeout(time_t now,int val)           
 {
    return now-lastatime_.toint()>val;    
 }
