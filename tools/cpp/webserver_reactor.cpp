/*
 * 程序名：webserver_reactor.cpp
 * 描述：基于 Reactor + 仿Muduo网络库 的气象数据共享平台数据访问接口（V2）
 *
 * 运行：
 *   ./webserver_reactor logfile port
 * 示例：
 *   ./webserver_reactor /log/idc/webserver_reactor.log 8081
 *   /project/tools/bin/procctl 5 /project/tools/bin/webserver_reactor /log/idc/webserver_reactor.log 8081
 *
 * 说明：
 *   - 与旧版 webserver(8080) 并行运行，新版监听 8081。
 *   - IO线程只负责收完整HTTP请求并投递线程池；工作线程负责查库/组XML/回包。
 *   - 请求格式：/api 或 /api2 均可，参数使用 query string：
 *       username=...&passwd=...&intername=...&begintime=...&endtime=...
 *
 * 重要前提（网络库补丁）：
 *   public/network 需要支持 HTTP 分隔符（\r\n\r\n）：
 *   1) Connection 构造函数支持 input_sep，并用它初始化 inputbuffer_
 *   2) TcpServer 增加 input_sep_ + setinputsep()，newconnection() 时把 sep 传给 Connection
 *
 * ★ 本版本修复点（关键）：
 *   1) Oracle OCI bindout 到 std::string 会 resize(len) 导致字符串后面带大量 '\0'：
 *      - 对 selectsql/colstr/bindin/colmeta 做 oci_str_clean(): 截断到第一个 '\0' 并 trim。
 *   2) 绑定输入参数使用 bindin1()，避免 bindin() 触发 resize(len) 污染。
 *   3) bindin 列表解析前先 clean + trim，过滤空 token，保证“无参接口”不误判。
 */

#include <csignal>
#include <unistd.h>

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <climits>
#include <string>
#include <vector>
#include <memory>

#include "_public.h"
#include "_ooci.h"

#include "network/TcpServer.h"
#include "network/ThreadPool.h"

using namespace idc;

// ===================== URL/query 工具 =====================
static inline int hexval(char c)
{
  if (c>='0' && c<='9') return c-'0';
  if (c>='a' && c<='f') return 10 + (c-'a');
  if (c>='A' && c<='F') return 10 + (c-'A');
  return -1;
}

static std::string url_decode(const std::string& s)
{
  std::string out;
  out.reserve(s.size());
  for (size_t i=0;i<s.size();++i)
  {
    if (s[i]=='+') { out.push_back(' '); continue; }
    if (s[i]=='%' && i+2<s.size())
    {
      int a=hexval(s[i+1]), b=hexval(s[i+2]);
      if (a>=0 && b>=0) { out.push_back(char((a<<4)|b)); i+=2; continue; }
    }
    out.push_back(s[i]);
  }
  return out;
}

// 从HTTP请求中提取 query string（? 后面到空格前）
static std::string extract_query(const std::string& http)
{
  // 取第一行：GET /api?... HTTP/1.1
  size_t line_end = http.find("\r\n");
  std::string first = (line_end==std::string::npos) ? http : http.substr(0, line_end);

  size_t qpos = first.find('?');
  if (qpos == std::string::npos) return "";
  size_t sp = first.find(' ', qpos);
  if (sp == std::string::npos) sp = first.size();
  return first.substr(qpos+1, sp-(qpos+1));
}

// ========== 简易 split/trim ==========
static void split_simple(const std::string& s, char delim, std::vector<std::string>& out)
{
  out.clear();
  std::string cur;
  for (char c : s)
  {
    if (c == delim) { out.push_back(cur); cur.clear(); }
    else cur.push_back(c);
  }
  out.push_back(cur);
}

static std::string trim_simple(std::string s)
{
  size_t b = s.find_first_not_of(" \t\r\n");
  size_t e = s.find_last_not_of(" \t\r\n");
  if (b == std::string::npos) return "";
  return s.substr(b, e - b + 1);
}

// ★★★ OCI 字符串清洗：截断 '\0' + trim
static inline void oci_str_clean(std::string &s)
{
  size_t z = s.find('\0');
  if (z != std::string::npos) s.resize(z);
  s = trim_simple(s);
}

// 从 query string 或整段 HTTP 请求中取参数值（兼容旧代码习惯）
static bool getvalue(const std::string &http_or_qs, const std::string &name, std::string &value)
{
  if (name.empty()) return false;

  std::string qs = http_or_qs;

  // 1) 如果包含HTTP头部，只取第一行，避免最后一个参数吞头部。
  size_t rn = qs.find("\r\n");
  if (rn != std::string::npos) qs = qs.substr(0, rn);

  // 2) 标准GET请求行：GET /path?x=1&y=2 HTTP/1.1
  if (qs.find("HTTP/") != std::string::npos || qs.rfind("GET ", 0) == 0)
  {
    qs = extract_query(qs);
  }
  else
  {
    // 3) 兼容 "/api2?x=1&y=2" 或 "x=1&y=2"
    size_t qpos = qs.find('?');
    if (qpos != std::string::npos) qs = qs.substr(qpos + 1);

    // 保险：截断空格
    size_t sp = qs.find(' ');
    if (sp != std::string::npos) qs = qs.substr(0, sp);
  }

  if (qs.empty()) return false;

  // 查找 name=
  std::string key = name + "=";
  size_t p = qs.find(key);
  if (p == std::string::npos) return false;
  p += key.size();

  size_t e = qs.find('&', p);
  if (e == std::string::npos) e = qs.size();

  value = url_decode(qs.substr(p, e - p));
  return true;
}


// ========== XML 标准化：转义 + 统一根节点(response) ==========
// 注意：XML 文本节点必须转义 & < > " '
static inline std::string xml_escape_text(const std::string& in)
{
  std::string out;
  out.reserve(in.size() + in.size()/8 + 16);
  for (unsigned char ch : in)
  {
    switch (ch)
    {
      case '&':  out.append("&amp;");  break;
      case '<':  out.append("&lt;");   break;
      case '>':  out.append("&gt;");   break;
      case '\"': out.append("&quot;"); break;
      case '\'': out.append("&apos;"); break;
      default:   out.push_back((char)ch); break;
    }
  }
  return out;
}
static inline std::string xml_escape_attr(const std::string& in) { return xml_escape_text(in); }

static inline void xml_begin(std::string& out)
{
  out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
  out.append("<response>\n");
}
static inline void xml_end(std::string& out) { out.append("</response>\n"); }

static inline void xml_node(std::string& out, const char* tag, const std::string& value)
{
  out.append("<"); out.append(tag); out.append(">");
  out.append(xml_escape_text(value));
  out.append("</"); out.append(tag); out.append(">\n");
}
static inline void xml_node_i(std::string& out, const char* tag, long long v)
{
  out.append("<"); out.append(tag); out.append(">");
  out.append(std::to_string(v));
  out.append("</"); out.append(tag); out.append(">\n");
}

// 大数据量保护：最大返回行数 / 最大XML体积（可按需调整或改成配置项）
static const long long kMaxRows = 50000;
static const size_t    kMaxXmlBytes = 50 * 1024 * 1024;

// ========== meta 生成：字段中文/单位/缩放（colmeta配置驱动 + 默认兜底） ==========
// colmeta 格式：label|unit|scale,label|unit|scale,... 顺序与 colstr 对齐
struct field_def
{
  const char* name;
  const char* label;
  const char* unit;
  const char* scale;
  const char* type;
  const char* fmt;
};

static const field_def kdefs[] =
{
  // 站点参数（T_ZHOBTCODE 常见字段）
  {"obtid","站号","","1","string",""},
  {"cityname","站名","","1","string",""},
  {"provname","省份","","1","string",""},
  // 注意：你的库里 lat=3427 表示 34.27，所以 scale=0.01；lon 同理
  {"lat","纬度","°","0.01","number",""},
  {"lon","经度","°","0.01","number",""},
  // height=442 表示 44.2，所以 scale=0.1
  {"height","海拔","m","0.1","number",""},

  // 分钟观测（T_ZHOBTMIND1 常见字段）
  {"ddatetime","时间","","1","datetime","yyyymmddhh24miss"},
  {"t","气温","℃","0.1","number",""},
  {"p","气压","hPa","0.1","number",""},
  {"u","相对湿度","%","1","number",""},
  {"wd","风向","°","1","number",""},
  {"wf","风速","m/s","0.1","number",""},
  {"r","降雨量","mm","0.1","number",""},
  {"vis","能见度","m","0.1","number",""},
};

static bool def_lookup(const std::string& name, field_def &out)
{
  for (auto &d : kdefs)
  {
    if (name == d.name) { out = d; return true; }
  }
  return false;
}

static std::string build_meta_xml_from_cfg(const std::string& colstr_in, const std::string& colmeta_in)
{
  std::string colstr = colstr_in;
  std::string colmeta = colmeta_in;
  oci_str_clean(colstr);
  oci_str_clean(colmeta);

  std::vector<std::string> cols;
  split_simple(colstr, ',', cols);
  for (auto &c : cols) c = trim_simple(c);

  std::vector<std::string> metas;
  split_simple(colmeta, ',', metas);
  for (auto &m : metas) m = trim_simple(m);

  std::string xml = "<meta>\n";

  for (size_t i=0; i<cols.size(); ++i)
  {
    std::string label, unit, scale_s;
    const char* type = "string";
    const char* fmt  = "";

    // 先用配置（如果有）
    if (i < metas.size() && !metas[i].empty())
    {
      std::vector<std::string> parts;
      split_simple(metas[i], '|', parts);
      while (parts.size() < 3) parts.push_back("");
      label   = trim_simple(parts[0]);
      unit    = trim_simple(parts[1]);
      scale_s = trim_simple(parts[2]);
    }

    // 再用默认兜底补齐
    field_def d{};
    bool hasd = def_lookup(cols[i], d);

    if (label.empty()) label = hasd ? d.label : cols[i];
    if (unit.empty())  unit  = hasd ? d.unit  : "";
    if (scale_s.empty()) scale_s = hasd ? d.scale : "1";

    if (hasd)
    {
      type = d.type;
      fmt  = d.fmt;
    }
    else
    {
      // 简单推断
      if (cols[i] == "ddatetime") { type = "datetime"; fmt = "yyyymmddhh24miss"; }
      else type = "string";
    }

    xml += "<field name=\"" + xml_escape_attr(cols[i]) + "\"";
    xml += " label=\"" + xml_escape_attr(label) + "\"";
    xml += " unit=\"" + xml_escape_attr(unit) + "\"";
    xml += " scale=\"" + xml_escape_attr(scale_s) + "\"";
    xml += " type=\"" + xml_escape_attr(std::string(type)) + "\"";
    if (fmt && *fmt) xml += " format=\"" + xml_escape_attr(std::string(fmt)) + "\"";
    xml += "/>\n";
  }

  xml += "</meta>";
  return xml;
}

// 关闭信号 + 关闭标准IO（保持与你旧 webserver 同风格）
static void disable_signals_and_io()
{
  for (int ii = 1; ii <= 64; ++ii) signal(ii, SIG_IGN);

  close(0); close(1); close(2);
  signal(SIGINT,  SIG_DFL);
  signal(SIGTERM, SIG_DFL);
}

// ===================== WebServerReactor（单文件应用） =====================
class WebServerReactor
{
public:
  bool init(const char* logfile, int port, int work_threads);
  void start();

private:
  void onMessage(spConnection conn, std::string& request);
  void handleRequest(spConnection conn, std::string request);

  void bizmain(connection &db, const std::string &clientip,
               const std::string &recvbuf, std::string &sendbuf);

  std::string build_http_200_xml(const std::string& body_xml, bool close_conn=true);

private:
  clogfile logfile_;
  int port_ = 8081;
  int work_threads_ = 5;

  std::unique_ptr<TcpServer>  server_;
  std::unique_ptr<ThreadPool> workpool_;
};

bool WebServerReactor::init(const char* logfile, int port, int work_threads)
{
  port_ = port;
  work_threads_ = work_threads;

  if (logfile_.open(logfile, ios::app) == false) return false;

  server_.reset(new TcpServer("0.0.0.0", (uint16_t)port_, 3));        // IO线程数：3
  workpool_.reset(new ThreadPool((size_t)work_threads_, "WORK"));     // WORK线程池

  // HTTP请求：以 \r\n\r\n 作为报文结束标记，所以 in_sep=2
  // HTTP响应：out_sep 必须为 0（原样输出）
  server_->setsep(2, 0);

  server_->setonmessagecb(std::bind(&WebServerReactor::onMessage, this,
                                  std::placeholders::_1,
                                  std::placeholders::_2));
  return true;
}

void WebServerReactor::start()
{
  logfile_.write("webserver_reactor start. port=%d, work_threads=%d\n", port_, work_threads_);
  server_->start();
}

void WebServerReactor::onMessage(spConnection conn, std::string& request)
{
  logfile_.write("onMessage: fd=%d, ip=%s, reqlen=%d\n",
               conn->fd(), conn->ip().c_str(), (int)request.size());
  workpool_->addtask(std::bind(&WebServerReactor::handleRequest, this, conn, request));
}

std::string WebServerReactor::build_http_200_xml(const std::string& body_xml, bool close_conn)
{
  std::string resp;
  resp  = "HTTP/1.1 200 OK\r\n";
  resp += "Server: webserver_reactor\r\n";
  resp += "Content-Type: text/xml; charset=utf-8\r\n";
  resp += std::string("Connection: ") + (close_conn ? "close" : "keep-alive") + "\r\n";
  resp += "Content-Length: " + std::to_string(body_xml.size()) + "\r\n\r\n";
  resp += body_xml;
  return resp;
}

void WebServerReactor::handleRequest(spConnection conn, std::string request)
{
  logfile_.write("handleRequest: fd=%d, ip=%s\n", conn->fd(), conn->ip().c_str());
  const std::string clientip = conn ? conn->ip() : "";

  // 每个工作线程一个DB连接，避免每次请求都connect
  thread_local connection db;
  thread_local bool db_ok = false;

  if (!db_ok)
  {
    if (db.connecttodb("liang/101018@snorcl11g_140", "Simplified Chinese_China.AL32UTF8") != 0)
    {
      logfile_.write("connect database failed: %s\n", db.message());
      std::string resp =
        "HTTP/1.1 500 Internal Server Error\r\n"
        "Content-Type: text/xml; charset=utf-8\r\n"
        "Connection: close\r\n"
        "\r\n"
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<response>\n"
        "<retcode>-1</retcode>\n"
        "<message>database connect failed</message>\n"
        "</response>\n";
      conn->send(resp.c_str(), resp.size());
      return;
    }
    db_ok = true;
  }

  std::string body_xml;
  logfile_.write("RAW REQUEST:\n%s\n", request.c_str());
  bizmain(db, clientip, request, body_xml);

  auto resp = build_http_200_xml(body_xml, true);
  conn->send(resp.c_str(), resp.size());
}

// ===================== bizmain：业务逻辑 =====================
void WebServerReactor::bizmain(connection &db, const std::string &clientip,
                              const std::string &recvbuf, std::string &sendbuf)
{
  std::string username, passwd, intername;
  getvalue(recvbuf, "username", username);
  getvalue(recvbuf, "passwd", passwd);
  getvalue(recvbuf, "intername", intername);

  username = trim_simple(username);
  passwd   = trim_simple(passwd);
  intername= trim_simple(intername);

  logfile_.write("PARSE RESULT: username='%s', passwd='%s', intername='%s'\n",
                 username.c_str(), passwd.c_str(), intername.c_str());

  if (username.empty() || passwd.empty() || intername.empty())
  {
    sendbuf.clear(); xml_begin(sendbuf); xml_node(sendbuf,"retcode","-1"); xml_node(sendbuf,"message","参数不完整：username/passwd/intername"); xml_end(sendbuf);
    return;
  }

  // 1）验证用户名/密码
  sqlstatement stmt(&db);
  std::string bind_ip;

  stmt.prepare("select ip from T_USERINFO where username=:1 and passwd=:2 and rsts=1");
  stmt.bindin1(1, username);
  stmt.bindin1(2, passwd);
  stmt.bindout(1, bind_ip, 50);

  if (stmt.execute() != 0 || stmt.next() != 0)
  {
    sendbuf.clear(); xml_begin(sendbuf); xml_node(sendbuf,"retcode","-1"); xml_node(sendbuf,"message","用户名或密码不正确。"); xml_end(sendbuf);
    return;
  }
  oci_str_clean(bind_ip);

  // 2）权限检查：用户是否有接口访问权限，且接口启用
  int icount = 0;
  stmt.prepare(
    "select count(*) from T_USERANDINTER "
    "where username=:1 and intername=:2 "
    "and intername in (select intername from T_INTERCFG where rsts=1)"
  );
  stmt.bindin1(1, username);
  stmt.bindin1(2, intername);
  stmt.bindout(1, icount);

  if (stmt.execute() != 0 || stmt.next() != 0 || icount == 0)
  {
    sendbuf.clear(); xml_begin(sendbuf); xml_node(sendbuf,"retcode","-1"); xml_node(sendbuf,"message","用户无权限，或接口不存在。"); xml_end(sendbuf);
    return;
  }

  // 3）读取接口配置（SQL/输出列/输入参数名列表/meta）
  std::string selectsql, colstr, bindin, colmeta;
  stmt.prepare("select selectsql,colstr,bindin,colmeta from T_INTERCFG where intername=:1");
  stmt.bindin1(1, intername);
  stmt.bindout(1, selectsql, 1000);
  stmt.bindout(2, colstr, 300);
  stmt.bindout(3, bindin, 300);
  stmt.bindout(4, colmeta, 2000);

  if (stmt.execute() != 0 || stmt.next() != 0)
  {
    sendbuf.clear(); xml_begin(sendbuf); xml_node(sendbuf,"retcode","-1"); xml_node(sendbuf,"message","内部错误（接口配置缺失）。"); xml_end(sendbuf);
    return;
  }

  // ★★★ 关键：清洗 OCI 输出字符串（去 '\0'）
  oci_str_clean(selectsql);
  oci_str_clean(colstr);
  oci_str_clean(bindin);
  oci_str_clean(colmeta);

  logfile_.write("INTERCFG: intername=%s, bindin='%s'\n", intername.c_str(), bindin.c_str());
  logfile_.write("SELECTSQL=[%s]\n", selectsql.c_str());

  // 4）准备并绑定输入参数
  sqlstatement qstmt(&db);
  qstmt.prepare(selectsql);

  std::vector<std::string> in_tokens;
  if (!bindin.empty())
  {
    std::vector<std::string> tmp;
    split_simple(bindin, ',', tmp);
    for (auto &t : tmp)
    {
      std::string p = trim_simple(t);
      if (!p.empty()) in_tokens.push_back(p);
    }
  }

  // 4）读取并绑定输入参数（重要：入参字符串必须保持到 execute 结束）
  std::vector<std::string> in_values;
  if (!in_tokens.empty())
  {
    in_values.resize(in_tokens.size());
    for (size_t i = 0; i < in_tokens.size(); ++i)
    {
      if (!getvalue(recvbuf, in_tokens[i], in_values[i]))
      {
        sendbuf.clear(); xml_begin(sendbuf); xml_node(sendbuf,"retcode","-1"); xml_node(sendbuf,"message",std::string("参数不完整：")+in_tokens[i]); xml_end(sendbuf);
        return;
      }

      in_values[i] = trim_simple(in_values[i]);

      logfile_.write("PARAM[%d] name='%s' value='%s' len=%d\n",
                     (int)i, in_tokens[i].c_str(), in_values[i].c_str(), (int)in_values[i].size());
      for (size_t k=0; k<in_values[i].size() && k<32; ++k)
        logfile_.write("  value[%d]=0x%02X\n", (int)k, (unsigned char)in_values[i][k]);

      if (in_values[i].empty())
      {
        sendbuf.clear(); xml_begin(sendbuf); xml_node(sendbuf,"retcode","-1"); xml_node(sendbuf,"message",std::string("参数不完整：")+in_tokens[i]); xml_end(sendbuf);
        return;
      }

      // ★★★ 关键：bindin1 内部很可能只保存指针；这里用 in_values[i] 保证生命周期
      qstmt.bindin1((int)i + 1, in_values[i]);
    }
  }

  // 5）绑定输出列
  ccmdstr outlist;
  outlist.splittocmd(colstr, ",", true);

  std::vector<std::string> outvals(outlist.size());
  for (int i=0; i<outlist.size(); ++i)
  {
    qstmt.bindout(i+1, outvals[i], 200);
  }

  // 6）执行查询
  if (qstmt.execute() != 0)
  {
    sendbuf.clear(); xml_begin(sendbuf); xml_node(sendbuf,"retcode",std::to_string(qstmt.rc())); xml_node(sendbuf,"message",qstmt.message()); xml_end(sendbuf);
    return;
  }

  // 7）组装 标准 XML（单根节点 + row + 转义 + 大数据量保护）
  sendbuf.clear();
  xml_begin(sendbuf);
  xml_node(sendbuf, "retcode", "0");
  xml_node(sendbuf, "message", "ok");

  // meta
  sendbuf += build_meta_xml_from_cfg(colstr, colmeta);
  sendbuf += "\n<data>\n";

  long long rowcount = 0;
  bool has_more = false;
  bool truncated_by_size = false;

  while (true)
  {
    if (qstmt.next() != 0) break;

    if (rowcount >= kMaxRows) { has_more = true; break; }

    sendbuf += "<row>";
    for (int i=0; i<outlist.size(); ++i)
    {
      sendbuf += "<" + outlist[i] + ">";
      // OCI 绑定到 std::string 时，字符串尾部常带 '\0' 填充；
      // 如果不清理，这些 0 会进入 XML（变成非法字符），并且在网络库用 C 字符串构造时还会被截断。
      std::string v = outvals[i];
      oci_str_clean(v);
      sendbuf += xml_escape_text(v);
      sendbuf += "</" + outlist[i] + ">";
    }
    sendbuf += "</row>\n";

    ++rowcount;

    if (sendbuf.size() > kMaxXmlBytes) { has_more = true; truncated_by_size = true; break; }
  }
  sendbuf += "</data>\n";

  // summary
  sendbuf += "<summary>\n";
  xml_node_i(sendbuf, "rowcount", rowcount);
  xml_node_i(sendbuf, "limit_rows", kMaxRows);
  xml_node_i(sendbuf, "limit_bytes", (long long)kMaxXmlBytes);
  xml_node(sendbuf, "has_more", has_more ? "true" : "false");
  xml_node(sendbuf, "truncated_by_size", truncated_by_size ? "true" : "false");
  sendbuf += "</summary>\n";

  xml_end(sendbuf);


  // 8）写接口调用日志（失败不影响主流程）
  sqlstatement istmt(&db);
  istmt.prepare("insert into T_USERLOG(keyid,username,intername,upttime,ip,rpc) "
                "values(SEQ_USERLOG.nextval,:1,:2,sysdate,:3,:4)");

  std::string ip = clientip;
  // rpc 字段用于记录本次接口返回的记录数，这里写 rowcount（已应用行数上限/体积上限）。
  // 不依赖任何外部上下文变量，避免出现未定义标识符（如 rpc）。
  int irpc = (rowcount > INT_MAX) ? INT_MAX : (int)rowcount;

  istmt.bindin1(1, username);
  istmt.bindin1(2, intername);
  istmt.bindin1(3, ip);
  istmt.bindin(4, irpc);
  istmt.execute();
}

// ===================== main =====================
int main(int argc, char *argv[])
{
  if (argc != 3)
  {
    printf("\n");
    printf("Using :./webserver_reactor logfile port\n\n");
    printf("Sample:./webserver_reactor /log/idc/webserver_reactor.log 8081\n\n");
    printf("        /project/tools/bin/procctl 5 /project/tools/bin/webserver_reactor /log/idc/webserver_reactor.log 8081\n\n");
    return -1;
  }

  disable_signals_and_io();

  WebServerReactor app;
  if (!app.init(argv[1], atoi(argv[2]), 5)) return -1;

  app.start();
  return 0;
}
