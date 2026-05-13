#!/usr/bin/env bash
set -euo pipefail

U="liang"
P="101018"
HOST8080="http://127.0.0.1:8080"
HOST8081="http://127.0.0.1:8081"

# 你常用的一个站号，按需改
OBTID="58015"

# 1) 基础：缺参应该返回标准XML错误
echo "==[1] missing params (8081) =="
curl -s "$HOST8081/api" | head -n 20
echo

# 2) 小范围（更容易命中）
echo "==[2] mind3 10min (8080) =="
curl -s "$HOST8080/api?username=$U&passwd=$P&intername=getzhobtmind3&obtid=$OBTID&begintime=20250109120000&endtime=20260110121000" | head -n 120
echo

echo "==[3] mind3 10min (8081) =="
curl -v "$HOST8081/api?username=$U&passwd=$P&intername=getzhobtmind3&obtid=$OBTID&begintime=20250109120000&endtime=20260110121000" -o /tmp/r_8081.xml || true
echo "saved to /tmp/r_8081.xml"
echo

# 3) 放大范围确认是否有数据（一天）
echo "==[4] mind3 1day (8080) =="
curl -s "$HOST8080/api?username=$U&passwd=$P&intername=getzhobtmind3&obtid=$OBTID&begintime=20260109110000&endtime=20260109120000" | grep -E "<rowcount>|<has_more>|<truncated_by_size>" -n || true
echo

# 4) 大数据保护（mind2 2days）
echo "==[5] mind2 big range (8080) =="
curl -s "$HOST8080/api?username=$U&passwd=$P&intername=getzhobtmind2&begintime=20260101000000&endtime=20260130000000" | grep -E "<rowcount>|<has_more>|<truncated_by_size>" -n || true
echo

