#!/usr/bin/env bash
#
# Speedboat 实机验证编排公共库（被 vm-cluster-test.sh / vm-crossdc-test.sh source）。
#
# 提供与具体环境无关的能力：
#   - SSH 通道（SSH_USER 由调用方指定）
#   - 结构化日志解析（event=STATE 行的 key=value 取值）
#   - 远端进程存活探测
#   - "给定 host 集合内唯一 Leader"的轮询等待与定位
#   - 断言计数与最终汇总报告
#
# 调用方约定（source 之前必须定义）：
#   SSH_USER   远端 ssh 用户名，如 vboxuser / tomcat
#   HOSTS      全部 host 短名数组（含序，用于遍历）
#   IP         关联数组：host 短名 -> IP，用于 nodeId 校验与 start
#   PORT       关联数组：host 短名 -> 端口，用于 nodeId 校验（可省略，届时跳过该校验）
#   SSH / SCP  ssh / scp 参数数组（须在 source 之前定义）
#   host_target() 可选覆盖：短名 -> SSH 目标（默认原样返回）
#
# 注意：本文件被 source，因此不单独声明 shebang 语义；调用方脚本应先 set -uo pipefail。
set -uo pipefail

PASS=0; FAIL=0; declare -a FAILURES

ts() { date +%H:%M:%S; }
log() { echo "[$(ts)] $*"; }

# 断言：check <描述> <命令...>；命令退出码 0 记 PASS
check() {
  local desc=$1; shift
  if "$@"; then PASS=$((PASS+1)); echo "  [PASS] $desc"
  else FAIL=$((FAIL+1)); FAILURES+=("$desc"); echo "  [FAIL] $desc"; fi
}

# 短名 -> SSH 目标解析。默认原样返回（适用于 vboxdeb001 这类可直接解析的主机名）；
# 调用方可覆盖本函数以支持别名（如 h174 -> 192.168.193.174）。
host_target() { echo "$1"; }

# 远端执行：rsh <host> <命令...>
rsh() { local h=$1; shift; "${SSH[@]}" "${SSH_USER}@$(host_target "$h")" "$@"; }

# 远端下发：scph <host> <本地文件> <远端路径（可含 ~）>
scph() {
  local h=$1 local_file=$2 remote_path=$3
  "${SCP[@]}" "$local_file" "${SSH_USER}@$(host_target "$h"):${remote_path}"
}

# 从一行结构化日志中取 key=value（如 node / term / leader / isMain / dc）
fld() { echo "$1" | sed -n "s/.*[[:space:]]$2=\([^[:space:]]*\).*/\1/p"; }

# 各节点日志在远端的相对路径
rlog() { echo "logs/$1.log"; }

# 最近一条 STATE 行（进程已死时返回的是其最后一行，仍可用于读取 term/leader）
state_line() { rsh "$1" "grep 'event=STATE' ~/speedboat-test/$(rlog "$1") 2>/dev/null | tail -1"; }

# 进程存活探测
alive() { [ "$(rsh "$1" '~/speedboat-test/vm_node_ctl.sh alive' 2>/dev/null)" = "alive" ]; }

# 轮询等待"给定 host 集合内恰好一个 isMain=true，且集合内各存活节点 leader 字段一致"。
# 用法：wait_single_leader <秒> <host...>；命中返回 0，超时返回 1。
#
# 判定口径用 isMain 而非 leader 字段：kill 之后残留的 leader 字段可能仍指向已死节点，
# 若仅按 leader 字段判唯一，会把"全网仍同意一个已死节点"误判为已选出新主。
wait_single_leader() {
  local secs=$1; shift
  local -a hs=("$@")
  local i h
  for ((i = 0; i < secs; i++)); do
    local mains=0 mnode=""
    for h in "${hs[@]}"; do
      alive "$h" || continue
      local line im
      line=$(state_line "$h"); im=$(fld "$line" isMain)
      if [ "$im" = "true" ]; then mains=$((mains + 1)); mnode=$(fld "$line" node); fi
    done
    if [ "$mains" -eq 1 ]; then
      local ok=1
      for h in "${hs[@]}"; do
        alive "$h" || continue
        [ "$(fld "$(state_line "$h")" leader)" = "$mnode" ] || ok=0
      done
      [ "$ok" = "1" ] && return 0
    fi
    sleep 1
  done
  return 1
}

# 返回给定 host 集合内当前 isMain=true 的 host（空表示未定）
leader_host_in() {
  local -a hs=("$@")
  local h
  for h in "${hs[@]}"; do
    alive "$h" || continue
    [ "$(fld "$(state_line "$h")" isMain)" = "true" ] && { echo "$h"; return; }
  done
  echo ""
}

# 给定 host 集合内 isMain=true 的数量
count_main_in() {
  local -a hs=("$@")
  local h n=0
  for h in "${hs[@]}"; do
    alive "$h" || continue
    [ "$(fld "$(state_line "$h")" isMain)" = "true" ] && n=$((n + 1))
  done
  echo "$n"
}

# ------------------------------------------------------------------
# 跨机房级联（双层 Raft）专用判据
#
# 双层结构下"机房内唯一代表"与"全网唯一主"是两个不同判据，不能混用：
#   子组（机房内层）→ intra=true 表示本进程是该机房推举的代表
#   父组（跨机房层）→ isMain=true 表示既是子组代表、又在父组当选（全局主）
# 备机房在主机房存活期间长期 intra=true 但 isMain=false，属预期形态而非故障。
#
# 旧的 wait_single_leader 要求集合内所有节点的 leader 字段等于全局主，
# 在双层下会因备机房子组 leader 与全局主不同而永不命中，故单机房三机脚本
# 继续使用旧函数，跨机房脚本改用本节函数。
# ------------------------------------------------------------------

# 给定 host 集合内 intra=true 的数量（机房代表计数）
count_intra_in() {
  local -a hs=("$@")
  local h n=0
  for h in "${hs[@]}"; do
    alive "$h" || continue
    [ "$(fld "$(state_line "$h")" intra)" = "true" ] && n=$((n + 1))
  done
  echo "$n"
}

# 返回给定 host 集合内当前 intra=true 的 host（空表示该机房暂无代表）
intra_host_in() {
  local -a hs=("$@")
  local h
  for h in "${hs[@]}"; do
    alive "$h" || continue
    [ "$(fld "$(state_line "$h")" intra)" = "true" ] && { echo "$h"; return; }
  done
  echo ""
}

# 轮询等待"给定 host 集合内恰好一个 intra=true"（机房内唯一代表）。
# 用法：wait_single_intra <秒> <host...>；命中返回 0，超时返回 1。
# 判定用 intra 而非 isMain：杀光对侧机房后本机房仍需有代表，但不再持有全局席位。
wait_single_intra() {
  local secs=$1; shift
  local -a hs=("$@")
  local i h
  for ((i = 0; i < secs; i++)); do
    local n=0
    for h in "${hs[@]}"; do
      alive "$h" || continue
      [ "$(fld "$(state_line "$h")" intra)" = "true" ] && n=$((n + 1))
    done
    [ "$n" -eq 1 ] && return 0
    sleep 1
  done
  return 1
}

# 轮询等待"全网恰好一个 isMain=true，且所有存活节点的父组 Leader 视角收敛一致"。
# 用法：wait_single_global_leader <秒> <host...>；命中返回 0，超时返回 1。
#
# 两级判据缺一不可：
#   1) isMain 计数 == 1          → 唯一性（防双主）
#   2) parentLeader 全网一致     → 父组已收敛到同一 leader，而非两房各持一词
# 仅判计数可能在父组尚未收敛的中间态误通过，故补第 2 条。
wait_single_global_leader() {
  local secs=$1; shift
  local -a hs=("$@")
  local i h
  for ((i = 0; i < secs; i++)); do
    local mains=0 ref=""
    for h in "${hs[@]}"; do
      alive "$h" || continue
      local line pl
      line=$(state_line "$h")
      if [ "$(fld "$line" isMain)" = "true" ]; then mains=$((mains + 1)); fi
      pl=$(fld "$line" parentLeader)
      [ -n "$pl" ] || continue
      if [ -z "$ref" ]; then ref=$pl; elif [ "$pl" != "$ref" ]; then ref="DIVERGED"; fi
    done

    if [ "$mains" -eq 1 ] && [ -n "$ref" ] && [ "$ref" != "DIVERGED" ] && [ "$ref" != "none" ]; then
      # 父组 Leader 必须以父组端口身份结尾（子组端口 2100x → 父组 2200x，两组身份分离）。
      # 六机拓扑各节点父组端口均为 22001；vbox 分区拓扑中 AP 接管者可能是
      # 宿主任一进程（父组端口 22001/22002/22003），故须接受全部父组端口形态。
      case "$ref" in
        *-22001|*-22002|*-22003) return 0 ;;
        *) ;;
      esac
    fi
    sleep 1
  done
  return 1
}

# 等待"全网无主"（isMain 计数 == 0）并保持稳定 <稳定秒>。
# 用于断言杀光主机房后退化为无主、且不是短暂抖动。
# 用法：wait_no_global_leader <保持秒> <超时秒> <host...>
wait_no_global_leader() {
  local hold=$1 timeout=$2; shift 2
  local -a hs=("$@")
  local i h seen=0
  for ((i = 0; i < timeout; i++)); do
    local n=0
    for h in "${hs[@]}"; do
      alive "$h" || continue
      [ "$(fld "$(state_line "$h")" isMain)" = "true" ] && n=$((n + 1))
    done
    if [ "$n" -eq 0 ]; then
      seen=$((seen + 1))
      [ "$seen" -ge "$hold" ] && return 0
    else
      # 期间出现主即判定未达成（对侧机房已死却仍报主 = 违反唯一性预期）
      return 1
    fi
    sleep 1
  done
  return 1
}

# nodeId 推导校验：node-<ip>-<port>（身份确定性）
node_id_ok() {
  local h=$1 nid
  nid=$(fld "$(state_line "$h")" node)
  [ "$nid" = "node-${IP[$h]}-${PORT[$h]}" ]
}

# 最终汇总报告；全部通过返回 0
report() {
  echo
  echo "========================================"
  echo "实机验证结果： $PASS 通过, $FAIL 失败"
  if [ "$FAIL" -gt 0 ]; then
    echo "失败项："
    local f
    for f in "${FAILURES[@]}"; do echo "  - $f"; done
  fi
  echo "========================================"
  [ "$FAIL" -eq 0 ]
}
