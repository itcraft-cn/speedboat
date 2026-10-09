#!/usr/bin/env bash
#
# Speedboat 三机 VirtualBox 实机验证编排。
#
# 覆盖（官方门面路径 Speedboat.start）：
#   P1 三节点选主一致性（唯一 Leader / leader 字段全网一致 / nodeId 自动推导）
#   P2 命名锁互斥（epoch 唯一、多成员可持有、获取-释放成对）
#   P3 Leader 故障转移（kill -9 主节点 → 重选，term 递增、仍唯一）
#   P4 原 Leader 重加入（重启后转为 Follower，集群仍唯一 Leader）
#   P5 mmap 持久化挂回（优雅停/重启后 term 不回退、mmap 文件非空）
#
# 前置：三台 VM 已互信、Java8(Dragonwell) 就绪、互相可达（见 config-vm3.properties）。
# 依赖：tools/vm/vm-test-lib.sh（SSH 通道、日志解析、唯一 Leader 判定、断言汇总）。
# 用法：bash tools/vm/vm-cluster-test.sh [deploy|run|stop|all]
#   deploy  仅下发资产并在远端编译测试节点
#   run     仅执行测试（假定已部署）
#   stop    停止所有节点
#   all     先 deploy 再 run（默认）
#
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"

# ---- 环境清单（source 公共库之前须定义）----
SSH_USER=vboxuser
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no)
SCP=(scp -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no)
HOSTS=(vboxdeb001 vboxdeb002 vboxdeb003)
declare -A IP=(
  [vboxdeb001]=192.168.77.101
  [vboxdeb002]=192.168.77.102
  [vboxdeb003]=192.168.77.103
)
declare -A PORT=([vboxdeb001]=21001 [vboxdeb002]=21001 [vboxdeb003]=21001)

# 公共库：SSH 通道、结构化日志解析、存活探测、唯一 Leader 等待、断言计数与汇总
. "$REPO/tools/vm/vm-test-lib.sh"

CONFIG=config-vm3.properties
LOCK=vm-lock
JAR="$REPO/target/speedboat-1.0-SNAPSHOT.jar"
LOGBACK="$REPO/tools/vm/logback-vm.xml"
CTL="$REPO/tools/vm/vm_node_ctl.sh"
RUNNER="$REPO/src/test/java/cn/itcraft/speedboat/sample/VmClusterNode.java"

deploy() {
  log "P0 部署资产到三台 VM"
  [ -f "$JAR" ] || { log "缺少 jar：$JAR（先 mvn package）"; exit 1; }
  for h in "${HOSTS[@]}"; do
    rsh "$h" 'mkdir -p ~/speedboat-test/{logs,run,runner}' || { log "$h mkdir 失败"; exit 1; }
    scph "$h" "$JAR"     "~/speedboat-test/speedboat.jar"
    scph "$h" "$REPO/$CONFIG" "~/speedboat-test/$CONFIG"
    scph "$h" "$LOGBACK" "~/speedboat-test/logback-vm.xml"
    scph "$h" "$CTL"     "~/speedboat-test/vm_node_ctl.sh"
    scph "$h" "$RUNNER"  "~/speedboat-test/VmClusterNode.java"
    rsh "$h" 'chmod +x ~/speedboat-test/vm_node_ctl.sh; cd ~/speedboat-test && ~/lang/dragonwell-8.29.28/bin/javac -cp speedboat.jar -d runner VmClusterNode.java' \
      || { log "$h 编译失败"; exit 1; }
    echo "  $h 部署+编译 OK"
  done
}

start_node() { rsh "$1" "~/speedboat-test/vm_node_ctl.sh start ${IP[$1]} $CONFIG run $LOCK $(rlog "$1")"; }
stop_all() { for h in "${HOSTS[@]}"; do rsh "$h" '~/speedboat-test/vm_node_ctl.sh stop' >/dev/null 2>&1; done; }

# 本环境只有一个集群组，直接委托公共库（按全量 HOSTS 判定）
leader_host() { leader_host_in "${HOSTS[@]}"; }

phase1_election() {
  echo; log "P1 三节点选主"
  for h in "${HOSTS[@]}"; do stop_all >/dev/null 2>&1; done
  for h in "${HOSTS[@]}"; do rsh "$h" '~/speedboat-test/vm_node_ctl.sh clean' >/dev/null 2>&1; done
  for h in "${HOSTS[@]}"; do start_node "$h" >/dev/null; done
  sleep 3
  if ! wait_single_leader 25 "${HOSTS[@]}"; then check "集群在 25s 内选出唯一 Leader" false; return; fi
  check "集群在 25s 内选出唯一 Leader" true

  local leaders="" mains=0 main_node="" nodes_ok=1
  for h in "${HOSTS[@]}"; do
    local line nid term L im
    line=$(state_line "$h"); nid=$(fld "$line" node); term=$(fld "$line" term); L=$(fld "$line" leader); im=$(fld "$line" isMain)
    echo "  $h: node=$nid term=$term leader=$L isMain=$im"
    leaders="$leaders $L"
    [ "$im" = "true" ] && { mains=$((mains+1)); main_node=$nid; }
    [ "$nid" = "node-${IP[$h]}-${PORT[$h]}" ] || nodes_ok=0
  done
  check "三节点 nodeId 均按 ip:port 确定性推导" [ "$nodes_ok" = "1" ]
  check "leader 字段全网一致" [ "$(echo "$leaders" | tr ' ' '\n' | grep -v '^$' | sort -u | grep -c .)" -eq 1 ]
  check "恰好一个节点 isMain=true" [ "$mains" -eq 1 ]
  check "isMain 节点即为全网 leader" [ "$main_node" = "$(echo "$leaders" | tr ' ' '\n' | grep -v '^$' | head -1)" ]
}

phase2_lock() {
  echo; log "P2 命名锁（观察 16s 竞争）"
  sleep 16
  local all_epochs="" all_nodes="" total_acq=0 total_rel=0 pair_fail=0
  for h in "${HOSTS[@]}"; do
    local acq rel
    acq=$(rsh "$h" "grep -o 'event=LOCK_ACQUIRE node=[^ ]* lock=[^ ]* epoch=[0-9]*' ~/speedboat-test/$(rlog "$h") 2>/dev/null")
    rel=$(rsh "$h" "grep -o 'event=LOCK_RELEASE node=[^ ]* lock=[^ ]* epoch=[0-9]*' ~/speedboat-test/$(rlog "$h") 2>/dev/null")
    local a_cnt r_cnt
    a_cnt=$(echo "$acq" | grep -c 'LOCK_ACQUIRE' || true)
    r_cnt=$(echo "$rel" | grep -c 'LOCK_RELEASE' || true)
    total_acq=$((total_acq+a_cnt)); total_rel=$((total_rel+r_cnt))
    echo "  $h: acquire=$a_cnt release=$r_cnt"
    # 采样瞬间至多 1 个在途持有（正处于 LOCK_HOLD 窗口）；差 >1 即释放缺失
    [ $((a_cnt - r_cnt)) -gt 1 ] && pair_fail=1
    all_epochs="$all_epochs $(echo "$acq" | grep -o 'epoch=[0-9]*' | grep -o '[0-9]*')"
    all_nodes="$all_nodes $(echo "$acq" | grep -o 'node=[^ ]*' | sed 's/node=//')"
  done
  local uniq_dup distinct
  uniq_dup=$(echo "$all_epochs" | tr ' ' '\n' | grep -v '^$' | sort | uniq -d)
  distinct=$(echo "$all_nodes" | tr ' ' '\n' | grep -v '^$' | sort -u | grep -c .)
  check "产生了锁获取事件（>0）" [ "$total_acq" -gt 0 ]
  check "fencing epoch 全局唯一（无重复授权）" [ -z "$uniq_dup" ]
  check "至少 2 个不同成员成功持锁（任意成员可持有）" [ "$distinct" -ge 2 ]
  check "各节点获取-释放差 ≤1（仅持有窗口内在途）" [ "$pair_fail" -eq 0 ]
}

phase3_failover() {
  echo; log "P3 Leader 故障转移"
  local lh; lh=$(leader_host)
  if [ -z "$lh" ]; then check "故障转移前存在 leader" false; return; fi
  local old_line old_term old_leader
  old_line=$(state_line "$lh"); old_term=$(fld "$old_line" term); old_leader=$(fld "$old_line" leader)
  echo "  当前 leader=$old_leader host=$lh term=$old_term"
  log "  kill -9 $lh 模拟崩溃"
  rsh "$lh" '~/speedboat-test/vm_node_ctl.sh kill9' >/dev/null
  if wait_single_leader 25 "${HOSTS[@]}"; then check "主节点崩溃后重新选出唯一 Leader" true; else check "主节点崩溃后重新选出唯一 Leader" false; return; fi
  local nh; nh=$(leader_host)
  local new_line new_term new_leader
  new_line=$(state_line "$nh"); new_term=$(fld "$new_line" term); new_leader=$(fld "$new_line" leader)
  echo "  新 leader=$new_leader host=$nh term=$new_term"
  check "新 Leader 与原 Leader 不同" [ "$new_leader" != "$old_leader" ]
  check "新任期严格递增（term 增长）" [ "$new_term" -gt "$old_term" ]
  # 存活节点中恰好一个 isMain
  local mains=0
  for h in "${HOSTS[@]}"; do alive "$h" || continue; [ "$(fld "$(state_line "$h")" isMain)" = "true" ] && mains=$((mains+1)); done
  check "存活节点中恰好一个 isMain（无脑裂）" [ "$mains" -eq 1 ]
}

phase4_rejoin() {
  echo; log "P4 原 Leader 重加入"
  local down=""
  for h in "${HOSTS[@]}"; do alive "$h" || down="$h"; done
  [ -z "$down" ] && { check "存在被杀的节点可供重加入" false; return; }
  log "  重启 $down"
  start_node "$down" >/dev/null
  sleep 10
  check "被重启节点存活" alive "$down"
  if wait_single_leader 15 "${HOSTS[@]}"; then check "重加入后集群仍唯一 Leader" true; else check "重加入后集群仍唯一 Leader" false; return; fi
  local line L im nid
  line=$(state_line "$down"); L=$(fld "$line" leader); im=$(fld "$line" isMain); nid=$(fld "$line" node)
  echo "  $down: node=$nid leader=$L isMain=$im"
  check "原 Leader 重加入后已降级为 Follower" [ "$im" = "false" ]
}

phase5_persistence() {
  echo; log "P5 mmap 持久化挂回"
  local lh; lh=$(leader_host)
  local fh=""
  for h in "${HOSTS[@]}"; do [ "$h" != "$lh" ] && { fh=$h; break; }; done
  [ -z "$fh" ] && { check "存在 Follower 节点用于持久化验证" false; return; }
  local line T nid
  line=$(state_line "$fh"); T=$(fld "$line" term); nid=$(fld "$line" node)
  echo "  目标 Follower=$fh node=$nid term=$T"
  local size
  size=$(rsh "$fh" "stat -c %s ~/speedboat-test/data/$nid/raft.mmap 2>/dev/null || echo 0")
  check "mmap 数据文件存在且非空（$nid）" [ "${size:-0}" -gt 0 ]
  log "  优雅停止 $fh（SIGTERM，触发 mmap 收尾）"
  rsh "$fh" '~/speedboat-test/vm_node_ctl.sh stop' >/dev/null
  sleep 1
  log "  重启 $fh"
  start_node "$fh" >/dev/null
  sleep 8
  check "重启后节点存活" alive "$fh"
  local nT
  nT=$(fld "$(state_line "$fh")" term)
  echo "  重启后 term=$nT（停机前 term=$T）"
  check "重启后 term 不回退（mmap 挂回，非从 1 重来）" [ "${nT:-0}" -ge "${T:-0}" ]
  if wait_single_leader 15 "${HOSTS[@]}"; then check "重启后集群仍唯一 Leader" true; else check "重启后集群仍唯一 Leader" false; fi
}

run_all() {
  phase1_election
  phase2_lock
  phase3_failover
  phase4_rejoin
  phase5_persistence
  echo; log "P6 清理"
  stop_all
  report
}

case "${1:-all}" in
  deploy) deploy ;;
  run)    run_all ;;
  stop)   stop_all ;;
  all)    deploy && run_all ;;
  *) echo "usage: $0 [deploy|run|stop|all]"; exit 64 ;;
esac
