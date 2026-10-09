#!/usr/bin/env bash
#
# Speedboat 六节点双机房（跨机房级联）实机验证编排。
#
# 拓扑（config-vm6-crossdc.properties）：
#   主机房 机房0 = 192.168.193.174 / .175 / .176（aarch64）
#   备机房 机房1 = 192.168.193.51 / .53 / .55（x86_64）
# 两组在 cross-datacenter 模式下各自独立成组（见 Speedboat.doStartCrossDatacenterMode）。
#
# 覆盖（官方门面路径 Speedboat.start，逐项断言 + 拓扑探测）：
#   P0 基线    两机房各选出唯一 Leader；nodeId / 机房归属正确
#   P1 场景1   杀主机房 Leader → 主机房重选（优先在主机房），备机房不受牵动
#   P2 场景2   杀光主机房 → 备机房仍维持唯一 Leader
#   P3 场景3   重启主机房 → 重新加入并选主，恢复双 Leader 形态
#   P4 场景4   杀光备机房 → 主机房仍维持唯一 Leader
#   P5 拓扑探测 由 P0/P1/P2 证据判定两机房是否真有跨机房联动
#
# 前置：六台已单向信任、互相可达；主机房（17x）无 javac，故测试节点由宿主机
#       预编译为字节码（架构无关）后下发，目标机仅需 java。
# 用法：bash tools/vm/vm-crossdc-test.sh [deploy|run|stop|all]
#   deploy  宿主机预编译测试节点 + 下发六台
#   run     执行 P0~P5（假定已部署）
#   stop    停止所有节点
#   all     先 deploy 再 run（默认）
#
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"

# ---- 环境清单（source 公共库之前须定义）----
SSH_USER=tomcat
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no
      -o ControlMaster=auto -o ControlPath=/tmp/sb-vmtest-%r@%h -o ControlPersist=180)
SCP=(scp -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no
      -o ControlMaster=auto -o ControlPath=/tmp/sb-vmtest-%r@%h -o ControlPersist=180)

# 机房0（主机房）
DC0=(h174 h175 h176)
# 机房1（备机房）
DC1=(h51 h53 h55)
HOSTS=("${DC0[@]}" "${DC1[@]}")

declare -A IP=(
  [h174]=192.168.193.174
  [h175]=192.168.193.175
  [h176]=192.168.193.176
  [h51]=192.168.193.51
  [h53]=192.168.193.53
  [h55]=192.168.193.55
)
declare -A PORT=(
  [h174]=21001 [h175]=21001 [h176]=21001
  [h51]=21001 [h53]=21001 [h55]=21001
)
# host -> 机房索引（0=主机房，1=备机房）
declare -A DC=([h174]=0 [h175]=0 [h176]=0 [h51]=1 [h53]=1 [h55]=1)

# 公共库：SSH 通道、结构化日志解析、存活探测、唯一 Leader 等待、断言计数与汇总
. "$REPO/tools/vm/vm-test-lib.sh"

# 本清单用短别名（h174/h51）便于书写，需解析为真实 IP 才能 ssh/scp
host_target() { echo "${IP[$1]:-$1}"; }

CONFIG=config-vm6-crossdc.properties
LOCK=vm-lock
JAR="$REPO/target/speedboat-1.0-SNAPSHOT.jar"
LOGBACK="$REPO/tools/vm/logback-vm.xml"
CTL="$REPO/tools/vm/vm_node_ctl.sh"
RUNNER_SRC="$REPO/src/test/java/cn/itcraft/speedboat/sample/VmClusterNode.java"

# 宿主机预编译输出（字节码架构无关，17x 目标机无 javac）
JDK_HOME="${SB_JDK_HOME:-$HOME/lang/dragonwell-8.29.28}"
RUNNER_DIR="$REPO/target/vm-runner"
RUNNER_JAR="$REPO/target/vm-runner.jar"

# ------------------------------------------------------------------ 跨阶段状态
# 由各 phase 在观测点写入，供 P5 拓扑探测汇总口径使用
BASELINE_MAIN=0     # P0 基线全网 isMain 数
NO_COUPLING_P1=0    # P1 杀主机房 Leader 后备机房 leader/term 均未变 -> 1
NO_COUPLING_P2=0    # P2 主机房全灭后备机房 leader/term 均未变 -> 1
BAK_NODE_P2=""      # P2 结束时备机房 Leader 节点，供 P3 断言"未被打断"
MAIN_LAST_TERM=0    # 主机房最后一次可观测 term（P1/P2 后记录，P3 用于 mmap 挂回断言）

# ============================ 机房维度辅助 ============================
dc_leader_host() { case "$1" in 0) leader_host_in "${DC0[@]}";; *) leader_host_in "${DC1[@]}";; esac; }
dc_wait_leader() { case "$1" in 0) wait_single_leader "$2" "${DC0[@]}";; *) wait_single_leader "$2" "${DC1[@]}";; esac; }
dc_count_main() { case "$1" in 0) count_main_in "${DC0[@]}";; *) count_main_in "${DC1[@]}";; esac; }

# 全网 isMain 总数
total_main() { echo "$(( $(dc_count_main 0) + $(dc_count_main 1) ))"; }

# 节点启动时 event=START 行（含 dc= 机房归属，STATE 行不含该字段）
start_line() { rsh "$1" "grep 'event=START' ~/speedboat-test/$(rlog "$1") 2>/dev/null | tail -1"; }

start_node() { rsh "$1" "~/speedboat-test/vm_node_ctl.sh start ${IP[$1]} $CONFIG run $LOCK $(rlog "$1")"; }
kill9_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh kill9' >/dev/null; }
clean_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh clean' >/dev/null; }
stop_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh stop' >/dev/null 2>&1; }
stop_all() { local h; for h in "${HOSTS[@]}"; do stop_host "$h"; done; }

# 读取某 host 最近 STATE 的字段
h_field() { fld "$(state_line "$1")" "$2"; }

# ============================ 部署 ============================
deploy() {
  log "P0a 宿主机预编译测试节点（字节码架构无关，17x 无 javac）"
  [ -f "$JAR" ] || { log "缺少 jar：$JAR（先 mvn package）"; exit 1; }
  local javac="$JDK_HOME/bin/javac" jarbin="$JDK_HOME/bin/jar"
  [ -x "$javac" ] || { log "缺少 javac：$javac（可用 SB_JDK_HOME 指定 JDK8）"; exit 1; }
  rm -rf "$RUNNER_DIR"
  mkdir -p "$RUNNER_DIR"
  "$javac" -source 8 -target 8 -cp "$JAR" -d "$RUNNER_DIR" "$RUNNER_SRC" || { log "编译测试节点失败"; exit 1; }
  (cd "$RUNNER_DIR" && "$jarbin" cf "$RUNNER_JAR" .) || { log "打包 vm-runner.jar 失败"; exit 1; }
  log "  -> $RUNNER_JAR"

  log "P0b 下发六台并校验"
  local h
  for h in "${HOSTS[@]}"; do
    rsh "$h" 'mkdir -p ~/speedboat-test/logs ~/speedboat-test/run' || { log "$h mkdir 失败"; exit 1; }
    scph "$h" "$JAR"        "~/speedboat-test/speedboat.jar"
    scph "$h" "$RUNNER_JAR" "~/speedboat-test/vm-runner.jar"
    scph "$h" "$REPO/$CONFIG" "~/speedboat-test/$CONFIG"
    scph "$h" "$LOGBACK"    "~/speedboat-test/logback-vm.xml"
    scph "$h" "$CTL"        "~/speedboat-test/vm_node_ctl.sh"
    rsh "$h" 'chmod +x ~/speedboat-test/vm_node_ctl.sh' || { log "$h chmod 失败"; exit 1; }
    echo "  $h 部署 OK"
  done
}

# ============================ P0 基线 ============================
phase0_baseline() {
  echo; log "P0 基线：启动 6 节点，两机房各自选主"
  local h
  for h in "${HOSTS[@]}"; do stop_host "$h"; clean_host "$h"; done
  for h in "${HOSTS[@]}"; do start_node "$h" >/dev/null; done
  sleep 3

  local ok0=1 ok1=1
  dc_wait_leader 0 30 || ok0=0
  dc_wait_leader 1 30 || ok1=0
  check "主机房（174/175/176）在 30s 内选出唯一 Leader" [ "$ok0" = 1 ]
  check "备机房（51/53/55）在 30s 内选出唯一 Leader" [ "$ok1" = 1 ]

  local nodes_ok=1 dc_ok=1
  for h in "${HOSTS[@]}"; do
    echo "  $h: node=$(h_field "$h" node) dc=$(fld "$(start_line "$h")" dc) term=$(h_field "$h" term) leader=$(h_field "$h" leader) isMain=$(h_field "$h" isMain)"
    node_id_ok "$h" || nodes_ok=0
    [ "$(fld "$(start_line "$h")" dc)" = "dc-${DC[$h]}" ] || dc_ok=0
  done
  check "六节点 nodeId 均按 ip:port 确定性推导" [ "$nodes_ok" = 1 ]
  check "六节点机房归属 dc 与配置一致（dc-0/dc-1）" [ "$dc_ok" = 1 ]
  check "主机房恰好一个 isMain" [ "$(dc_count_main 0)" = 1 ]
  check "备机房恰好一个 isMain" [ "$(dc_count_main 1)" = 1 ]
  check "基线全网 isMain=2（两机房各选各主）" [ "$(total_main)" = 2 ]
  BASELINE_MAIN=$(total_main)
}

# ============================ P1 场景1：杀主机房 Leader ============================
phase1_kill_main_leader() {
  echo; log "P1 场景1：杀主机房 Leader，看选举是否优先在主机房"
  local lh; lh=$(dc_leader_host 0)
  local bh; bh=$(dc_leader_host 1)
  if [ -z "$lh" ] || [ -z "$bh" ]; then check "场景1前两机房均有 Leader" false; return; fi

  local old_node old_term bak_node bak_term
  old_node=$(h_field "$lh" node); old_term=$(h_field "$lh" term)
  bak_node=$(h_field "$bh" node); bak_term=$(h_field "$bh" term)
  echo "  主机房 leader=$old_node term=$old_term host=$lh"
  echo "  备机房 leader=$bak_node term=$bak_term host=$bh"

  log "  kill -9 $lh（模拟主机房主节点崩溃）"
  kill9_host "$lh"

  if dc_wait_leader 0 30; then check "主机房崩溃后重新选出唯一 Leader" true
  else check "主机房崩溃后重新选出唯一 Leader" false; return; fi

  local nh; nh=$(dc_leader_host 0)
  local new_node new_term
  new_node=$(h_field "$nh" node); new_term=$(h_field "$nh" term)
  echo "  新 leader=$new_node host=$nh term=$new_term"
  MAIN_LAST_TERM=$new_term

  check "新 Leader 落在主机房内（host 属于机房0）" [ "${DC[$nh]}" = 0 ]
  check "新 Leader 与被杀节点不同（已换主）" [ "$new_node" != "$old_node" ]
  check "主机房任期严格递增（term 增长）" [ "$new_term" -gt "$old_term" ]

  # ---- 拓扑探测：两机房是否联动 ----
  local b2; b2=$(dc_leader_host 1)
  local bak2_node bak2_term
  bak2_node=$(h_field "$b2" node); bak2_term=$(h_field "$b2" term)
  echo "  备机房（未受影响）leader=$bak2_node term=$bak2_term"
  if [ "$bak2_node" = "$bak_node" ] && [ "$bak2_term" = "$bak_term" ]; then NO_COUPLING_P1=1; fi
  check "拓扑探测：备机房 Leader 未被主机房故障牵动（仍为原节点）" [ "$bak2_node" = "$bak_node" ]
  check "拓扑探测：备机房 term 未因主机房换主而变化（两组无跨机房联动）" [ "$bak2_term" = "$bak_term" ]
  check "全网 isMain=2（每房各一，双组并存）" [ "$(total_main)" = 2 ]
}

# ============================ P2 场景2：杀光主机房 ============================
phase2_kill_all_main() {
  echo; log "P2 场景2：杀光主机房，看备机房选举"
  local bh; bh=$(dc_leader_host 1)
  if [ -z "$bh" ]; then check "场景2前备机房存在 Leader" false; return; fi
  local bak_node bak_term
  bak_node=$(h_field "$bh" node); bak_term=$(h_field "$bh" term)
  echo "  备机房基线 leader=$bak_node term=$bak_term"

  log "  kill -9 主机房全部三台"
  local h
  for h in "${DC0[@]}"; do kill9_host "$h"; done
  sleep 4

  local all_dead=1
  for h in "${DC0[@]}"; do alive "$h" && all_dead=0; done
  check "主机房三节点均已停止" [ "$all_dead" = 1 ]

  if dc_wait_leader 1 20; then check "备机房仍维持唯一 Leader" true
  else check "备机房仍维持唯一 Leader" false; return; fi

  local n2; n2=$(dc_leader_host 1)
  local bak2_node bak2_term
  bak2_node=$(h_field "$n2" node); bak2_term=$(h_field "$n2" term)
  echo "  备机房（主机房全灭后）leader=$bak2_node term=$bak2_term"
  BAK_NODE_P2=$bak2_node
  if [ "$bak2_node" = "$bak_node" ] && [ "$bak2_term" = "$bak_term" ]; then NO_COUPLING_P2=1; fi
  check "备机房 Leader 与故障前一致（主机房全灭未触发其重选）" [ "$bak2_node" = "$bak_node" ]
  check "备机房 term 未因主机房全灭而变化（无跨机房联动）" [ "$bak2_term" = "$bak_term" ]
  check "全网 isMain=1（仅备机房，主机房已无主）" [ "$(total_main)" = 1 ]
}

# ============================ P3 场景3：重启主机房 ============================
phase3_restart_main() {
  echo; log "P3 场景3：重启全部主机房，看是否正常加入"
  local h
  for h in "${DC0[@]}"; do start_node "$h" >/dev/null; done
  sleep 5

  local all_alive=1
  for h in "${DC0[@]}"; do alive "$h" || all_alive=0; done
  check "主机房三节点均已存活（重启成功）" [ "$all_alive" = 1 ]

  if dc_wait_leader 0 30; then check "主机房重新加入并选出唯一 Leader" true
  else check "主机房重新加入并选出唯一 Leader" false; return; fi

  local nh; nh=$(dc_leader_host 0)
  local new_term; new_term=$(h_field "$nh" term)
  echo "  主机房重新加入后 leader=$(h_field "$nh" node) host=$nh term=$new_term（灭房前 term=$MAIN_LAST_TERM）"
  check "主机房 Leader 落在主机房内" [ "${DC[$nh]}" = 0 ]
  check "主机房 term 不低于灭房前（mmap 持久化挂回，非从 1 重来）" [ "$new_term" -ge "$MAIN_LAST_TERM" ]
  check "全网 isMain=2（恢复双 Leader 形态）" [ "$(total_main)" = 2 ]

  # ---- 备机房应全程未被打断 ----
  # 注意：dc_leader_host 返回 host 短名，须再取 nodeId 才能与 BAK_NODE_P2 比较
  local bh; bh=$(dc_leader_host 1)
  if [ -z "$bh" ]; then
    check "备机房 Leader 在主机房重入期间未被打断（仍为原节点）" false
  else
    check "备机房 Leader 在主机房重入期间未被打断（仍为原节点）" [ "$(h_field "$bh" node)" = "$BAK_NODE_P2" ]
  fi
}

# ============================ P4 场景4：杀光备机房 ============================
phase4_kill_all_backup() {
  echo; log "P4 场景4：杀光备机房，看主机房"
  local mh; mh=$(dc_leader_host 0)
  if [ -z "$mh" ]; then check "场景4前主机房存在 Leader" false; return; fi
  local m_node m_term
  m_node=$(h_field "$mh" node); m_term=$(h_field "$mh" term)
  echo "  主机房基线 leader=$m_node term=$m_term"

  log "  kill -9 备机房全部三台"
  local h
  for h in "${DC1[@]}"; do kill9_host "$h"; done
  sleep 4

  local all_dead=1
  for h in "${DC1[@]}"; do alive "$h" && all_dead=0; done
  check "备机房三节点均已停止" [ "$all_dead" = 1 ]

  if dc_wait_leader 0 20; then check "主机房仍维持唯一 Leader" true
  else check "主机房仍维持唯一 Leader" false; return; fi

  local n2; n2=$(dc_leader_host 0)
  local m2_node m2_term
  m2_node=$(h_field "$n2" node); m2_term=$(h_field "$n2" term)
  echo "  主机房（备机房全灭后）leader=$m2_node term=$m2_term"
  check "主机房 Leader 与故障前一致（备机房全灭未触发其重选）" [ "$m2_node" = "$m_node" ]
  check "主机房 term 未因备机房全灭而变化（无跨机房联动）" [ "$m2_term" = "$m_term" ]
  check "全网 isMain=1（仅主机房，备机房已无主）" [ "$(total_main)" = 1 ]
}

# ============================ P5 拓扑探测结论 ============================
phase5_topology_verdict() {
  echo; log "P5 拓扑探测结论（两机房是否真有跨机房联动）"
  echo "  证据1：P0 基线全网 isMain=2 —— 两机房各自独立选出 Leader"
  echo "  证据2：P1 杀主机房 Leader 后，备机房 term 不变"
  echo "  证据3：P2 主机房全灭后，备机房 leader/term 均不变"
  echo "  代码依据：Speedboat.doStartCrossDatacenterMode 只以 allNodes.get(datacenterIndex)"
  echo "            建组，peer 列表仅含本机房节点；election.cross.timeout 被读取但未接入；"
  echo "            单机房模式传入的 voteWeightStrategy 在跨机房模式未传；级联父组未实现。"
  check "拓扑探测：两机房各自独立成组（基线即 2 个 Leader）" [ "$BASELINE_MAIN" = 2 ]
  check "拓扑探测：主机房故障未向备机房传播 term（两组无联动）" [ "$NO_COUPLING_P1" = 1 ]
  check "拓扑探测：主机房全灭后备机房未被牵动（两组无联动）" [ "$NO_COUPLING_P2" = 1 ]
}

# ============================ 主流程 ============================
run_all() {
  phase0_baseline
  phase1_kill_main_leader
  phase2_kill_all_main
  phase3_restart_main
  phase4_kill_all_backup
  phase5_topology_verdict

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
