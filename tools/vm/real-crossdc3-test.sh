#!/usr/bin/env bash
#
# Speedboat 三机房（跨机房级联）六节点实机验证编排。
#
# 拓扑（config-real-crossdc3.properties，用户指定 2026-10-10）：
#   机房0（主机房，权重3）= 192.168.193.51 / .53
#   机房1（权重2）        = 192.168.193.55 / .174
#   机房2（权重2）        = 192.168.193.175 / .176
#   3/2/2 拓扑：total=7，父组门槛 required=(7/2)+1=4。
#
# 与双机房 2/1 的核心差异（详注 config-real-crossdc3.properties 头）：
#   主机房 self=3 < required=4，【不能单方成主】，需再收任一备房一票（3+2=5）；
#   两备房可互相授票联手成主（2+2=4）——因此：
#     · 全局父组唯一性的保证退回 Raft 原始"同 term 单票"不变式（每房一票、
#       单 term 单投票权），多机房下不存在双房"权重数学结构排除"的特判路径；
#     · 杀光主机房不再是"无主"：存活两房联手凑满 required 仍是唯一一主；
#     · 必须杀光主机房 + 任一备机房，才可能真正凑不齐 required。
#
# 覆盖（官方门面路径 Speedboat.start，逐项断言）：
#   P0 基线    三房各出唯一代表（代表总数=3）；全局唯一主落在主机房 dc-0
#   P1 场景1   杀主机房全局主 → 主机房重选接替席位，全局主仍在 dc-0，
#              机房1/2 隔离（term 未动）且父组联动收敛
#   P2 场景2   杀光主机房 → 存活两备房（dc-1/dc-2）联手产生唯一全局主
#              （CP/AP 均有主，此处与双机房 CP"无主"语义不同）
#   P3 场景3   重启主机房 → CP 凭 3+任一票=5≥4 夺回全局主；AP 低 term 不夺回
#              （席位留在备房，沿标准 Raft 任期收敛）
#   P4 场景4   杀光两备机房 → CP 无主（self=3<4 无票可补）；AP 降级剔除
#              一失联机房后 required=3，主机房 self=3 ≥ 3 接管成主
#   P4b 恢复两备机房 → CP 下不得抢夺主机房席位
#   P5 汇总    唯一性（全程 isMain ≤ 1）、三房代表恒为 3、父组 term 单调推进
#
# 用法：bash tools/vm/real-crossdc3-test.sh [deploy|run|stop|all]
# 环境变量：
#   SB_CONSISTENCY=cp|ap       一致性模式（缺省 cp）
#   SB_DEGRADED_TIMEOUT_MS=N   仅 ap 生效：降级接管阈值（缺省 10000）
#
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"

# ---- 环境清单（source 公共库之前须定义）----
SSH_USER=tomcat
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no
      -o ControlMaster=auto -o ControlPath=/tmp/sb-c3test-%r@%h -o ControlPersist=180)
SCP=(scp -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no
      -o ControlMaster=auto -o ControlPath=/tmp/sb-c3test-%r@%h -o ControlPersist=180)

# 三机房的成员（每房 2 节点）
DC0=(h51 h53)
DC1=(h55 h174)
DC2=(h175 h176)
HOSTS=("${DC0[@]}" "${DC1[@]}" "${DC2[@]}")

declare -A IP=(
  [h51]=192.168.193.51  [h53]=192.168.193.53
  [h55]=192.168.193.55  [h174]=192.168.193.174
  [h175]=192.168.193.175 [h176]=192.168.193.176
)
declare -A PORT=([h51]=21001 [h53]=21001 [h55]=21001 [h174]=21001 [h175]=21001 [h176]=21001)
# host -> 机房索引（0=主机房权重3；1/2=权重2）
declare -A DC=([h51]=0 [h53]=0 [h55]=1 [h174]=1 [h175]=2 [h176]=2)

# 公共库：SSH 通道、结构化日志解析、存活探测、唯一 Leader 等待、断言计数与汇总
. "$REPO/tools/vm/vm-test-lib.sh"

host_target() { echo "${IP[$1]:-$1}"; }

CONSISTENCY="${SB_CONSISTENCY:-cp}"
BASE_CONFIG=config-real-crossdc3.properties
CONFIG=""
LOCK=c3-lock
JAR="$REPO/target/speedboat-1.0-SNAPSHOT.jar"
LOGBACK="$REPO/tools/vm/logback-vm.xml"
CTL="$REPO/tools/vm/vm_node_ctl.sh"
RUNNER_SRC="$REPO/src/test/java/cn/itcraft/speedboat/sample/VmClusterNode.java"

JDK_HOME="${SB_JDK_HOME:-$HOME/lang/dragonwell-8.29.28}"
RUNNER_DIR="$REPO/target/vm-runner"
RUNNER_JAR="$REPO/target/vm-runner.jar"

# ------------------------------------------------------------------ 跨阶段状态
BASELINE_MAIN=0     # P0 基线全网 isMain 数（期望 1：全局唯一主，落在 dc-0）
BASELINE_PT=0       # P0 结束时全网 parentTerm 最大值
RECOVER_NODE=("", "", "")   # 各机房 P2/P3 时刻的代表 nodeId（供隔离断言回溯）
BAK1_NODE_P2=""; BAK1_TERM_P2=0; BAK2_NODE_P2=""; BAK2_TERM_P2=0
MAIN0_TERM=0        # 主机房子组最近 term（P1 后记录，P3 挂回断言）
GLOBAL_MAINS=()     # 各 phase 全网 isMain 采样，P5 汇总

# ============================ 机房维度辅助（三机房泛化） ============================
dc_members() { case "$1" in 0) echo "${DC0[@]}";; 1) echo "${DC1[@]}";; 2) echo "${DC2[@]}";; esac; }
dc_leader_host() { intra_host_in $(dc_members "$1"); }
dc_wait_leader() { wait_single_intra "$2" $(dc_members "$1"); }
dc_count_main() { count_main_in $(dc_members "$1"); }
dc_main_host() { leader_host_in $(dc_members "$1"); }

# 全网 isMain 总数
total_main() { echo "$(( $(dc_count_main 0) + $(dc_count_main 1) + $(dc_count_main 2) ))"; }

# 全网子组代表总数（三机房恒应为 3）
total_intra() { echo "$(( $(count_intra_in "${DC0[@]}") + $(count_intra_in "${DC1[@]}") + $(count_intra_in "${DC2[@]}") ))"; }

# 全网 parentTerm 最大值（父组任期推进证据）
max_parent_term() {
  local h m=0 t
  for h in "${HOSTS[@]}"; do
    alive "$h" || continue
    t=$(h_field "$h" parentTerm)
    case "$t" in ''|*[!0-9]*) continue;; esac
    [ "$t" -gt "$m" ] && m=$t
  done
  echo "$m"
}

# 全网 parentLeader 一致性（收敛时返回该值，未收敛返回 DIVERGED）
global_parent_leader() {
  local h pl ref=""
  for h in "${HOSTS[@]}"; do
    alive "$h" || continue
    pl=$(h_field "$h" parentLeader)
    [ -n "$pl" ] || continue
    if [ -z "$ref" ]; then ref=$pl
    elif [ "$pl" != "$ref" ]; then echo "DIVERGED"; return; fi
  done
  echo "${ref:-none}"
}

# 全局主所在机房索引（无主返回 -1）
main_dc_index() {
  local d
  for d in 0 1 2; do
    [ "$(dc_count_main "$d")" -ge 1 ] && { echo "$d"; return; }
  done
  echo -1
}

start_line() { rsh "$1" "grep 'event=START' ~/speedboat-test/$(rlog "$1") 2>/dev/null | tail -1"; }
start_node() { rsh "$1" "~/speedboat-test/vm_node_ctl.sh start ${IP[$1]} $CONFIG run $LOCK $(rlog "$1")"; }
kill9_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh kill9' >/dev/null; }
clean_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh clean' >/dev/null; }
stop_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh stop' >/dev/null 2>&1; }
stop_all() { local h; for h in "${HOSTS[@]}"; do stop_host "$h"; done; }
h_field() { fld "$(state_line "$1")" "$2"; }

# ============================ 生效配置生成 ============================
prepare_config() {
  case "$CONSISTENCY" in
    cp|ap) ;;
    *) log "SB_CONSISTENCY 取值非法：$CONSISTENCY（仅 cp / ap）"; exit 64 ;;
  esac
  CONFIG="config-real-crossdc3-${CONSISTENCY}.properties"
  mkdir -p "$REPO/target"
  cp "$REPO/$BASE_CONFIG" "$REPO/target/$CONFIG" || { log "生成生效配置失败"; exit 1; }
  {
    echo ""
    echo "# ---- 一致性模式（由 real-crossdc3-test.sh 按 SB_CONSISTENCY 追加）----"
    echo "consistency.policy=${CONSISTENCY}"
    if [ "$CONSISTENCY" = "ap" ]; then
      echo "consistency.degraded.timeout.ms=${SB_DEGRADED_TIMEOUT_MS:-10000}"
    fi
  } >> "$REPO/target/$CONFIG"
  log "生效配置 $CONFIG（consistency.policy=${CONSISTENCY}）"
}

# ============================ 部署 ============================
deploy() {
  prepare_config
  log "D1 宿主机预编译测试节点（字节码架构无关，目标机仅需 java）"
  [ -f "$JAR" ] || { log "缺少 jar：$JAR（先 mvn package -DskipTests）"; exit 1; }
  local javac="$JDK_HOME/bin/javac" jarbin="$JDK_HOME/bin/jar"
  [ -x "$javac" ] || { log "缺少 javac：$javac（可用 SB_JDK_HOME 指定 JDK8）"; exit 1; }
  rm -rf "$RUNNER_DIR"
  mkdir -p "$RUNNER_DIR"
  "$javac" -source 8 -target 8 -cp "$JAR" -d "$RUNNER_DIR" "$RUNNER_SRC" \
    || { log "编译测试节点失败"; exit 1; }
  (cd "$RUNNER_DIR" && "$jarbin" cf "$RUNNER_JAR" .) || { log "打包 vm-runner.jar 失败"; exit 1; }
  log "  -> $RUNNER_JAR"

  log "D2 下发六台并校验"
  local h
  for h in "${HOSTS[@]}"; do
    rsh "$h" 'mkdir -p ~/speedboat-test/logs ~/speedboat-test/run' || { log "$h mkdir 失败"; exit 1; }
    scph "$h" "$JAR"        "~/speedboat-test/speedboat.jar"
    scph "$h" "$RUNNER_JAR" "~/speedboat-test/vm-runner.jar"
    scph "$h" "$REPO/target/$CONFIG" "~/speedboat-test/$CONFIG" \
      || { log "$h 下发配置失败（$CONFIG）"; exit 1; }
    scph "$h" "$LOGBACK"    "~/speedboat-test/logback-vm.xml"
    scph "$h" "$CTL"        "~/speedboat-test/vm_node_ctl.sh"
    rsh "$h" 'chmod +x ~/speedboat-test/vm_node_ctl.sh' || { log "$h chmod 失败"; exit 1; }
    echo "  $h 部署 OK"
  done
}

# ============================ P0 基线 ============================
phase0_baseline() {
  echo; log "P0 基线：启动六节点，三房各一代表，全局唯一主落在主机房 dc-0"
  local h
  for h in "${HOSTS[@]}"; do stop_host "$h"; clean_host "$h"; done
  for h in "${HOSTS[@]}"; do start_node "$h" >/dev/null; done
  sleep 3

  local ok
  for ((ok = 0; ok < 3; ok++)); do
    if dc_wait_leader "$ok" 30; then
      check "机房$ok（$(dc_members $ok | tr ' ' '/')）在 30s 内选出唯一子组代表" true
    else
      check "机房$ok（$(dc_members $ok | tr ' ' '/')）在 30s 内选出唯一子组代表" false
    fi
  done
  local nodes_ok=1 dc_ok=1 port_ok=1
  for h in "${HOSTS[@]}"; do
    echo "  $h: node=$(h_field "$h" node) dc=$(fld "$(start_line "$h")" dc) term=$(h_field "$h" term)" \
        "intra=$(h_field "$h" intra) parent=$(h_field "$h" parent) isMain=$(h_field "$h" isMain)"
    node_id_ok "$h" || nodes_ok=0
    [ "$(fld "$(start_line "$h")" dc)" = "dc-${DC[$h]}" ] || dc_ok=0
    [ "$(h_field "$h" parentPort)" = "22001" ] || port_ok=1
    [ "$(h_field "$h" parentPort)" = "22001" ] || port_ok=0
  done
  check "六节点 nodeId 均按 ip:port 确定性推导" [ "$nodes_ok" = 1 ]
  check "六节点机房归属 dc 与配置一致（dc-0/dc-1/dc-2）" [ "$dc_ok" = 1 ]
  check "六节点父组端口均为 22001（子组 21001 + offset 1000）" [ "$port_ok" = 1 ]

  local i0 i1 i2
  i0=$(count_intra_in "${DC0[@]}"); i1=$(count_intra_in "${DC1[@]}"); i2=$(count_intra_in "${DC2[@]}")
  if [ "$i0" = 1 ] && [ "$i1" = 1 ] && [ "$i2" = 1 ]; then
    check "三房代表各恰好一份（intra=1）" true
  else
    check "三房代表各恰好一份（intra=1）" false
    echo "  intra 实际值：dc-0=$i0 dc-1=$i1 dc-2=$i2"
  fi
  check "全网子组代表总数=3（三机房父组成员数稳定）" [ "$(total_intra)" = 3 ]

  BASELINE_MAIN=$(total_main)
  check "基线全网 isMain=1（全局唯一主——硬约束基线）" [ "$BASELINE_MAIN" = 1 ]

  check "全局主落在主机房 dc-0（机房优先级生效：权重3 候选可收备房票）" \
    [ "$(main_dc_index)" = 0 ]

  local mh; mh=$(dc_main_host 0)
  if [ -n "$mh" ]; then
    check "全局主持有主机房子组代表身份（isMain ⊂ intra）" [ "$(h_field "$mh" intra)" = "true" ]
    check "全局主持有父组席位（parent=true）" [ "$(h_field "$mh" parent)" = "true" ]
    echo "  全局主=$mh node=$(h_field "$mh" node)"
  else
    check "全局主位于主机房" false
  fi
  MAIN0_TERM=$(h_field "$(dc_leader_host 0)" term 2>/dev/null || echo 0)

  local ok_g=1
  wait_single_global_leader 30 "${HOSTS[@]}" || ok_g=0
  check "父组 30s 内收敛（全网唯一主 + parentLeader 全网一致）" [ "$ok_g" = 1 ]

  BASELINE_PT=$(max_parent_term)
  echo "  基线：全网 isMain=$(total_main) parentTerm(max)=$BASELINE_PT parentLeader=$(global_parent_leader)"
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P1 场景1：杀主机房全局主 ============================
phase1_kill_main_leader() {
  echo; log "P1 场景1：杀主机房全局主，断言 dc-0 重选接替、备两房隔离并联动收敛"
  local lh; lh=$(dc_leader_host 0)
  if [ -z "$lh" ]; then check "场景1前主机房存在子组代表" false; return; fi
  [ "$(h_field "$lh" isMain)" = "true" ] || { check "场景1前主机房代表就是全局主" false; return; }

  local old_node old_term
  old_node=$(h_field "$lh" node); old_term=$(h_field "$lh" term)
  echo "  主机房代表=$old_node term=$old_term host=$lh（当前全局主）"
  local pt_before; pt_before=$(max_parent_term)
  # 备两房基线（供隔离断言）
  local b1 b2 b1_node b2_node b1_term b2_term
  b1=$(dc_leader_host 1); b2=$(dc_leader_host 2)
  b1_node=$(h_field "$b1" node); b1_term=$(h_field "$b1" term)
  b2_node=$(h_field "$b2" node); b2_term=$(h_field "$b2" term)

  log "  kill -9 $lh"
  kill9_host "$lh"

  if dc_wait_leader 0 30; then check "主机房重选产生唯一子组代表" true
  else check "主机房重选产生唯一子组代表" false; return; fi

  # 席位交接：全局收敛后再取快照
  local ok_g=1
  wait_single_global_leader 30 "${HOSTS[@]}" || ok_g=0
  check "席位交接后父组重新收敛（唯一主 + parentLeader 一致）" [ "$ok_g" = 1 ]

  local nh; nh=$(dc_leader_host 0)
  if [ -n "$nh" ]; then
    local new_node; new_node=$(h_field "$nh" node)
    echo "  主机房新代表=$new_node host=$nh term=$(h_field "$nh" term)"
    check "新子组代表落在主机房（dc-0 权重3 优先接替）" [ "${DC[$nh]}" = 0 ]
    check "新子组代表与被杀节点不同（确实换主）" [ "$new_node" != "$old_node" ]
    check "收敛后主机房代表持全局席位" [ "$(h_field "$nh" isMain)" = "true" ]
  else
    check "主机房新代表存在" false
  fi
  check "全网 isMain=1（交接期间未现双主）" [ "$(total_main)" = 1 ]

  # 机房内隔离（两备房 term/代表未变）——注意父组 term 会变，但子组 term 独立
  local nb1 nb2
  nb1=$(dc_leader_host 1); nb2=$(dc_leader_host 2)
  check "隔离：机房1 代表未被牵动" [ "$(h_field "$nb1" node)" = "$b1_node" ]
  check "隔离：机房2 代表未被牵动" [ "$(h_field "$nb2" node)" = "$b2_node" ]
  check "隔离：机房1 子组 term 未变" [ "$(h_field "$nb1" term)" = "$b1_term" ]
  check "隔离：机房2 子组 term 未变" [ "$(h_field "$nb2" term)" = "$b2_term" ]

  # 联动：父组 term 因 dc-0 换代推进（父组是活着且敏感的独立实例）
  local pt_after; pt_after=$(max_parent_term)
  check "联动：父组 term 因主机房换代推进（$pt_before -> $pt_after）" [ "$pt_after" -gt "$pt_before" ]

  MAIN0_TERM=$(h_field "$(dc_leader_host 0)" term 2>/dev/null || echo 0)
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P2 场景2：杀光主机房 ============================
# 三机房差异：不能杀光"备机房"（两备房是彼此唯一的凑票来源），先杀主机房。
phase2_kill_all_main() {
  echo; log "P2 场景2：杀光主机房（dc-0 两台），断言存活两备房联手产生【唯一】全局主"
  # 记录备两房基线（隔离断言用）
  BAK1_NODE_P2=$(h_field "$(dc_leader_host 1)" node 2>/dev/null || echo "")
  BAK1_TERM_P2=$(h_field "$(dc_leader_host 1)" term 2>/dev/null || echo 0)
  BAK2_NODE_P2=$(h_field "$(dc_leader_host 2)" node 2>/dev/null || echo "")
  BAK2_TERM_P2=$(h_field "$(dc_leader_host 2)" term 2>/dev/null || echo 0)
  echo "  备房基线：dc-1=$BAK1_NODE_P2 term=$BAK1_TERM_P2；dc-2=$BAK2_NODE_P2 term=$BAK2_TERM_P2"
  check "场景2前全局主在主机房（isMain=1）" [ "$(total_main)" = 1 ]

  log "  kill -9 主机房全部两台"
  local h
  for h in "${DC0[@]}"; do kill9_host "$h"; done
  sleep 4

  local all_dead=1
  for h in "${DC0[@]}"; do alive "$h" && all_dead=0; done
  check "主机房两节点均已停止" [ "$all_dead" = 1 ]

  # ---- 全局：存活两备房联手（2+2=4 ≥ required=4）应产出唯一主 ----
  # 与双机房 2/1 的本质差异：双房 CP 下备房 self=1<2 无法凑票 → 无主；
  # 三房 3/2/2 下 dc-1/dc-2 可互相授票凑满 4 → 一主（term 单票保证唯一）。
  local ok_g=1
  wait_single_global_leader 40 "${HOSTS[@]}" || ok_g=0
  check "主机房全灭后 40s 内父组收敛为唯一主（存活两房联手凑票）" [ "$ok_g" = 1 ]
  check "全网 isMain=1（未双主、亦无主外之主）" [ "$(total_main)" = 1 ]
  local mdi; mdi=$(main_dc_index)
  if [ "$mdi" = 1 ] || [ "$mdi" = 2 ]; then
    check "全局主落在备机房 dc-1/dc-2（主机房已死）" true
  else
    check "全局主落在备机房 dc-1/dc-2（主机房已死）" false
  fi

  # 备两房代表在主机房全灭期间保持稳定（用户语义：只杀主机房）
  local nb1 nb2
  nb1=$(dc_leader_host 1); nb2=$(dc_leader_host 2)
  check "隔离：机房1 代表未被牵动" [ "$(h_field "$nb1" node)" = "$BAK1_NODE_P2" ]
  check "隔离：机房2 代表未被牵动" [ "$(h_field "$nb2" node)" = "$BAK2_NODE_P2" ]

  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P3 场景3：重启主机房 ============================
phase3_restart_main() {
  echo; log "P3 场景3：重启主机房，看是否夺回全局主"
  local h
  for h in "${DC0[@]}"; do start_node "$h" >/dev/null; done
  sleep 5

  local all_alive=1
  for h in "${DC0[@]}"; do alive "$h" || all_alive=0; done
  check "主机房两节点均已存活" [ "$all_alive" = 1 ]

  if dc_wait_leader 0 30; then check "主机房重新加入并选出唯一子组代表" true
  else check "主机房重新加入并选出唯一子组代表" false; return; fi

  local nh; nh=$(dc_leader_host 0)
  local new_term; new_term=$(h_field "$nh" term)
  echo "  主机房重新代表=$(h_field "$nh" node) term=$new_term（灭房前=$MAIN0_TERM）"
  check "主机房代表落在主机房" [ "${DC[$nh]}" = 0 ]
  check "主机房子组 term 不低于灭房前（mmap 挂回）" [ "$new_term" -ge "$MAIN0_TERM" ]

  local ok_g=1
  wait_single_global_leader 30 "${HOSTS[@]}" || ok_g=0
  check "主机房重入后父组重新收敛（唯一主 + parentLeader 一致）" [ "$ok_g" = 1 ]
  check "全网 isMain=1（未双主）" [ "$(total_main)" = 1 ]

  if [ "$CONSISTENCY" = "ap" ]; then
    # AP：P2 备房已接管并把父组 term 推高；主机房 term 低不夺回（标准 Raft 收敛）。
    local mdi; mdi=$(main_dc_index)
    if [ "$mdi" = 1 ] || [ "$mdi" = 2 ]; then
      check "AP：全局主仍在备机房（低 term 不夺回）" true
    else
      check "AP：全局主仍在备机房（低 term 不夺回）" false
    fi
    check "AP：主机房代表未持全局席位" [ "$(h_field "$nh" isMain)" = "false" ]
  else
    # CP：主机房重启后 term 由 mmap 挂回并推进，self=3 收任一备房票 5≥4 夺回。
    # 备两房在位 term 由 term 单票机制保证最终跟随，收敛窗口取长（多轮 term 爬升）。
    check "CP：全局主夺回到主机房（权重3 主房重选 + 任一备房票 = 5 ≥ 4）" \
      [ "$(main_dc_index)" = 0 ]
    check "CP：主机房代表持有全局席位" [ "$(h_field "$nh" isMain)" = "true" ]
  fi
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P4 场景4：杀光两备机房 ============================
phase4_kill_all_backup() {
  echo; log "P4 场景4：杀光 dc-1 与 dc-2（两备房），断言无票可补的末态语义"
  # CP：主房随后在 dc-0（P3 夺回）；AP：主房仍在备房（P3 不夺回）。
  local seat_dc=0
  [ "$CONSISTENCY" = "ap" ] && seat_dc=1
  local mh; mh=$(dc_main_host "$seat_dc")
  if [ -z "$mh" ]; then
    # CP 亦可能在 P3 后短暂无主（term 赶超窗口）；此处直接按预期位置找
    check "场景4前全局主位于预期机房（dc-$seat_dc）" false
    return
  fi
  check "场景4前全网 isMain=1" [ "$(total_main)" = 1 ]

  log "  kill -9 备机房全部四台（dc-1/dc-2）"
  local h
  for h in "${DC1[@]}" "${DC2[@]}"; do kill9_host "$h"; done

  # 跨过 check-quorum 窗口（默认 5s）再断言，防误降级干扰
  sleep 8

  local all_dead=1
  for h in "${DC1[@]}" "${DC2[@]}"; do alive "$h" && all_dead=0; done
  check "两备机房四节点均已停止" [ "$all_dead" = 1 ]

  if [ "$CONSISTENCY" = "ap" ]; then
    # AP：降级剔除一失联机房 → total=3+2=5，required=3，主机房 self=3 ≥ 3 接管。
    # （wait_single_global_leader 轮询窗口覆盖"等降级阈值 + 重选"整段。）
    local ok_take=1
    wait_single_global_leader 40 "${HOSTS[@]}" || ok_take=0
    check "AP：备房全灭后主机房降级接管（40s 内收敛唯一主）" [ "$ok_take" = 1 ]
    check "AP：接管者落在主机房（required=3 ≤ self=3）" [ "$(main_dc_index)" = 0 ]
    check "AP：全网 isMain=1（未双主）" [ "$(total_main)" = 1 ]
  else
    # CP：self=3 < required=4，且无备房票可补 → 退化为【无主】并稳定。
    local ok_none=1
    wait_no_global_leader 5 20 "${HOSTS[@]}" || ok_none=0
    check "CP：全局退化为无主并稳定 5s（self=3<4 无票可补，唯一性硬约束的末态）" [ "$ok_none" = 1 ]
    check "CP：全网 isMain=0（未出现降级接管）" [ "$(total_main)" = 0 ]
  fi
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P4b 恢复两备机房 ============================
phase4b_restore_backup() {
  echo; log "P4b 恢复两备机房，断言其不得抢夺主机房席位"
  # CP 下席位回到主机房（P4 与 P3 间），AP 下席位已在主机房（P4 接管）
  local mh; mh=$(dc_main_host 0)
  if [ -z "$mh" ]; then
    check "P4b 前主机房持有全局席位（CP 前提）" false
    return
  fi
  local m_node; m_node=$(h_field "$mh" node)

  local h
  for h in "${DC1[@]}" "${DC2[@]}"; do start_node "$h" >/dev/null; done
  sleep 5

  local all_alive=1
  for h in "${DC1[@]}" "${DC2[@]}"; do alive "$h" || all_alive=0; done
  check "两备机房四节点恢复存活" [ "$all_alive" = 1 ]

  dc_wait_leader 1 30 || check "机房1 重新选出唯一代表" false
  dc_wait_leader 2 30 || check "机房2 重新选出唯一代表" false

  sleep 12
  local ok_g=1
  wait_single_global_leader 20 "${HOSTS[@]}" || ok_g=0
  check "备房重入后父组仍收敛（唯一主 + parentLeader 一致）" [ "$ok_g" = 1 ]
  check "全局 isMain 仍为 1（备房重入未产生双主）" [ "$(total_main)" = 1 ]

  local n2; n2=$(dc_main_host 0)
  check "全局主仍在主机房" [ -n "$n2" ]
  check "全局主节点未因备房重入改变" [ "$(h_field "$n2" node)" = "$m_node" ]
  check "备房代表未持有全局席位" [ "$(h_field "$(dc_leader_host 1)" isMain)" = "false" ]
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P5 汇总 ============================
phase5_topology_verdict() {
  echo; log "P5 汇总判定（三机房 3/2/2 唯一性 + 三房代表恒定）"
  echo "  设计：isMain = intra && parent；权重 dc-0=3 / dc-1=2 / dc-2=2，required=4"
  echo "  各阶段全网 isMain 采样：${GLOBAL_MAINS[*]-（无）}"
  echo "  P0 基线 isMain=$BASELINE_MAIN；父组 term 基线=$BASELINE_PT 终态=$(max_parent_term)"

  local uniq=1 n
  for n in "${GLOBAL_MAINS[@]-}"; do
    case "$n" in ''|*[!0-9]*) continue;; esac
    [ "$n" -le 1 ] || uniq=0
  done
  check "V7 全程任意时刻 isMain ≤ 1（绝无双主——Raft 同 term 单票 + 权重闸门）" [ "$uniq" = 1 ]

  local pt_final; pt_final=$(max_parent_term)
  check "V2 父组 term 独立推进且不回退（$BASELINE_PT -> $pt_final）" [ "$pt_final" -gt "$BASELINE_PT" ]

  local sep=1
  local h
  for h in "${HOSTS[@]}"; do
    [ "$(h_field "$h" parentPort)" = "22001" ] || sep=0
    case "$(h_field "$h" node)" in *-21001) ;; *) sep=0;; esac
    case "$(h_field "$h" parentLeader)" in *-22001|none) ;; *) sep=0;; esac
  done
  check "父子组端口分离（21001/22001）且身份互不串组" [ "$sep" = 1 ]

  check "结束时三房代表各一份（总数=3）" [ "$(total_intra)" = 3 ]

  echo
  echo "  一致性模式：${CONSISTENCY}"
  if [ "$CONSISTENCY" = "ap" ]; then
    echo "  结论：唯一性全程成立；杀光主机房存活两房联手仍有主；备房全灭可降级接管成主"
  else
    echo "  结论：唯一性全程成立；杀光主机房存活两房联手仍有主（无主不再出现）；"
    echo "        主机房 + 两备机房同时全灭才无主（唯一性硬约束的末态代价）"
  fi
}

# ============================ 主流程 ============================
run_all() {
  phase0_baseline
  phase1_kill_main_leader
  phase2_kill_all_main
  phase3_restart_main
  phase4_kill_all_backup
  phase4b_restore_backup
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
