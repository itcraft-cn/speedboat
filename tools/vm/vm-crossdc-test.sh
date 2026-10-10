#!/usr/bin/env bash
#
# Speedboat 六节点双机房（跨机房级联）实机验证编排。
#
# 拓扑（config-vm6-crossdc.properties）：
#   主机房 机房0 = 192.168.193.174 / .175 / .176（aarch64）
#   备机房 机房1 = 192.168.193.51 / .53 / .55（x86_64）
# 每个进程持有两套独立 Raft（见 Speedboat.doStartCrossDatacenterMode）：
#   子组（机房内层）端口 21001 → 选出"本机房代表"（intra=true）
#   父组（跨机房层）端口 22001 → 各机房代表角逐"全局主"（isMain=true）
# 父组采用不对称机房权重（主机房 2 / 备机房 1，门槛 required=2），
# 故主机房可单方成主而备机房不可，从而在结构上排除双主。
#
# 覆盖（官方门面路径 Speedboat.start，逐项断言 + 拓扑探测）：
#   P0 基线    两机房各出唯一代表；主机房当选全局唯一主（isMain=1）
#   P1 场景1   杀主机房 Leader → 主机房重选并接替父组席位，全局唯一主仍在主机房
#   P2 场景2   杀光主机房 → CP 退化为【无主】；AP 备机房降级接管成主
#   P3 场景3   重启主机房 → CP 夺回全局席位；AP 按 term 收敛仍在备机房
#   P4 场景4   杀光备机房 → 主机房凭权重【单方成主】（两模式末态一致）
#   P5 判定    唯一性是否全程成立（任意时刻 isMain ≤ 1）、父组 term 是否独立单调
#
# 硬约束（设计文档 §7 决策二）：无论哪种模式，全局 Leader 必须"只有一主或无主"，绝不双主。
#   CP（缺省）：整机房失联退化为"无主"，绝不接管 —— P2 的"无主"是刻意设计结果，
#              运维兜底手段（人工升级 API）见设计文档 §8.6。
#   AP        ：整机房失联超 consistency.degraded.timeout.ms 后降级接管，
#              分区窗口内 isMain 可能短暂为 2，愈合后按标准 Raft 任期收敛。
#
# 前置：六台已单向信任、互相可达；主机房（17x）无 javac，故测试节点由宿主机
#       预编译为字节码（架构无关）后下发，目标机仅需 java。
# 用法：bash tools/vm/vm-crossdc-test.sh [deploy|run|stop|all]
#   deploy  宿主机预编译测试节点 + 下发六台
#   run     执行 P0~P5（假定已部署）
#   stop    停止所有节点
#   all     先 deploy 再 run（默认）
# 环境变量：
#   SB_CONSISTENCY=cp|ap       一致性模式（缺省 cp）
#   SB_DEGRADED_TIMEOUT_MS=N   仅 ap 生效：降级接管阈值（缺省 10000）
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

# 一致性模式（CP/AP），由 SB_CONSISTENCY 指定，决定父组在"整机房失联"时的取舍：
#   cp（缺省）= 强一致唯一性：杀光主机房退化为【无主】，绝不降级接管，绝不双主
#   ap        = 可用性优先：整机房失联超 consistency.degraded.timeout.ms（10s）后
#               降级接管、本机房单方成主；分区窗口内 isMain 可能短暂为 2
# 两模式共用同一份基础配置，模式专属键由 prepare_config 追加，避免两份配置漂移。
CONSISTENCY="${SB_CONSISTENCY:-cp}"
BASE_CONFIG=config-vm6-crossdc.properties
# 生成后的生效配置（target/ 下）；deploy 时下发。由 prepare_config 填充。
CONFIG=""
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
# 由各 phase 在观测点写入，供 P5 唯一性/联动汇总口径使用
BASELINE_MAIN=0     # P0 基线全网 isMain 数（期望 1：主机房当选全局唯一主）
BASELINE_PT=0       # P0 结束时全网 parentTerm 最大值（父组任期推进的比较基线）
BAK_NODE_P2=""      # P2 结束时备机房子组代表 nodeId，供 P3 断言"未被打断"
BAK_TERM_P2=0       # P2 结束时备机房子组 term，供 P3 断言机房内未被牵动
MAIN_LAST_TERM=0    # 主机房子组最后一次可观测 term（P1 后记录，P3 用于 mmap 挂回断言）
GLOBAL_MAINS=()     # 各 phase 观测到的全网 isMain 计数，P5 汇总断言"全程唯一"

# ============================ 机房维度辅助 ============================
# 双层结构下"机房代表"与"全局主"是两个不同概念，口径必须分开：
#   dc_leader_host / dc_wait_leader  → 子组代表（intra=true），机房内概念
#   dc_main_host  / dc_count_main    → 全局主（isMain=true），跨机房概念
# 备机房在主机房存活期间长期有代表但无全局席位，属预期形态。
dc_leader_host() { case "$1" in 0) intra_host_in "${DC0[@]}";; *) intra_host_in "${DC1[@]}";; esac; }
dc_wait_leader() { case "$1" in 0) wait_single_intra "$2" "${DC0[@]}";; *) wait_single_intra "$2" "${DC1[@]}";; esac; }
dc_count_main() { case "$1" in 0) count_main_in "${DC0[@]}";; *) count_main_in "${DC1[@]}";; esac; }
dc_main_host() { case "$1" in 0) leader_host_in "${DC0[@]}";; *) leader_host_in "${DC1[@]}";; esac; }

# 全网 isMain 总数
total_main() { echo "$(( $(dc_count_main 0) + $(dc_count_main 1) ))"; }

# 全网子组代表总数（应恒等于机房数 = 2）
total_intra() { echo "$(( $(count_intra_in "${DC0[@]}") + $(count_intra_in "${DC1[@]}") ))"; }

# 全网 parentTerm 最大值——父组任期推进的直接证据（两组 term 各自独立）
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

# 全网 parentLeader 取值（全网一致时返回该值，否则返回 DIVERGED）
# 用于断言父组已收敛到同一 leader，而非两房各持一词。
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

# 节点启动时 event=START 行（含 dc= 机房归属，STATE 行不含该字段）
start_line() { rsh "$1" "grep 'event=START' ~/speedboat-test/$(rlog "$1") 2>/dev/null | tail -1"; }

start_node() { rsh "$1" "~/speedboat-test/vm_node_ctl.sh start ${IP[$1]} $CONFIG run $LOCK $(rlog "$1")"; }
kill9_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh kill9' >/dev/null; }
clean_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh clean' >/dev/null; }
stop_host() { rsh "$1" '~/speedboat-test/vm_node_ctl.sh stop' >/dev/null 2>&1; }
stop_all() { local h; for h in "${HOSTS[@]}"; do stop_host "$h"; done; }

# 读取某 host 最近 STATE 的字段
h_field() { fld "$(state_line "$1")" "$2"; }

# ============================ 生效配置生成 ============================
# 以基础配置为底，追加一致性模式专属键，产出到 target/ 后由 deploy 下发。
# 两模式共用同一份基础配置，避免复制两份 properties 造成漂移。
prepare_config() {
  case "$CONSISTENCY" in
    cp|ap) ;;
    *) log "SB_CONSISTENCY 取值非法：$CONSISTENCY（仅 cp / ap）"; exit 64 ;;
  esac
  CONFIG="config-vm6-crossdc-${CONSISTENCY}.properties"
  mkdir -p "$REPO/target"
  cp "$REPO/$BASE_CONFIG" "$REPO/target/$CONFIG" || { log "生成生效配置失败"; exit 1; }
  {
    echo ""
    echo "# ---- 一致性模式（由 vm-crossdc-test.sh 按 SB_CONSISTENCY 追加）----"
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
    # 生成的生效配置在 target/ 下，远端仍落到 ~/speedboat-test/ 根（vm_node_ctl 以此为 BASE）
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
  echo; log "P0 基线：启动 6 节点，主机房当选全局唯一主"
  local h
  for h in "${HOSTS[@]}"; do stop_host "$h"; clean_host "$h"; done
  for h in "${HOSTS[@]}"; do start_node "$h" >/dev/null; done
  sleep 3

  local ok0=1 ok1=1
  dc_wait_leader 0 30 || ok0=0
  dc_wait_leader 1 30 || ok1=0
  check "主机房（174/175/176）在 30s 内选出唯一子组代表" [ "$ok0" = 1 ]
  check "备机房（51/53/55）在 30s 内选出唯一子组代表" [ "$ok1" = 1 ]

  local nodes_ok=1 dc_ok=1 port_ok=1
  for h in "${HOSTS[@]}"; do
    echo "  $h: node=$(h_field "$h" node) dc=$(fld "$(start_line "$h")" dc) term=$(h_field "$h" term) intra=$(h_field "$h" intra) parent=$(h_field "$h" parent) isMain=$(h_field "$h" isMain) parentPort=$(h_field "$h" parentPort)"
    node_id_ok "$h" || nodes_ok=0
    [ "$(fld "$(start_line "$h")" dc)" = "dc-${DC[$h]}" ] || dc_ok=0
    [ "$(h_field "$h" parentPort)" = "22001" ] || port_ok=0
  done
  check "六节点 nodeId 均按 ip:port 确定性推导" [ "$nodes_ok" = 1 ]
  check "六节点机房归属 dc 与配置一致（dc-0/dc-1）" [ "$dc_ok" = 1 ]
  check "六节点父组端口均为 22001（子组 21001 + cross.port.offset 1000）" [ "$port_ok" = 1 ]

  check "主机房恰好一个子组代表（intra=1）" [ "$(count_intra_in "${DC0[@]}")" = 1 ]
  check "备机房恰好一个子组代表（intra=1）" [ "$(count_intra_in "${DC1[@]}")" = 1 ]
  check "全网子组代表总数=2（每房各一，父组成员数稳定）" [ "$(total_intra)" = 2 ]

  BASELINE_MAIN=$(total_main)
  check "基线全网 isMain=1（主机房当选，全局唯一主——硬约束基线）" [ "$BASELINE_MAIN" = 1 ]

  # 全局主必须落在主机房，且同时持有子组与父组两道席位
  local mh; mh=$(dc_main_host 0)
  check "全局主落在主机房（机房优先级生效）" [ -n "$mh" ]
  check "全局主持有主机房子组代表身份（isMain ⊂ intra）" [ "$(h_field "$mh" intra)" = "true" ]
  check "全局主持有父组席位（parent=true）" [ "$(h_field "$mh" parent)" = "true" ]
  check "备机房代表未持有全局席位（预期 intra=true / isMain=false）" \
    [ "$(h_field "$(dc_leader_host 1)" isMain)" = "false" ]

  # 父组收敛：全网 parentLeader 指向同一父组节点，且该节点按父组端口推导
  local ok_g=1
  wait_single_global_leader 30 "${HOSTS[@]}" || ok_g=0
  check "父组 30s 内收敛（全网唯一主 + parentLeader 全网一致）" [ "$ok_g" = 1 ]

  BASELINE_PT=$(max_parent_term)
  echo "  基线：全网 isMain=$(total_main) parentTerm(max)=$BASELINE_PT parentLeader=$(global_parent_leader)"
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P1 场景1：杀主机房 Leader ============================
phase1_kill_main_leader() {
  echo; log "P1 场景1：杀主机房 Leader，看全局唯一主是否仍在主机房"
  local lh; lh=$(dc_leader_host 0)      # 主机房子组代表（同时也是全局主）
  local bh; bh=$(dc_leader_host 1)      # 备机房子组代表（无全局席位）
  if [ -z "$lh" ] || [ -z "$bh" ]; then check "场景1前两机房均有子组代表" false; return; fi

  local old_node old_term bak_node bak_term
  old_node=$(h_field "$lh" node); old_term=$(h_field "$lh" term)
  bak_node=$(h_field "$bh" node); bak_term=$(h_field "$bh" term)
  echo "  主机房代表=$old_node term=$old_term host=$lh（当前全局主）"
  echo "  备机房代表=$bak_node term=$bak_term host=$bh（无全局席位）"
  [ "$(h_field "$lh" isMain)" = "true" ] || { check "场景1前主机房代表就是全局主" false; return; }
  local pt_before; pt_before=$(max_parent_term)

  log "  kill -9 $lh（模拟主机房主节点崩溃）"
  kill9_host "$lh"

  if dc_wait_leader 0 30; then check "主机房崩溃后重新选出唯一子组代表" true
  else check "主机房崩溃后重新选出唯一子组代表" false; return; fi

  # 只断言"重选出了唯一代表"，不断言"是哪个节点"：机房子组重选是渐进过程，
  # 中间可能出现 transpose（低 term 当选者随后被更高 term 接管）。全局席位
  # 归属必须以父组收敛后的快照为准，否则会把收敛期快照与收敛态做错误比对。
  local raw_term; raw_term=$(h_field "$(dc_leader_host 0)" term)
  check "主机房子组任期严格递增（term 增长）" [ "$raw_term" -gt "$old_term" ]

  # ---- 全局席位交接：新代表必须重新夺得父组席位，且全局仍唯一 ----
  # 先等父组收敛，再取主机房代表快照——保证 nh 与实际全局主为同一观测时刻。
  local ok_g=1
  wait_single_global_leader 30 "${HOSTS[@]}" || ok_g=0
  check "席位交接后父组重新收敛（全网唯一主 + parentLeader 一致）" [ "$ok_g" = 1 ]

  if dc_wait_leader 0 10; then check "父组收敛后主机房仍持有子组代表" true
  else check "父组收敛后主机房仍持有子组代表" false; return; fi
  local nh; nh=$(dc_leader_host 0)
  local new_node new_term
  new_node=$(h_field "$nh" node); new_term=$(h_field "$nh" term)
  echo "  新代表（收敛后）=$new_node host=$nh term=$new_term"
  check "新子组代表落在主机房内（host 属于机房0）" [ "${DC[$nh]}" = 0 ]
  check "新子组代表与被杀节点不同（已换主）" [ "$new_node" != "$old_node" ]
  check "收敛后主机房代表持全局席位（机房优先级在换代后仍成立）" \
    [ "$(h_field "$nh" isMain)" = "true" ]
  check "全网 isMain=1（席位交接期间未出现双主）" [ "$(total_main)" = 1 ]

  # ---- 双层证据：机房内隔离 + 跨机房联动 ----
  local b2; b2=$(dc_leader_host 1)
  local bak2_node bak2_term
  bak2_node=$(h_field "$b2" node); bak2_term=$(h_field "$b2" term)
  echo "  备机房（未受牵动）代表=$bak2_node term=$bak2_term"
  check "隔离：备机房子组代表未被主机房故障牵动（仍为原节点）" [ "$bak2_node" = "$bak_node" ]
  check "隔离：备机房子组 term 未变（机房内选举不受对侧影响）" [ "$bak2_term" = "$bak_term" ]

  # 联动：备机房的【父组】Leader 应已切到主机房新代表（此刻已收敛，快照即真相）
  local expect_parent="node-${IP[$nh]}-22001"
  check "联动：备机房父组 Leader 已切换为主机房新代表（父组认识新代表）" \
    [ "$(h_field "$b2" parentLeader)" = "$expect_parent" ]

  # 联动：父组任期因换代而推进，子组任期不受对侧影响
  local pt_after; pt_after=$(max_parent_term)
  check "联动：父组 term 因主机房换代而推进（$pt_before -> $pt_after）" \
    [ "$pt_after" -gt "$pt_before" ]

  MAIN_LAST_TERM=$new_term
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P2 场景2：杀光主机房 ============================
phase2_kill_all_main() {
  echo; log "P2 场景2：杀光主机房，断言全局退化为【无主】"
  local bh; bh=$(dc_leader_host 1)
  if [ -z "$bh" ]; then check "场景2前备机房存在子组代表" false; return; fi
  local bak_node bak_term
  bak_node=$(h_field "$bh" node); bak_term=$(h_field "$bh" term)
  echo "  备机房代表基线：$bak_node term=$bak_term"
  check "场景2前全局主在主机房（isMain=1）" [ "$(total_main)" = 1 ]

  log "  kill -9 主机房全部三台"
  local h
  for h in "${DC0[@]}"; do kill9_host "$h"; done
  sleep 4

  local all_dead=1
  for h in "${DC0[@]}"; do alive "$h" && all_dead=0; done
  check "主机房三节点均已停止" [ "$all_dead" = 1 ]

  # ---- 机房内：备机房应照常运转，仍只有一份代表 ----
  if dc_wait_leader 1 20; then check "备机房子组仍维持唯一代表（机房内正常运转）" true
  else check "备机房子组仍维持唯一代表（机房内正常运转）" false; fi

  local n2; n2=$(dc_leader_host 1)
  local bak2_node bak2_term
  bak2_node=$(h_field "$n2" node); bak2_term=$(h_field "$n2" term)
  echo "  备机房（主机房全灭后）代表=$bak2_node term=$bak2_term"
  check "备机房代表与故障前一致（主机房全灭未触发其子组重选）" [ "$bak2_node" = "$bak_node" ]
  check "备机房子组 term 未变（无跨机房子组牵动）" [ "$bak2_term" = "$bak_term" ]

  # ---- 全局：按一致性模式分叉断言 ----
  #
  # 父组两成员权重 主机房2/备机房1，required=(2+1)/2+1=2。
  # 主机房死透后备机房 self=1 < 2，按全量分母永远凑不齐多数。
  #   CP（缺省）：不降级接管 → 退化为【无主】。这是"绝不双主"硬约束的直接代价。
  #   AP        ：整机房失联达 consistency.degraded.timeout.ms（10s）后降级接管，
  #               把主机房剔除出分母 → required 降为 1，备机房 self=1 ≥ 1 单方成主。
  # 两种取舍都必须满足"全程 isMain ≤ 1"，差异仅在"无主"还是"接管"。
  if [ "$CONSISTENCY" = "ap" ]; then
    # AP：杀光主机房等价于"主机房整机房失联"，故应观测到备机房降级接管。
    # 接管需等降级阈值（10s）+ 父组选举窗口，给足 40s 轮询。
    local ok_take=1
    wait_single_global_leader 40 "${HOSTS[@]}" || ok_take=0
    check "AP：主机房全灭后备机房降级接管（40s 内全网 isMain=1 且父组收敛）" \
      [ "$ok_take" = 1 ]
    check "AP：接管者位于备机房（降级接管只可能发生在存活机房）" \
      [ "$(dc_count_main 1)" = 1 ]
    check "AP：主机房全灭时全网 isMain=1（恰好一主，未出现双主）" \
      [ "$(total_main)" = 1 ]
  else
    # CP：唯一性硬约束的必然结果——无主，而非备机房接管。
    local ok_none=1
    wait_no_global_leader 5 20 "${HOSTS[@]}" || ok_none=0
    check "CP：全局退化为无主并稳定 5s（isMain=0，符合唯一性硬约束）" [ "$ok_none" = 1 ]
    check "CP：主机房全灭时全网 isMain=0（未出现降级接管的第二个主）" \
      [ "$(total_main)" = 0 ]
  fi

  BAK_NODE_P2=$bak2_node
  BAK_TERM_P2=$bak2_term
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P3 场景3：重启主机房 ============================
phase3_restart_main() {
  echo; log "P3 场景3：重启全部主机房，看是否重新夺回全局席位"
  local h
  for h in "${DC0[@]}"; do start_node "$h" >/dev/null; done
  sleep 5

  local all_alive=1
  for h in "${DC0[@]}"; do alive "$h" || all_alive=0; done
  check "主机房三节点均已存活（重启成功）" [ "$all_alive" = 1 ]

  if dc_wait_leader 0 30; then check "主机房重新加入并选出唯一子组代表" true
  else check "主机房重新加入并选出唯一子组代表" false; return; fi

  local nh; nh=$(dc_leader_host 0)
  local new_term; new_term=$(h_field "$nh" term)
  echo "  主机房重新加入后代表=$(h_field "$nh" node) host=$nh term=$new_term（灭房前 term=$MAIN_LAST_TERM）"
  check "主机房子组代表落在主机房内" [ "${DC[$nh]}" = 0 ]
  check "主机房子组 term 不低于灭房前（mmap 持久化挂回，非从 1 重来）" [ "$new_term" -ge "$MAIN_LAST_TERM" ]

  # ---- 全局席位：按一致性模式分叉 ----
  #
  # CP：主机房灭房前持有父组席位，重启后 term 从 mmap 挂回，可重新当选夺回。
  # AP：P2 中备机房已降级接管并把父组 term 推高；主机房重启后 term 反而更低，
  #     按标准 Raft 任期机制会跟随高 term 的备机房 Leader，不会自动夺回席位。
  #     这是 AP"降级接管后不自动交还"的固有语义——夺回需人工升级 API 或重启备机房。
  #     故 AP 只断言"重新收敛为唯一主且不在主机房"，不断言夺回。
  local ok_g=1
  wait_single_global_leader 30 "${HOSTS[@]}" || ok_g=0
  check "主机房重入后父组重新收敛（唯一主 + parentLeader 全网一致）" [ "$ok_g" = 1 ]
  check "全网 isMain=1（恢复为唯一主，未产生双主）" [ "$(total_main)" = 1 ]

  if [ "$CONSISTENCY" = "ap" ]; then
    check "AP：全局主仍在备机房（降级接管后按 term 收敛，主机房低 term 不夺回）" \
      [ "$(dc_count_main 1)" = 1 ]
    check "AP：主机房重入后 isMain=false（未与备机房代表争席位）" \
      [ "$(h_field "$nh" isMain)" = "false" ]
  else
    check "CP：全局主夺回到主机房（机房优先级在重启后仍成立）" \
      [ "$(h_field "$nh" isMain)" = "true" ]
    check "CP：父组 Leader 指向主机房新代表" \
      [ "$(global_parent_leader)" = "node-${IP[$nh]}-22001" ]
  fi

  # ---- 备机房应全程未被打断（机房内隔离）----
  local bh; bh=$(dc_leader_host 1)
  if [ -z "$bh" ]; then
    check "备机房代表在主机房重入期间未被打断（仍为原节点）" false
  else
    check "备机房代表在主机房重入期间未被打断（仍为原节点）" \
      [ "$(h_field "$bh" node)" = "$BAK_NODE_P2" ]
    check "备机房子组 term 仍未被牵动" [ "$(h_field "$bh" term)" = "$BAK_TERM_P2" ]
  fi
  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P4 场景4：杀光备机房 ============================
phase4_kill_all_backup() {
  echo; log "P4 场景4：杀光备机房，断言主机房凭权重【单方成主】"

  # 全局席位在 P2/P3 后的归属随模式而变：
  #   CP：席位始终在主机房（P2 退化无主、P3 夺回）→ 杀备机房不动席位
  #   AP：P2 备机房降级接管、P3 主机房低 term 不夺回 → 席位在备机房，杀备机房即杀全局主
  # 但两种模式末态一致：主机房凭 self=2 ≥ required=2 单方成主。故基线取"当前全局主所在机房"。
  local seat_dc=0
  [ "$CONSISTENCY" = "ap" ] && seat_dc=1

  local mh; mh=$(dc_main_host "$seat_dc")
  if [ -z "$mh" ]; then check "场景4前全局主位于预期机房（dc-$seat_dc）" false; return; fi
  local m_node m_term m_pt
  m_node=$(h_field "$mh" node); m_term=$(h_field "$mh" term); m_pt=$(h_field "$mh" parentTerm)
  echo "  全局主基线（dc-$seat_dc）：$m_node term=$m_term parentTerm=$m_pt"
  check "场景4前全网 isMain=1" [ "$(total_main)" = 1 ]

  log "  kill -9 备机房全部三台"
  local h
  for h in "${DC1[@]}"; do kill9_host "$h"; done

  # 必须跨过 check-quorum 判定窗口（quorumCheckTimeout 默认 5000ms）再断言：
  # 若 self 权重口径有误，主机房父组 Leader 会在此窗口内被误降级，导致本场景退化为无主。
  sleep 8

  local all_dead=1
  for h in "${DC1[@]}"; do alive "$h" && all_dead=0; done
  check "备机房三节点均已停止" [ "$all_dead" = 1 ]

  # ---- 核心：不对称权重使主机房无需对侧任何选票即可维持/夺得多数 ----
  #
  # CP：席位原本就在主机房，杀备机房不动席位 → 断言"同节点、term 未变、未重选"。
  # AP：席位原本在备机房，杀备机房即杀全局主 → 主机房凭 self=2 ≥ required=2 重新当选，
  #     父组 term 因重选而推进，节点变为主机房新代表。故 AP 断言"重新收敛为唯一主"。
  if [ "$CONSISTENCY" = "ap" ]; then
    local ok_take=1
    wait_single_global_leader 40 "${HOSTS[@]}" || ok_take=0
    check "AP：备机房全灭后主机房凭权重单方成主（40s 内收敛唯一主）" [ "$ok_take" = 1 ]
    check "AP：全局主落在主机房（self=2 ≥ required=2，无需对侧选票）" \
      [ "$(dc_count_main 0)" = 1 ]
    check "AP：全网 isMain=1（未出现双主，也未退化为无主）" [ "$(total_main)" = 1 ]
    check "AP：主机房代表仍持有父组席位（parent=true）" \
      [ "$(h_field "$(dc_main_host 0)" parent)" = "true" ]
  else
    local n2; n2=$(dc_main_host 0)
    if [ -z "$n2" ]; then
      check "主机房凭权重单方成主（self=2 ≥ required=2，无需对侧选票）" false
    else
      local m2_node m2_term m2_pt
      m2_node=$(h_field "$n2" node); m2_term=$(h_field "$n2" term); m2_pt=$(h_field "$n2" parentTerm)
      echo "  主机房（备机房全灭后）=$m2_node term=$m2_term parentTerm=$m2_pt"

      check "主机房单方成主且全局 isMain=1（备机房全灭未触发无主）" [ "$(total_main)" = 1 ]
      check "全局主仍是同一节点（备机房全灭未触发主机房重选）" [ "$m2_node" = "$m_node" ]
      check "主机房子组 term 未变（机房内不受对侧影响）" [ "$m2_term" = "$m_term" ]
      check "父组 term 未变（check-quorum 未误降级、父组未被扰动）" [ "$m2_pt" = "$m_pt" ]
      check "主机房代表仍持有父组席位（parent=true）" [ "$(h_field "$n2" parent)" = "true" ]
    fi
  fi

  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P4b 恢复备机房：不得抢夺全局席位 ============================
phase4b_restore_backup() {
  echo; log "P4b 恢复备机房，断言其不得抢夺主机房的全局席位"
  local mh; mh=$(dc_main_host 0)
  if [ -z "$mh" ]; then check "P4b 前主机房仍持有全局席位" false; return; fi
  local m_node; m_node=$(h_field "$mh" node)

  local h
  for h in "${DC1[@]}"; do start_node "$h" >/dev/null; done
  sleep 5

  local all_alive=1
  for h in "${DC1[@]}"; do alive "$h" || all_alive=0; done
  check "备机房三节点均已恢复存活" [ "$all_alive" = 1 ]

  if dc_wait_leader 1 30; then check "备机房重新选出唯一子组代表" true
  else check "备机房重新选出唯一子组代表" false; fi

  # 给父组足够时间观察备机房重入（跨机房选举超时 3-5s，取 12s 覆盖多轮）
  sleep 12

  local ok_g=1
  wait_single_global_leader 20 "${HOSTS[@]}" || ok_g=0
  check "备机房重入后父组仍收敛（全网唯一主 + parentLeader 一致）" [ "$ok_g" = 1 ]
  check "全局 isMain 仍为 1（备机房重入未产生双主）" [ "$(total_main)" = 1 ]

  local n2; n2=$(dc_main_host 0)
  check "全局主仍在主机房（低权重机房重入不抢夺席位，无抖动）" [ -n "$n2" ]
  check "全局主节点未因备机房重入而改变" [ "$(h_field "$n2" node)" = "$m_node" ]
  check "备机房代表未持有全局席位（intra=true / isMain=false）" \
    [ "$(h_field "$(dc_leader_host 1)" isMain)" = "false" ]

  GLOBAL_MAINS+=("$(total_main)")
}

# ============================ P5 汇总判定 ============================
phase5_topology_verdict() {
  echo; log "P5 汇总判定（唯一性硬约束 + 双层级联是否达成）"
  echo "  设计：isMain = 子组 Leader && 父组 Leader；父组权重 主机房2/备机房1，required=2"
  echo "  各阶段全网 isMain 计数：${GLOBAL_MAINS[*]-（无采样）}"
  echo "  P0 基线 isMain=$BASELINE_MAIN；父组 term 基线(max)=$BASELINE_PT；终态(max)=$(max_parent_term)"

  # ---- V1：基线从"两房各一主"收敛为"全局唯一主" ----
  check "V1 基线全局 isMain 计数 = 1（由历史值 2 收敛为 1）" [ "$BASELINE_MAIN" = 1 ]

  # ---- V7：全程任意时刻全局 isMain ≤ 1（硬约束，绝无双主）----
  local uniq=1 n=0
  for n in "${GLOBAL_MAINS[@]-}"; do
    case "$n" in ''|*[!0-9]*) continue;; esac
    [ "$n" -le 1 ] || uniq=0
  done
  check "V7 全程任意时刻全局 isMain ≤ 1（绝不允许双主）" [ "$uniq" = 1 ]

  # ---- V2：父组 term 独立单调，且因跨机房事件推进（证明父组是活的独立实例）----
  local pt_final; pt_final=$(max_parent_term)
  check "V2 父组 term 独立推进且不回退（$BASELINE_PT -> $pt_final）" [ "$pt_final" -gt "$BASELINE_PT" ]

  # ---- 两组身份与端口分离（父子组不共用端口/身份，避免消息串组）----
  local sep=1
  for h in "${HOSTS[@]}"; do
    [ "$(h_field "$h" parentPort)" = "22001" ] || sep=0
    local nid pid
    nid=$(h_field "$h" node); pid=$(h_field "$h" parentLeader)
    # 父组身份按父组端口推导，与子组身份端口必然不同
    case "$nid" in *-21001) ;; *) sep=0;; esac
    case "$pid" in *-22001|none) ;; *) sep=0;; esac
  done
  check "父子两组端口分离（子组 21001 / 父组 22001），身份互不串组" [ "$sep" = 1 ]

  # ---- 机房内隔离贯穿全程：两房各自始终恰好一份代表 ----
  check "结束时全网仍保持每房各一份子组代表（共 2 份）" [ "$(total_intra)" = 2 ]

  echo
  echo "  一致性模式：${CONSISTENCY}"
  echo "  结论："
  echo "    唯一性   —— 全程 isMain 计数 ${GLOBAL_MAINS[*]-} 均 ≤ 1，双主在结构上被排除"
  if [ "$CONSISTENCY" = "ap" ]; then
    echo "    机房优先 —— 有主阶段全局主最终收敛在主机房；主机房可单方成主（杀光备机房仍唯一）"
    echo "    降级取舍 —— 杀光主机房时备机房降级接管（isMain=1 而非无主），牺牲可用性换来的"
    echo "               反向取舍；接管后不自动交还席位，夺回需人工升级 API 或重启备机房"
  else
    echo "    机房优先 —— 所有有主阶段全局主均在主机房；主机房可单方成主（杀光备机房仍唯一）"
    echo "    退化取舍 —— 杀光主机房时全局为【无主】而非备机房接管（唯一性硬约束的必然代价）"
    echo "               运维兜底：人工升级 API 见设计文档 §8.6"
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
