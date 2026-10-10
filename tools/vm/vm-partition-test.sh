#!/usr/bin/env bash
#
# Speedboat 阶段六：vboxnet0 网络分区专项（CP/AP 双模式）。
#
# 拓扑（见设计文档 2026-10-10-04 §9，三路语义已实测）：
#   主机房 dc-0 = vbox 三虚机 vboxdeb001/002/003，地址 172.22.133.251/252/253（enp0s8/vboxnet0）
#   备机房 dc-1 = 宿主机三个本地进程，均绑 0.0.0.0、配置地址 172.22.133.1:21001/21002/21003
#     （NettyTransport 现状 bind(port)=0.0.0.0；绑全接口是本环境单地址复用的选择，
#       非 speedboat 约束——绑具体地址会让停用后本机进程间一并断开）
#   权重 dc-0=2 / dc-1=1，required=2（主机房可单方成主，备机房不可）
#
# 分区开关（两房内部各自连通、跨房断）：
#   建立 nmcli c down vbn0_172_133（连接级停用，等效 ~/bin/disable-vboxnet0）
#     + sudo ip addr add 172.22.133.1/32 dev lo
#     （lo 别名保住备机房三进程的本机互连；虚机跨机访问仍需 vboxnet0 物理链路故断）
#   解除 ~/bin/enable-vboxnet0  + sudo ip addr del 172.22.133.1/32 dev lo
#
# 陷阱与对策（设计 §9.4，均已验证）：
#   1) 备机房进程必须绑 0.0.0.0 —— 配置地址即 172.22.133.1，天然满足；
#   2) 停用 vboxnet0 同时切断宿主机→虚机 SSH —— 虚机侧探测由 nohup 预置脚本在
#      窗口内本地采样（跨房连通性 + 本机 isMain），事后 ssh 回收 probe.log；
#   3) 虚机经 NAT 可达宿主机，但 speedboat 只连配置列出的地址，不影响分区语义。
#
# 断言（设计 §9.5，按 SB_CONSISTENCY 分叉）：
#   CP 分区内：主机房保持主（isMain=true 贯穿窗口），备机房退化为 0 主，
#              全程 isMain ≤ 1；解除分区后主机房继续持席收敛。
#   AP 分区内：备机房在降级阈值（10s）后降级接管成主，而主机房此时仍持主
#              （只因本机房存活即不降级）→ 短暂双主窗口（isMain=2），
#              且绝不超过 2；解除分区后按标准 Raft 任期收敛为唯一主。
#   两模式共同：窗口内跨房必断、两房内部必通（拓扑语义证据）；愈合后全网收敛。
#
# 用法：bash tools/vm/vm-partition-test.sh [deploy|run|stop|all]
#   deploy  宿主机预编译 + 下发虚机/宿主本地骨架
#   run     执行分区专项（假定已部署）
#   stop    停止全部进程 + 确保网络已愈合
#   all     先 deploy 再 run（默认）
# 环境变量：
#   SB_CONSISTENCY=cp|ap       一致性模式（缺省 cp）
#   SB_DEGRADED_TIMEOUT_MS=N   仅 ap：降级阈值（缺省 10000）
#
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"

# ---- 一致性模式（与 vm-crossdc-test.sh 同一语义）----
CONSISTENCY="${SB_CONSISTENCY:-cp}"
BASE_CONFIG=config-vbox-partition.properties

# ---- 环境清单（source 公共库之前须定义）----
SSH_USER=vboxuser
SSH=(ssh -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no
      -o ControlMaster=auto -o ControlPath=/tmp/sb-parttest-%r@%h -o ControlPersist=180)
SCP=(scp -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no
      -o ControlMaster=auto -o ControlPath=/tmp/sb-parttest-%r@%h -o ControlPersist=180)

# 主机房（虚机）
DC0=(vboxdeb001 vboxdeb002 vboxdeb003)
declare -A IP=(
  [vboxdeb001]=172.22.133.251
  [vboxdeb002]=172.22.133.252
  [vboxdeb003]=172.22.133.253
)
declare -A PORT=([vboxdeb001]=21001 [vboxdeb002]=21001 [vboxdeb003]=21001)

. "$REPO/tools/vm/vm-test-lib.sh"

# ---- 跨机房辅助（与 vm-crossdc-test.sh 同语义；本脚本全网 = 虚机三台 + 宿主三进程）----
h_field() { fld "$(state_line "$1")" "$2"; }
total_main() { echo "$(( $(count_main_in "${DC0[@]}") + $(host_count_main) ))"; }
max_parent_term() {
  local h m=0 t
  for h in "${DC0[@]}"; do
    alive "$h" || continue
    t=$(h_field "$h" parentTerm)
    case "$t" in ''|*[!0-9]*) continue;; esac
    [ "$t" -gt "$m" ] && m=$t
  done
  local p
  for p in "${HOST_PORTS[@]}"; do
    t=$(hfld "$p" parentTerm)
    case "$t" in ''|*[!0-9]*) continue;; esac
    [ "$t" -gt "$m" ] && m=$t
  done
  echo "$m"
}

# ---- 备机房（宿主机三进程，绑 0.0.0.0、配置地址 172.22.133.1）----
HOST_PROC_IP=172.22.133.1
HOST_PORTS=(21001 21002 21003)
HOST_BASE="$HOME/speedboat-test"
HOST_PORTS_ALL=(21001 21002 21003 22001 22002 22003)  # 子组+父组全端口，用于连通探测

CONFIG=""
LOCK=vm-lock
JAR="$REPO/target/speedboat-1.0-SNAPSHOT.jar"
LOGBACK="$REPO/tools/vm/logback-vm.xml"
CTL="$REPO/tools/vm/vm_node_ctl.sh"
RUNNER_SRC="$REPO/src/test/java/cn/itcraft/speedboat/sample/VmClusterNode.java"
JDK_HOME="${SB_JDK_HOME:-$HOME/lang/dragonwell-8.29.28}"

# 宿主机预编译产物（虚机与宿主进程共用）
RUNNER_DIR="$REPO/target/vm-runner"
RUNNER_JAR="$REPO/target/vm-runner.jar"
PROBE_SRC="$REPO/target/vbox-probe.sh"   # 生成的虚机侧预置探测脚本

# ============================ 生效配置与探测脚本 ============================
prepare_config() {
  case "$CONSISTENCY" in
    cp|ap) ;;
    *) log "SB_CONSISTENCY 取值非法：$CONSISTENCY（仅 cp / ap）"; exit 64 ;;
  esac
  CONFIG="config-vbox-partition-${CONSISTENCY}.properties"
  mkdir -p "$REPO/target"
  cp "$REPO/$BASE_CONFIG" "$REPO/target/$CONFIG" || { log "生成生效配置失败"; exit 1; }
  {
    echo ""
    echo "# ---- 一致性模式（由 vm-partition-test.sh 按 SB_CONSISTENCY 追加）----"
    echo "consistency.policy=${CONSISTENCY}"
    if [ "$CONSISTENCY" = "ap" ]; then
      echo "consistency.degraded.timeout.ms=${SB_DEGRADED_TIMEOUT_MS:-10000}"
    fi
  } >> "$REPO/target/$CONFIG"
  log "生效配置 $CONFIG（consistency.policy=${CONSISTENCY}）"
}

# 虚机侧预置探测：停用窗口内宿主机 SSH 不可达，虚机只能本地 nohup 采样。
# 参数 $1=采样时长秒；每 2s 记录一行：跨房连通性（→172.22.133.1）、本机房连通性
# （→参数 $2 指定的兄弟虚机）、本机 isMain 与 STATE 快照，全部追加 probe.log。
prepare_probe() {
  cat > "$PROBE_SRC" << 'EOS'
#!/bin/bash
OUT="$HOME/speedboat-test/probe.log"
DUR="${1:-120}"
PEER="$2"
END=$(( $(date +%s) + DUR ))
echo "$$" > "$HOME/speedboat-test/probe.pid"
while [ "$(date +%s)" -lt "$END" ]; do
  ts=$(date +%H:%M:%S)
  cross=FAIL
  timeout 1 bash -c "exec 3<>/dev/tcp/${HOST_PROC_IP}/21001" 2>/dev/null && cross=OK
  intra=FAIL
  [ -n "$PEER" ] && timeout 1 bash -c "exec 3<>/dev/tcp/${PEER}/21001" 2>/dev/null && intra=OK
  line=$(grep 'event=STATE' "$HOME"/speedboat-test/logs/*.log 2>/dev/null | tail -1)
  im=$(echo "$line" | sed -n 's/.*isMain=\([^ ]*\).*/\1/p')
  node=$(echo "$line" | sed -n 's/.* node=\([^ ]*\).*/\1/p')
  echo "probe ts=$ts cross=$cross intra=$intra isMain=${im:-?} node=${node:-?}" >> "$OUT"
  sleep 2
done
rm -f "$HOME/speedboat-test/probe.pid"
EOS
  # 跨房目标地址注入（宿主进程的 172.22.133.1）
  sed -i "s|\${HOST_PROC_IP}|${HOST_PROC_IP}|" "$PROBE_SRC"
}

# ============================ 部署 ============================
deploy() {
  prepare_config
  prepare_probe
  log "P0a 宿主机预编译测试节点（字节码架构无关）"
  [ -f "$JAR" ] || { log "缺少 jar：$JAR（先 mvn package）"; exit 1; }
  local javac="$JDK_HOME/bin/javac" jarbin="$JDK_HOME/bin/jar"
  [ -x "$javac" ] || { log "缺少 javac：$javac（可用 SB_JDK_HOME 指定）"; exit 1; }
  rm -rf "$RUNNER_DIR"; mkdir -p "$RUNNER_DIR"
  "$javac" -source 8 -target 8 -cp "$JAR" -d "$RUNNER_DIR" "$RUNNER_SRC" \
    || { log "编译测试节点失败"; exit 1; }
  (cd "$RUNNER_DIR" && "$jarbin" cf "$RUNNER_JAR" .) || { log "打包 vm-runner.jar 失败"; exit 1; }
  log "  -> $RUNNER_JAR"

  log "P0b 下发虚机并校验"
  local h
  for h in "${DC0[@]}"; do
    rsh "$h" 'mkdir -p ~/speedboat-test/{logs,run,runner,data}' || { log "$h mkdir 失败"; exit 1; }
    scph "$h" "$JAR"          "~/speedboat-test/speedboat.jar"
    scph "$h" "$RUNNER_JAR"   "~/speedboat-test/vm-runner.jar"
    scph "$h" "$REPO/target/$CONFIG" "~/speedboat-test/$CONFIG"
    scph "$h" "$LOGBACK"      "~/speedboat-test/logback-vm.xml"
    scph "$h" "$CTL"          "~/speedboat-test/vm_node_ctl.sh"
    scph "$h" "$PROBE_SRC"    "~/speedboat-test/vbox-probe.sh"
    rsh "$h" 'chmod +x ~/speedboat-test/vm_node_ctl.sh ~/speedboat-test/vbox-probe.sh' \
      || { log "$h chmod 失败"; exit 1; }
    echo "  $h 部署 OK"
  done

  log "P0c 备机房（宿主机）本地骨架"
  mkdir -p "$HOST_BASE"/{logs,run,data}
  cp "$JAR"        "$HOST_BASE/speedboat.jar"
  cp "$RUNNER_JAR" "$HOST_BASE/vm-runner.jar"
  cp "$REPO/target/$CONFIG" "$HOST_BASE/$CONFIG"
  cp "$LOGBACK"    "$HOST_BASE/logback-vm.xml"
  cp "$CTL"        "$HOST_BASE/vm_node_ctl.sh"
  chmod +x "$HOST_BASE/vm_node_ctl.sh"
  # 三进程共用 PID 目录，须以 SB_PID_SUFFIX 区分（vm_node_ctl.sh 支持）
  grep -q 'SB_PID_SUFFIX' "$HOST_BASE/vm_node_ctl.sh" \
    || { log "vm_node_ctl.sh 缺少 SB_PID_SUFFIX 支持，需先更新工具脚本"; exit 1; }
  echo "  宿主机骨架 OK"
}

# ============================ 进程控制 ============================
# 虚机（远端 vm_node_ctl.sh，经 vboxnet0 SSH；IP 唯一亦显式钉端口，校验配置一致性）
vm_start()  { rsh "$1" "~/speedboat-test/vm_node_ctl.sh start ${IP[$1]} $CONFIG run $LOCK $(rlog "$1") 21001"; }
vm_stop()   { rsh "$1" '~/speedboat-test/vm_node_ctl.sh stop'  >/dev/null 2>&1; }
vm_alive()  { alive "$1"; }

# 备机房（宿主机本地 vm_node_ctl.sh，SB_PID_SUFFIX 区分三进程、localPort 区分身份）
host_start() {
  local idx=$1 port=${HOST_PORTS[$1]}
  SB_PID_SUFFIX="-h$idx" SB_JAVA="$JDK_HOME/bin/java" \
    bash "$HOST_BASE/vm_node_ctl.sh" start "$HOST_PROC_IP" "$CONFIG" run "$LOCK" "logs/host-$port.log" "$port" \
    | sed "s/^/  [host:$port] /"
}
host_stop() {
  local i
  for i in 0 1 2; do
    SB_PID_SUFFIX="-h$i" bash "$HOST_BASE/vm_node_ctl.sh" stop >/dev/null 2>&1
  done
}
host_alive() {
  SB_PID_SUFFIX="-h$1" bash "$HOST_BASE/vm_node_ctl.sh" alive | grep -q alive
}

# 宿主进程 STATE 字段（本地 grep，分区窗口内 SSH 不依赖）
hfld() { fld "$(grep 'event=STATE' "$HOST_BASE/logs/host-$1.log" 2>/dev/null | tail -1)" "$2"; }
host_count_main() {
  local p n=0
  for p in "${HOST_PORTS[@]}"; do [ "$(hfld "$p" isMain)" = "true" ] && n=$((n+1)); done
  echo "$n"
}
host_wait_single_main() {   # host_wait_single_main <超时秒> → 恰好 1 个 host 进程持 isMain
  local secs=$1 i
  for ((i = 0; i < secs; i++)); do
    [ "$(host_count_main)" = 1 ] && return 0
    sleep 1
  done
  return 1
}
host_wait_no_main() {       # host_wait_no_main <保持秒> <超时秒> → 持续 0 主
  local hold=$1 timeout=$2 i seen=0
  for ((i = 0; i < timeout; i++)); do
    if [ "$(host_count_main)" = 0 ]; then
      seen=$((seen + 1)); [ "$seen" -ge "$hold" ] && return 0
    else
      return 1
    fi
    sleep 1
  done
  return 1
}

# 全网收敛判定（分区拓扑专用）：六端点（虚机三 SSH + 宿主三本地）的 parentLeader
# 全部一致（且为父组端口身份），全网 isMain 恰为 1。
# vm-test-lib 的 wait_single_global_leader 只统计传入的主机清单，AP 愈合后席位在
# 备机房宿主侧、DC0 内 isMain 恒 0，用它会永不收敛——故此处按两房合并计数。
wait_all_converged() {
  local secs=$1 i h p
  for ((i = 0; i < secs; i++)); do
    local ref="" pl bad=0
    for h in "${DC0[@]}"; do
      alive "$h" || { bad=1; break; }
      pl=$(h_field "$h" parentLeader)
      case "$pl" in
        *-22001|*-22002|*-22003) ;;
        *) bad=1; break ;;
      esac
      if [ -z "$ref" ]; then ref=$pl; elif [ "$pl" != "$ref" ]; then bad=1; break; fi
    done
    if [ "$bad" = 0 ]; then
      for p in "${HOST_PORTS[@]}"; do
        pl=$(hfld "$p" parentLeader)
        case "$pl" in
          *-22001|*-22002|*-22003) ;;
          *) bad=1; break ;;
        esac
        [ "$pl" = "$ref" ] || { bad=1; break; }
      done
    fi
    if [ "$bad" = 0 ] && [ "$(total_main)" = 1 ]; then
      CONVERGED_LEADER="$ref"
      return 0
    fi
    sleep 1
  done
  return 1
}

# ============================ 网络分区开关 ============================
NET_HOLDED=0   # 1=当前处于停用窗口（lo 别名已挂），stop 必须回收
net_partition_on() {
  log "  建立分区：停用 vbn0_172_133 + lo 别名 ${HOST_PROC_IP}/32"
  # 停用连接级即可断开 vboxnet0（第二条冗余的设备级 disconnect 在设备已停用时
  # 会报 "not active" 使 ~/bin/disable-vboxnet0 返回 6，故此处直接调连接级命令，
  # 以跨房探测自检为真凭据而非依赖退出码）
  sudo -n nmcli c down vbn0_172_133 2>/dev/null \
    || sudo nmcli c down vbn0_172_133 || { log "停用 vbn0_172_133 失败"; return 1; }
  sudo -n ip addr add "${HOST_PROC_IP}/32" dev lo 2>/dev/null \
    || sudo ip addr add "${HOST_PROC_IP}/32" dev lo || { log "lo 别名挂载失败"; return 1; }
  NET_HOLDED=1
  # 拓扑语义自检（全部本地，不经 SSH）：
  #   跨房：宿主 → 虚机 host-only 必须断；备机房内：宿主进程间 loopback 必须通
  local cross=OK intra=OK p
  timeout 1 bash -c "exec 3<>/dev/tcp/${IP[vboxdeb001]}/21001" 2>/dev/null || cross=FAIL
  for p in "${HOST_PORTS_ALL[@]}"; do
    timeout 1 bash -c "exec 3<>/dev/tcp/${HOST_PROC_IP}/$p" 2>/dev/null || intra=FAIL
  done
  echo "    拓扑自检：跨房(宿→虚)=$cross 备机房内(宿本地)=$intra"
  [ "$cross" = "FAIL" ] && [ "$intra" = "OK" ]
}

net_partition_off() {
  log "  解除分区：启用 vbn0_172_133 + 删除 lo 别名"
  sudo -n nmcli c up vbn0_172_133 2>/dev/null \
    || sudo nmcli c up vbn0_172_133 || { log "启用 vbn0_172_133 失败"; return 1; }
  sudo -n ip addr del "${HOST_PROC_IP}/32" dev lo 2>/dev/null \
    || sudo ip addr del "${HOST_PROC_IP}/32" dev lo 2>/dev/null || true
  NET_HOLDED=0
  # 愈合自检：跨房必须恢复
  local cross=FAIL i
  for ((i = 0; i < 15; i++)); do
    timeout 1 bash -c "exec 3<>/dev/tcp/${IP[vboxdeb001]}/21001" 2>/dev/null && { cross=OK; break; }
    sleep 1
  done
  echo "    愈合自检：跨房(宿→虚)=$cross"
  [ "$cross" = "OK" ]
}

# ============================ 虚机侧探针 ============================
probe_start() {   # probe_start <时长>：三虚机各按兄弟地址预置 nohup 探测
  local h dur=$1 peer
  rm -rf "$REPO/target/partition-probe"   # 清上一轮残留，避免陈旧探针日志混入统计
  for h in "${DC0[@]}"; do
    case "$h" in
      vboxdeb001) peer=${IP[vboxdeb002]};;
      vboxdeb002) peer=${IP[vboxdeb003]};;
      vboxdeb003) peer=${IP[vboxdeb001]};;
    esac
    rsh "$h" "rm -f ~/speedboat-test/probe.log ~/speedboat-test/probe.pid; \
      nohup bash ~/speedboat-test/vbox-probe.sh $dur $peer >/dev/null 2>&1 & echo probe-started"
  done
}
probe_fetch() {   # probe_fetch：回收三虚机 probe.log 到 target/
  local h
  mkdir -p "$REPO/target/partition-probe"
  for h in "${DC0[@]}"; do
    rsh "$h" 'sleep 1' >/dev/null 2>&1   # 控制通道预热（愈合后首次连接）
    "${SCP[@]}" "${SSH_USER}@$(host_target "$h"):~/speedboat-test/probe.log" \
      "$REPO/target/partition-probe/$h.log" >/dev/null 2>&1 \
      || log "  回收 $h probe.log 失败"
  done
}
probe_kill() {    # 按 pid 文件回收探针（避免 pkill -f 匹配自身）
  local h
  for h in "${DC0[@]}"; do
    rsh "$h" 'p=$(cat ~/speedboat-test/probe.pid 2>/dev/null) && kill "$p" 2>/dev/null; true' \
      >/dev/null 2>&1
  done
}
probe_probe_files() { ls "$REPO/target/partition-probe"/*.log 2>/dev/null; }

# 探针聚合：停用窗口内（首个 FAIL 至其后首个 OK 之间）某虚机 isMain=true 的样本数。
# 逐文件独立统计（三份日志拼接会重置窗口序列，导致判定失真）。
probe_in_window_main_samples() {
  local f n=0 c
  for f in "$REPO"/target/partition-probe/*.log; do
    [ -f "$f" ] || continue
    # 窗口 = 首个 FAIL 行至其后首个 OK 行（含 FAIL 行本身）；
    # 早期版本在 FAIL 分支 next 跳过计数规则，而窗口内样本恰好全是 FAIL 行，恒漏计
    c=$(awk '/cross=FAIL/{if(!w && !e) w=1}
             /cross=OK/{if(w && !e) e=1}
             w && !e && /isMain=true/{c++}
             END{print c+0}' "$f")
    n=$((n + c))
  done
  echo "$n"
}

# 停用窗口内出现 isMain=true 的虚机文件数（≥1 = 主机房在窗口内保持主）。
# isMain ⊆ intra 且机房内 intra=1，故该值结构上 ≤1。
probe_in_window_main_files() {
  local f n=0
  for f in "$REPO"/target/partition-probe/*.log; do
    [ -f "$f" ] || continue
    local c
    c=$(awk '/cross=FAIL/{if(!w && !e) w=1}
             /cross=OK/{if(w && !e) e=1}
             w && !e && /isMain=true/{c++}
             END{print c+0}' "$f")
    [ "$c" -gt 0 ] && n=$((n + 1))
  done
  echo "$n"
}

# 停用窗口内跨房恒断核验：每份探针日志必须出现 FAIL 窗口（started），
# 且窗口闭合（出现 OK）之前不得混入 OK、闭合之后不得再出现 FAIL。
probe_window_intact() {
  local f bad=0 nostart=0 any=0
  for f in "$REPO"/target/partition-probe/*.log; do
    [ -f "$f" ] || continue
    any=1
    local r
    r=$(awk '
      /cross=FAIL/ { if (e) bad=1; else s=1; next }
      /cross=OK/   { if (s && !e) e=1 }
      END { if (!s) print "nostart"; else if (bad) print "bad"; else print "ok" }' "$f")
    [ "$r" = "nostart" ] && nostart=1
    [ "$r" = "bad" ] && bad=1
  done
  [ "$any" = 1 ] && [ "$bad" = 0 ] && [ "$nostart" = 0 ]
}
probe_intra_always_ok() {   # 探针窗口内本机房连通必须恒 OK（FAIL 样本=0）
  local n
  n=$(grep -h "intra=FAIL" "$REPO"/target/partition-probe/*.log 2>/dev/null | wc -l)
  [ "$n" -eq 0 ]
}

# ============================ 阶段 ============================
HOST_WIN_MAIN=0   # 分区窗口末宿主侧主数（phase1 记录，phase2 聚合窗口全网计数）
CONVERGED_LEADER=""  # 愈合后收敛的父组 Leader 身份（wait_all_converged 回填，供打印）

phase0_baseline() {
  echo; log "P0 基线：六进程（虚机三 + 宿主三）启动，主机房当选全局唯一主"
  local h
  for h in "${DC0[@]}"; do vm_stop "$h"; rsh "$h" 'rm -rf ~/speedboat-test/data ~/speedboat-test/logs; mkdir -p ~/speedboat-test/logs ~/speedboat-test/data' >/dev/null 2>&1; done
  host_stop
  rm -rf "$HOST_BASE/data" "$HOST_BASE/logs"; mkdir -p "$HOST_BASE/logs" "$HOST_BASE/data"
  for h in "${DC0[@]}"; do vm_start "$h" >/dev/null; done
  local i
  for i in 0 1 2; do host_start "$i"; done
  sleep 3

  # 机房内唯一代表（虚机房）
  if wait_single_intra 30 "${DC0[@]}"; then check "主机房（虚机三台）30s 内选出唯一子组代表" true
  else check "主机房（虚机三台）30s 内选出唯一子组代表" false; fi
  # 备机房（宿主三进程）：恰好一个 intra
  local n_intra=0 i2
  for ((i2 = 0; i2 < 30; i2++)); do
    n_intra=0
    for p in "${HOST_PORTS[@]}"; do [ "$(hfld "$p" intra)" = "true" ] && n_intra=$((n_intra+1)); done
    [ "$n_intra" -eq 1 ] && break
    sleep 1
  done
  check "备机房（宿主三进程）30s 内选出唯一子组代表" [ "$n_intra" = 1 ]

  # 全局唯一主：isMain=1 且落在虚机（主机房）
  if wait_single_global_leader 30 "${DC0[@]}"; then check "父组 30s 内收敛（全网唯一主 + parentLeader 一致）" true
  else check "父组 30s 内收敛（全网唯一主 + parentLeader 一致）" false; fi
  check "基线全网 isMain=1（主机房当选全局唯一主）" [ "$(total_main)" = 1 ]
  local mh; mh=$(leader_host_in "${DC0[@]}")
  check "全局主落在主机房（虚机侧，机房优先级生效）" [ -n "$mh" ]
  check "备机房（宿主）全程无 isMain（基线 isMain=0）" [ "$(host_count_main)" = 0 ]

  # 身份与端口
  local nid_ok=1 port_ok=1 h2
  for h2 in "${DC0[@]}"; do
    node_id_ok "$h2" || nid_ok=0
    [ "$(h_field "$h2" parentPort)" = "22001" ] || port_ok=0
  done
  check "虚机 nodeId 按 172.22.133.25x:21001 确定性推导" [ "$nid_ok" = 1 ]
  check "虚机父组端口 22001（子组 21001 + offset 1000）" [ "$port_ok" = 1 ]
  # 宿主三进程 nodeId 验证：node-172.22.133.1-2100x（同 IP 多进程身份钉定）
  local hid_ok=1 p
  for p in "${HOST_PORTS[@]}"; do
    [ "$(hfld "$p" node)" = "node-${HOST_PROC_IP}-${p}" ] || hid_ok=0
    [ "$(hfld "$p" parentPort)" = "$((p + 1000))" ] || hid_ok=0
  done
  check "宿主三进程 nodeId=node-172.22.133.1-2100x 且父组端口 +1000" [ "$hid_ok" = 1 ]

  BASELINE_MAIN=$(total_main)
  BASELINE_PT=$(max_parent_term)
  # mh 可能为空（基线失败时），须先判空再查字段，避免向空主机名发 SSH
  [ -n "$mh" ] && mh_info=$(h_field "$mh" node) || mh_info="(无)"
  echo "  基线：全网 isMain=$BASELINE_MAIN parentTerm(max)=$BASELINE_PT 主=$mh_info"
  GLOBAL_MAINS+=("$(total_main)")
}

phase1_partition_window() {
  echo; log "P1 分区窗口：断跨房链路（vboxnet0），观测 CP/AP 差异"
  check "场景前全网 isMain=1（主机房持主）" [ "$(total_main)" = 1 ]

  # 探针窗口 = 建立分区前即启动，覆盖整个停用窗口 + 愈合后 20s 尾样
  local probe_dur=110
  probe_start "$probe_dur"

  net_partition_on || { check "建立分区（disable-vboxnet0 + lo 别名）" false; return; }
  check "建立分区成功：跨房断、备机房内通（本地拓扑自检）" true

  # 观测窗口：显著大于降级阈值（AP 10s）+ 父组选举窗口。
  # 停用同时切断宿→虚 SSH（设计 §9.4 陷阱 2），窗口内虚机侧一律不可观测——
  # 本阶段只做宿主本地观测；虚机侧证据由 nohup 探针采样、phase2 愈合后回收核验。
  local win=30
  if [ "$CONSISTENCY" = "ap" ]; then
    local ok_take=1
    host_wait_single_main "$win" || ok_take=0
    check "AP：窗口内备机房降级接管（${win}s 内宿主恰 1 个 isMain=true）" [ "$ok_take" = 1 ]
    check "AP：接管后宿主侧仍恒 ≤1 主（双主上限本侧不超 1）" [ "$(host_count_main)" -le 1 ]
    sleep 5
  else
    # CP：完整观测 win 秒，备机房全程不得出现 isMain（权重 1 < required 2 永远无法接管）。
    # 逐秒断言而非"连续 5s 即返回"——窗口须给足备机房尝试选举的时间，短窗口证据不足。
    local ok_none=1 i
    for ((i = 0; i < win; i++)); do
      if [ "$(host_count_main)" != 0 ]; then ok_none=0; break; fi
      sleep 1
    done
    check "CP：窗口内备机房持续无主（isMain=0 满 ${win}s，绝不降级接管）" [ "$ok_none" = 1 ]
    check "CP：窗口内宿主侧 isMain=0（主机房存活即权重不足不可接管）" \
      [ "$(host_count_main)" = 0 ]
    sleep 5
  fi
  HOST_WIN_MAIN=$(host_count_main)   # 窗口末宿主侧主数，phase2 与探针聚合
}

phase2_heal() {
  echo; log "P2 解除分区：恢复 vboxnet0，观测愈合收敛"
  net_partition_off || { check "解除分区（enable-vboxnet0 + 删 lo 别名）" false; return; }
  check "解除分区成功（跨房恢复，本地愈合自检）" true

  # 等探针跑完窗口尾样再回收（探针 dur 覆盖愈合后 20s+）
  sleep 8
  probe_fetch

  # ---- 窗口全网证据聚合（探针回收后，虚机侧才可观测——设计 §9.4 陷阱 2）----
  local vm_win_files; vm_win_files=$(probe_in_window_main_files)
  local win_global=$(( HOST_WIN_MAIN + vm_win_files ))
  echo "  窗口证据：宿主侧主=$HOST_WIN_MAIN 虚机侧窗口持主文件数=$vm_win_files → 全网=$win_global"
  if [ "$CONSISTENCY" = "ap" ]; then
    check "AP：双主窗口成立（宿主降级接管=1 且主机房窗口内仍持主 ⇒ isMain=2）" \
      [ "$win_global" = 2 ]
    check "AP：双主窗口未越界（全网 isMain=2 ≤ 上限 2）" [ "$win_global" -le 2 ]
  else
    check "CP：窗口内主机房保持主（探针显示虚机房 isMain=true 贯穿窗口）" \
      [ "$vm_win_files" -ge 1 ]
    check "CP：窗口全网 isMain ≤ 1（宿主 0 + 虚机房 ≤1）" [ "$win_global" -le 1 ]
  fi
  GLOBAL_MAINS+=("$win_global")

  # 愈合收敛：六端点 parentLeader 全一致 + 全网唯一主（覆盖席位在任一房的情形）
  CONVERGED_LEADER=""
  if wait_all_converged 40; then check "愈合后 40s 内全网收敛为唯一主（parentLeader 一致）" true
  else check "愈合后 40s 内全网收敛为唯一主（parentLeader 一致）" false; fi
  local tm; tm=$(total_main)
  check "愈合后全网 isMain=1（归一单主，两模式共同硬断言）" [ "$tm" = 1 ]
  GLOBAL_MAINS+=("$tm")

  # 探针语义核验（逐文件）
  if probe_window_intact; then check "探针证据：停用窗口内跨房链路全程断（FAIL 区间无 OK 混入）" true
  else check "探针证据：停用窗口内跨房链路全程断（FAIL 区间无 OK 混入）" false; fi
  if probe_intra_always_ok; then check "探针证据：窗口内主机房内部链路全程通（intra 无 FAIL）" true
  else check "探针证据：窗口内主机房内部链路全程通（intra 无 FAIL）" false; fi

  if [ "$CONSISTENCY" = "cp" ]; then
    # CP 愈合：主机房全程持席，解除后仍是主机房持主
    local mh; mh=$(leader_host_in "${DC0[@]}")
    check "CP：愈合后全局主仍在主机房（分区未动摇席位）" [ -n "$mh" ]
    check "CP：备机房愈合后仍无 isMain（降级接管全程未发生）" [ "$(host_count_main)" = 0 ]
  else
    # AP 愈合：降级接管抬高了备机房侧 term，按标准 Raft 任期收敛——
    # 不强制归属哪一房，只断言唯一（归属以 term 竞争结果为准）。
    local holder=""
    [ "$(count_main_in "${DC0[@]}")" = 1 ] && holder=dc-0
    [ "$(host_count_main)" = 1 ] && holder=dc-1
    echo "    AP 愈合后席位归属：${holder:-unknown}（term 竞争结果，不断言唯一归属）"
    check "AP：愈合后双主窗口关闭（全网恰 1 主）" [ "$tm" = 1 ]
  fi
}

phase3_verdict() {
  echo; log "P3 汇总判定（分区专项，模式=${CONSISTENCY}）"
  echo "  设计：isMain = 子组 Leader && 父组 Leader；权重 主机房2/备机房1，required=2"
  echo "  各阶段全网 isMain 计数：${GLOBAL_MAINS[*]-（无采样）}"

  # 双主上限：CP 恒 1；AP 分区窗口允许 2（愈合后必须回到 1，由 phase2 断言）
  local limit=1 uniq=1 n=0
  [ "$CONSISTENCY" = "ap" ] && limit=2
  for n in "${GLOBAL_MAINS[@]-}"; do
    case "$n" in ''|*[!0-9]*) continue;; esac
    [ "$n" -le "$limit" ] || uniq=0
  done
  check "全程全网 isMain ≤ ${limit}（${CONSISTENCY} 模式双主上限）" [ "$uniq" = 1 ]
  check "基线 isMain=1（主机房当选全局唯一主）" [ "${BASELINE_MAIN:-0}" = 1 ]

  # 拓扑语义统计（探针已核验，此处打印证据量）
  local n_fail n_ok
  n_fail=$(grep -h "cross=FAIL" "$REPO"/target/partition-probe/*.log 2>/dev/null | wc -l)
  n_ok=$(grep -h "intra=OK" "$REPO"/target/partition-probe/*.log 2>/dev/null | wc -l)
  echo "  探针统计：跨房 FAIL 样本 $n_fail 个 / 房内 OK 样本 $n_ok 个"

  echo
  echo "  结论："
  if [ "$CONSISTENCY" = "ap" ]; then
    echo "    AP —— 分区超时后备机房降级接管（短暂双主窗口 isMain=2），愈合后按 term 收敛单主"
  else
    echo "    CP —— 分区全程备机房无主、主机房持席，恒 ≤1，绝不降级接管"
  fi
  echo "    拓扑 —— 停用窗口内跨房断、两房内部各自连通（vboxnet0 三路语义）"
}

# ============================ 清理 ============================
cleanup() {
  echo; log "P4 清理：探针回收 + 进程停止 + 网络愈合确认"
  probe_kill 2>/dev/null
  local h
  for h in "${DC0[@]}"; do vm_stop "$h"; done
  host_stop
  # 网络必须处于愈合态（无论异常退出路径）
  if [ "$NET_HOLDED" = 1 ]; then
    net_partition_off || log "  警告：网络愈合失败，请手工执行 ~/bin/enable-vboxnet0 并删除 lo 别名"
  fi
  local residue
  residue=$(ip addr show lo | grep -c "${HOST_PROC_IP}/32" || true)
  log "  lo 残留别名检查：$([ "$residue" = 0 ] && echo 无 || echo 有残留) ${HOST_PROC_IP}/32"
  log "  vboxnet0 状态：$(ip link show vboxnet0 2>/dev/null | grep -o 'state [A-Z]*' | head -1)"
}

# 同步生效配置到全部执行点（run 模式复用既有部署时，须重发按 SB_CONSISTENCY
# 新生成的配置——虚机远端与宿主机骨架都可能停留在上一次模式的配置上）
sync_config() {
  local h
  for h in "${DC0[@]}"; do
    scph "$h" "$REPO/target/$CONFIG" "~/speedboat-test/$CONFIG" || { log "$h 配置下发失败"; exit 1; }
  done
  cp "$REPO/target/$CONFIG" "$HOST_BASE/$CONFIG" || { log "宿主机配置复制失败"; exit 1; }
  log "生效配置已同步至三虚机与宿主机骨架"
}

run_all() {
  prepare_config   # run 模式不重新部署，但必须生成生效配置（CONFIG 变量由此确定）
  sync_config
  phase0_baseline
  phase1_partition_window
  phase2_heal
  phase3_verdict

  cleanup
  report
}

# review L5：异常退出（Ctrl-C / 中途 exit）时确保清理照常执行。
# 正常路径 cleanup 本就幂等（probe_kill/vm_stop 吞错、NET_HOLDED=0 时跳过网络分支），
# EXIT 重复触发零副作用；trap - EXIT 防递归。
trap 'cleanup; trap - EXIT' EXIT

case "${1:-all}" in
  deploy) deploy ;;
  run)    run_all ;;
  stop)   cleanup ;;
  all)    deploy && run_all ;;
  *) echo "usage: $0 [deploy|run|stop|all]"; exit 64 ;;
esac
