#!/bin/bash

# Speedboat 集群选举测试脚本
# 测试场景：
# 1. 启动三个节点，观察主节点选举
# 2. 杀掉主节点，观察重新选举  
# 3. 重启原主节点，观察重加入

echo "=== Speedboat 集群选举集成测试 ==="
echo "开始时间: $(date)"
echo ""

# 设置Java环境
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# 设置项目路径
PROJECT_DIR="/disk2/helly_data/code/java/speedboat"
cd "$PROJECT_DIR"

echo "1. 编译项目..."
mvn clean compile -q
if [ $? -ne 0 ]; then
    echo "编译失败"
    exit 1
fi
echo "编译成功"
echo ""

echo "2. 运行集群选举测试..."
echo "测试将启动3个节点，观察选举行为"
echo ""

# 编译测试类
mvn test-compile -q

# 运行测试
java -cp "target/classes:target/test-classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt 2>/dev/null && cat /tmp/cp.txt)" \
    cn.itcraft.speedboat.test.ClusterElectionIntegrationTest

TEST_RESULT=$?

echo ""
echo "测试结束时间: $(date)"

if [ $TEST_RESULT -eq 0 ]; then
    echo "=== 测试执行完成 ==="
    echo "请查看日志了解详细测试结果"
else
    echo "=== 测试执行失败 ==="
    exit 1
fi

echo ""
echo "附加验证: 运行所有单元测试确保功能完整"
echo "mvn test -DskipTests=false"

exit 0
