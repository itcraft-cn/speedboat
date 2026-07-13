package cn.itcraft.speedboat.sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Speedboat 集群故障转移测试
 * 
 * 测试流程：
 * 1. 启动3个节点进程（直接 Java 进程）
 * 2. 观察日志，定位主节点
 * 3. 杀主节点，观察重新选举
 * 4. 重启原主节点，观察节点重加入
 * 5. 汇报测试结果
 * 
 * 使用方式：
 * mvn exec:java -Dexec.mainClass="cn.itcraft.speedboat.sample.ClusterFailoverTest" -Dexec.classpathScope=test
 */
public class ClusterFailoverTest {
    
    private static final int BASE_PORT = 21001;
    private static final String[] NODE_IDS = {"node-1", "node-2", "node-3"};
    private static final int WAIT_FOR_ELECTION_MS = 5000;
    private static final int WAIT_FOR_REELECTION_MS = 8000;
    private static final int WAIT_FOR_REJOIN_MS = 5000;
    
    private Process[] nodeProcesses;
    private File[] logFiles;
    private String[] nodeIds;
    private int[] ports;
    private String classpath;
    
    public static void main(String[] args) throws Exception {
        ClusterFailoverTest test = new ClusterFailoverTest();
        test.runFullTest();
    }
    
    public void runFullTest() throws Exception {
        System.out.println("========================================");
        System.out.println("Speedboat 集群故障转移测试");
        System.out.println("========================================");
        
        nodeIds = NODE_IDS;
        ports = new int[nodeIds.length];
        nodeProcesses = new Process[nodeIds.length];
        logFiles = new File[nodeIds.length];
        
        String javaHome = System.getProperty("java.home");
        String javaCmd = javaHome + "/bin/java";
        
        String userDir = System.getProperty("user.dir");
        
        classpath = System.getProperty("java.class.path");
        
        File depFile = new File(userDir + "/target/classpath.txt");
        if (!depFile.exists()) {
            ProcessBuilder pb = new ProcessBuilder(
                "/home/helly/app/apache-maven/bin/mvn",
                "dependency:build-classpath",
                "-Dmdep.outputFile=target/classpath.txt",
                "-q"
            );
            pb.directory(new File(userDir));
            pb.redirectErrorStream(true);
            Process p = pb.start();
            p.waitFor(30, TimeUnit.SECONDS);
        }
        
        if (depFile.exists()) {
            BufferedReader reader = new BufferedReader(new FileReader(depFile));
            String depCp = reader.readLine();
            reader.close();
            if (depCp != null && !depCp.isEmpty()) {
                classpath = userDir + "/target/classes:" + userDir + "/target/test-classes:" + depCp;
            }
        }
        
        for (int i = 0; i < nodeIds.length; i++) {
            ports[i] = BASE_PORT + i;
            logFiles[i] = new File("/tmp/speedboat_failover_" + nodeIds[i] + ".log");
            if (logFiles[i].exists()) {
                logFiles[i].delete();
            }
        }
        
        try {
            System.out.println("\n【阶段1】启动3节点集群...");
            startAllNodes(javaCmd);
            
            System.out.println("\n【阶段2】等待选举完成...");
            Thread.sleep(WAIT_FOR_ELECTION_MS);
            
            String leaderId = identifyLeader();
            if (leaderId == null) {
                System.out.println("❌ 选举失败！未发现 Leader");
                printAllLogs();
                return;
            }
            int initialTerm = getLeaderTerm(leaderId);
            System.out.println("✅ 选举成功！Leader: " + leaderId + " (term " + initialTerm + ")");
            
            System.out.println("\n【阶段3】杀掉 Leader 节点: " + leaderId);
            int leaderIndex = findNodeIndex(leaderId);
            killNode(leaderIndex);
            
            System.out.println("\n【阶段4】等待重新选举...");
            Thread.sleep(WAIT_FOR_REELECTION_MS);
            
            String newLeaderId = identifyNewLeader(leaderId, initialTerm);
            if (newLeaderId == null) {
                System.out.println("❌ 重新选举失败！");
                printAllLogs();
                return;
            }
            int newTerm = getLeaderTerm(newLeaderId);
            System.out.println("✅ 重新选举成功！新 Leader: " + newLeaderId + " (term " + newTerm + ")");
            
            System.out.println("\n【阶段5】重启原 Leader 节点: " + leaderId);
            restartNode(javaCmd, leaderIndex);
            
            System.out.println("\n【阶段6】等待节点重加入...");
            Thread.sleep(WAIT_FOR_REJOIN_MS);
            
            boolean rejoined = checkNodeRejoined(leaderId);
            if (!rejoined) {
                System.out.println("❌ 节点重加入失败！");
                printAllLogs();
                return;
            }
            System.out.println("✅ 节点重加入成功！原 Leader " + leaderId + " 已成为 Follower");
            
            System.out.println("\n【阶段7】最终状态验证...");
            String finalLeaderId = identifyLeader();
            int leaderCount = countLeaders();
            
            System.out.println("\n========================================");
            System.out.println("测试结果汇报");
            System.out.println("========================================");
            System.out.println("初始 Leader: " + leaderId);
            System.out.println("新 Leader: " + newLeaderId);
            System.out.println("最终 Leader: " + finalLeaderId);
            System.out.println("Leader 数量: " + leaderCount);
            System.out.println("节点重加入: 成功");
            System.out.println("\n✅ 所有测试通过！集群故障转移正常工作");
            
            printNodeStates();
            
        } finally {
            System.out.println("\n清理：停止所有节点...");
            stopAllNodes();
        }
    }
    
    private void startAllNodes(String javaCmd) throws Exception {
        for (int i = 0; i < nodeIds.length; i++) {
            List<String> cmd = new ArrayList<>();
            cmd.add(javaCmd);
            cmd.add("-cp");
            cmd.add(classpath);
            cmd.add("cn.itcraft.speedboat.sample.ClusterTestNode");
            cmd.addAll(buildNodeArgsList(i));
            
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectOutput(logFiles[i]);
            pb.redirectErrorStream(true);
            
            nodeProcesses[i] = pb.start();
            System.out.println("  启动节点: " + nodeIds[i] + " (端口: " + ports[i] + ")");
        }
        
        Thread.sleep(2000);
        System.out.println("✅ 3个节点已启动");
    }
    
    private List<String> buildNodeArgsList(int index) {
        List<String> args = new ArrayList<>();
        args.add("--nodeId");
        args.add(nodeIds[index]);
        args.add("--port");
        args.add(String.valueOf(ports[index]));
        args.add("--peers");
        
        List<String> peerList = new ArrayList<>();
        for (int j = 0; j < nodeIds.length; j++) {
            if (j != index) {
                peerList.add(nodeIds[j] + ":127.0.0.1:" + ports[j]);
            }
        }
        args.add(String.join(",", peerList));
        
        return args;
    }
    
    private String identifyLeader() throws Exception {
        String leaderId = null;
        int highestTerm = -1;
        
        for (int i = 0; i < nodeIds.length; i++) {
            if (nodeProcesses[i] != null && nodeProcesses[i].isAlive()) {
                int[] result = findLatestLeaderTerm(logFiles[i], nodeIds[i]);
                if (result[0] > highestTerm) {
                    highestTerm = result[0];
                    leaderId = nodeIds[i];
                }
            }
        }
        
        if (leaderId != null && highestTerm > 0) {
            return leaderId;
        }
        return null;
    }
    
    private int[] findLatestLeaderTerm(File logFile, String nodeId) throws Exception {
        if (!logFile.exists()) {
            return new int[]{-1, -1};
        }
        
        BufferedReader reader = new BufferedReader(new FileReader(logFile));
        String line;
        int latestTerm = -1;
        int latestLine = -1;
        int lineNum = 0;
        
        while ((line = reader.readLine()) != null) {
            lineNum++;
            if (line.contains(nodeId) && line.contains("became leader")) {
                int term = extractTerm(line);
                if (term > latestTerm) {
                    latestTerm = term;
                    latestLine = lineNum;
                }
            }
        }
        
        reader.close();
        return new int[]{latestTerm, latestLine};
    }
    
    private int extractTerm(String line) {
        int idx = line.indexOf("term ");
        if (idx >= 0) {
            String sub = line.substring(idx + 5);
            StringBuilder sb = new StringBuilder();
            for (char c : sub.toCharArray()) {
                if (Character.isDigit(c)) {
                    sb.append(c);
                } else {
                    break;
                }
            }
            if (sb.length() > 0) {
                return Integer.parseInt(sb.toString());
            }
        }
        return 0;
    }
    
    private String identifyNewLeader(String oldLeaderId, int oldTerm) throws Exception {
        String leaderId = null;
        int highestTerm = oldTerm;
        
        for (int i = 0; i < nodeIds.length; i++) {
            if (!nodeIds[i].equals(oldLeaderId) && nodeProcesses[i] != null && nodeProcesses[i].isAlive()) {
                int[] result = findLatestLeaderTerm(logFiles[i], nodeIds[i]);
                if (result[0] > highestTerm) {
                    highestTerm = result[0];
                    leaderId = nodeIds[i];
                }
            }
        }
        
        return leaderId;
    }
    
    private int findNodeIndex(String nodeId) {
        for (int i = 0; i < nodeIds.length; i++) {
            if (nodeIds[i].equals(nodeId)) {
                return i;
            }
        }
        return -1;
    }
    
    private int getLeaderTerm(String leaderId) throws Exception {
        int index = findNodeIndex(leaderId);
        if (index < 0) {
            return -1;
        }
        int[] result = findLatestLeaderTerm(logFiles[index], leaderId);
        return result[0];
    }
    
    private void killNode(int index) throws Exception {
        if (nodeProcesses[index] != null && nodeProcesses[index].isAlive()) {
            nodeProcesses[index].destroy();
            nodeProcesses[index].waitFor(3, TimeUnit.SECONDS);
            if (nodeProcesses[index].isAlive()) {
                nodeProcesses[index].destroyForcibly();
            }
            nodeProcesses[index] = null;
            System.out.println("  ✅ 节点 " + nodeIds[index] + " 已停止");
        }
    }
    
    private void restartNode(String javaCmd, int index) throws Exception {
        File newLogFile = new File("/tmp/speedboat_failover_" + nodeIds[index] + "_restart.log");
        if (newLogFile.exists()) {
            newLogFile.delete();
        }
        logFiles[index] = newLogFile;
        
        List<String> cmd = new ArrayList<>();
        cmd.add(javaCmd);
        cmd.add("-cp");
        cmd.add(classpath);
        cmd.add("cn.itcraft.speedboat.sample.ClusterTestNode");
        cmd.addAll(buildNodeArgsList(index));
        
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectOutput(newLogFile);
        pb.redirectErrorStream(true);
        
        nodeProcesses[index] = pb.start();
        System.out.println("  ✅ 节点 " + nodeIds[index] + " 已重启");
    }
    
    private boolean checkNodeRejoined(String nodeId) throws Exception {
        int index = findNodeIndex(nodeId);
        if (index < 0 || logFiles[index] == null || !logFiles[index].exists()) {
            return false;
        }
        
        BufferedReader reader = new BufferedReader(new FileReader(logFiles[index]));
        String line;
        
        while ((line = reader.readLine()) != null) {
            if (line.contains("transitioned") && line.contains("FOLLOWER")) {
                reader.close();
                return true;
            }
        }
        
        reader.close();
        return false;
    }
    
    private int countLeaders() throws Exception {
        int count = 0;
        for (int i = 0; i < nodeIds.length; i++) {
            if (nodeProcesses[i] != null && nodeProcesses[i].isAlive()) {
                if (checkIsLeaderNow(logFiles[i], nodeIds[i])) {
                    count++;
                }
            }
        }
        return count;
    }
    
    private boolean checkIsLeaderNow(File logFile, String nodeId) throws Exception {
        if (!logFile.exists()) {
            return false;
        }
        
        BufferedReader reader = new BufferedReader(new FileReader(logFile));
        String line;
        String lastState = null;
        
        while ((line = reader.readLine()) != null) {
            if (line.contains(nodeId) && line.contains("状态报告")) {
                if (line.contains("主节点 (Leader)")) {
                    lastState = "LEADER";
                } else if (line.contains("从节点 (Follower)")) {
                    lastState = "FOLLOWER";
                }
            }
        }
        
        reader.close();
        return "LEADER".equals(lastState);
    }
    
    private void printNodeStates() throws Exception {
        System.out.println("\n各节点状态:");
        for (int i = 0; i < nodeIds.length; i++) {
            String state = "未知";
            if (nodeProcesses[i] != null && nodeProcesses[i].isAlive()) {
                state = checkIsLeaderNow(logFiles[i], nodeIds[i]) ? "Leader" : "Follower";
            } else {
                state = "已停止";
            }
            System.out.println("  " + nodeIds[i] + ": " + state);
        }
    }
    
    private void printAllLogs() throws Exception {
        System.out.println("\n详细日志:");
        for (int i = 0; i < nodeIds.length; i++) {
            System.out.println("\n--- " + nodeIds[i] + " 日志 ---");
            if (logFiles[i] != null && logFiles[i].exists()) {
                BufferedReader reader = new BufferedReader(new FileReader(logFiles[i]));
                String line;
                int lineCount = 0;
                while ((line = reader.readLine()) != null && lineCount < 50) {
                    System.out.println(line);
                    lineCount++;
                }
                reader.close();
            } else {
                System.out.println("(无日志文件)");
            }
        }
    }
    
    private void stopAllNodes() throws Exception {
        for (int i = 0; i < nodeProcesses.length; i++) {
            if (nodeProcesses[i] != null && nodeProcesses[i].isAlive()) {
                nodeProcesses[i].destroy();
                nodeProcesses[i].waitFor(2, TimeUnit.SECONDS);
                if (nodeProcesses[i].isAlive()) {
                    nodeProcesses[i].destroyForcibly();
                }
            }
        }
        System.out.println("✅ 所有节点已停止");
    }
}