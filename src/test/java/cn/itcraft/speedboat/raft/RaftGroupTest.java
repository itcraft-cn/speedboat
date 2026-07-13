package cn.itcraft.speedboat.raft;

import cn.itcraft.speedboat.config.SpeedboatConfig;
import cn.itcraft.speedboat.strategy.group.DefaultGroupStrategy;
import cn.itcraft.speedboat.strategy.group.GroupStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftGroupTest {

    private SpeedboatConfig config;

    @BeforeEach
    void setUp() {
        config = SpeedboatConfig.builder()
            .port(8080)
            .datacenterId("dc-1")
            .groupStrategy(new DefaultGroupStrategy())
            .build();
    }

    @Test
    @DisplayName("创建单层Raft组")
    void testCreateSingleLayerGroup() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080", "node-3:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        assertEquals("group-1", group.getGroupId());
        assertEquals(3, group.getNodes().size());
        assertNull(group.getParentGroup());
        assertTrue(group.getChildGroups().isEmpty());
        assertFalse(group.isRunning());
    }

    @Test
    @DisplayName("启动和关闭组")
    void testStartAndShutdown() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080", "node-3:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        group.start();
        assertTrue(group.isRunning());
        
        for (RaftNode node : group.getNodes()) {
            assertEquals(NodeState.FOLLOWER, node.getCurrentState());
        }
        
        group.shutdown();
        assertFalse(group.isRunning());
    }

    @Test
    @DisplayName("设置父子组关系")
    void testSetParentChildRelationship() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup parentGroup = new RaftGroup("parent-group", nodeUrls, config);
        RaftGroup childGroup = new RaftGroup("child-group", nodeUrls, config);
        
        childGroup.setParentGroup(parentGroup);
        
        assertEquals(parentGroup, childGroup.getParentGroup());
        assertTrue(parentGroup.getChildGroups().contains(childGroup));
        assertEquals(1, parentGroup.getChildGroups().size());
    }

    @Test
    @DisplayName("添加多个子组")
    void testAddMultipleChildGroups() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup parentGroup = new RaftGroup("parent-group", nodeUrls, config);
        RaftGroup childGroup1 = new RaftGroup("child-group-1", nodeUrls, config);
        RaftGroup childGroup2 = new RaftGroup("child-group-2", nodeUrls, config);
        
        parentGroup.addChildGroup(childGroup1);
        parentGroup.addChildGroup(childGroup2);
        
        assertEquals(2, parentGroup.getChildGroups().size());
        assertTrue(parentGroup.getChildGroups().contains(childGroup1));
        assertTrue(parentGroup.getChildGroups().contains(childGroup2));
        assertEquals(parentGroup, childGroup1.getParentGroup());
        assertEquals(parentGroup, childGroup2.getParentGroup());
    }

    @Test
    @DisplayName("获取组内节点")
    void testGetNodes() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080", "node-3:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        List<RaftNode> nodes = group.getNodes();
        assertEquals(3, nodes.size());
        
        assertEquals("node-1", nodes.get(0).getNodeId());
        assertEquals("node-2", nodes.get(1).getNodeId());
        assertEquals("node-3", nodes.get(2).getNodeId());
    }

    @Test
    @DisplayName("获取当前主节点")
    void testGetLeader() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080", "node-3:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        assertNull(group.getLeader());
        
        RaftNode leaderNode = group.getNodes().get(0);
        leaderNode.transitionTo(NodeState.CANDIDATE);
        leaderNode.becomeLeader();
        
        group.electLeader();
        
        assertNotNull(group.getLeader());
    }

    @Test
    @DisplayName("判断当前节点是否为主")
    void testIsMain() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        assertFalse(group.isMain());
    }

    @Test
    @DisplayName("子组健康检查")
    void testCheckChildGroupHealth() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup parentGroup = new RaftGroup("parent-group", nodeUrls, config);
        RaftGroup childGroup1 = new RaftGroup("child-group-1", nodeUrls, config);
        RaftGroup childGroup2 = new RaftGroup("child-group-2", nodeUrls, config);
        
        parentGroup.addChildGroup(childGroup1);
        parentGroup.addChildGroup(childGroup2);
        
        childGroup1.start();
        childGroup2.start();
        
        assertTrue(parentGroup.checkChildGroupHealth());
        
        childGroup1.shutdown();
        
        assertFalse(parentGroup.checkChildGroupHealth());
        
        childGroup2.shutdown();
    }

    @Test
    @DisplayName("创建空节点列表的组")
    void testCreateGroupWithEmptyNodeList() {
        RaftGroup group = new RaftGroup("group-1", Collections.emptyList(), config);
        
        assertEquals("group-1", group.getGroupId());
        assertTrue(group.getNodes().isEmpty());
    }

    @Test
    @DisplayName("组策略配置")
    void testGroupStrategy() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        GroupStrategy customStrategy = new DefaultGroupStrategy();
        SpeedboatConfig customConfig = SpeedboatConfig.builder()
            .port(8080)
            .groupStrategy(customStrategy)
            .build();
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, customConfig);
        
        assertNotNull(group.getGroupStrategy());
    }

    @Test
    @DisplayName("重复启动组应无效果")
    void testStartTwice() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        group.start();
        assertTrue(group.isRunning());
        
        group.start();
        assertTrue(group.isRunning());
        
        group.shutdown();
    }

    @Test
    @DisplayName("重复关闭组应无效果")
    void testShutdownTwice() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        group.start();
        group.shutdown();
        assertFalse(group.isRunning());
        
        group.shutdown();
        assertFalse(group.isRunning());
    }

    @Test
    @DisplayName("级联架构 - 三层结构")
    void testThreeLayerCascadingArchitecture() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup rootGroup = new RaftGroup("root-group", nodeUrls, config);
        RaftGroup middleGroup = new RaftGroup("middle-group", nodeUrls, config);
        RaftGroup leafGroup = new RaftGroup("leaf-group", nodeUrls, config);
        
        middleGroup.addChildGroup(leafGroup);
        rootGroup.addChildGroup(middleGroup);
        
        assertNull(rootGroup.getParentGroup());
        assertEquals(rootGroup, middleGroup.getParentGroup());
        assertEquals(middleGroup, leafGroup.getParentGroup());
        
        assertTrue(rootGroup.getChildGroups().contains(middleGroup));
        assertTrue(middleGroup.getChildGroups().contains(leafGroup));
        assertTrue(leafGroup.getChildGroups().isEmpty());
    }

    @Test
    @DisplayName("创建组 - null nodeUrls")
    void testCreateGroupWithNullNodeUrls() {
        RaftGroup group = new RaftGroup("group-1", null, config);
        
        assertEquals("group-1", group.getGroupId());
        assertTrue(group.getNodes().isEmpty());
    }

    @Test
    @DisplayName("setParentGroup - null 参数")
    void testSetParentGroupNull() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        group.setParentGroup(null);
        
        assertNull(group.getParentGroup());
    }

    @Test
    @DisplayName("addChildGroup - null 参数")
    void testAddChildGroupNull() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        group.addChildGroup(null);
        
        assertTrue(group.getChildGroups().isEmpty());
    }

    @Test
    @DisplayName("addChildGroup - 重复添加相同子组")
    void testAddChildGroupTwice() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup parentGroup = new RaftGroup("parent-group", nodeUrls, config);
        RaftGroup childGroup = new RaftGroup("child-group", nodeUrls, config);
        
        parentGroup.addChildGroup(childGroup);
        parentGroup.addChildGroup(childGroup);
        
        assertEquals(1, parentGroup.getChildGroups().size());
    }

    @Test
    @DisplayName("构造函数 - null groupId 应抛出异常")
    void testConstructorWithNullGroupId() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        assertThrows(NullPointerException.class, () -> {
            new RaftGroup(null, nodeUrls, config);
        });
    }

    @Test
    @DisplayName("构造函数 - null config 应抛出异常")
    void testConstructorWithNullConfig() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        assertThrows(NullPointerException.class, () -> {
            new RaftGroup("group-1", nodeUrls, null);
        });
    }

    @Test
    @DisplayName("checkChildGroupHealth - 无子组返回 true")
    void testCheckChildGroupHealthNoChildren() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        
        assertTrue(group.checkChildGroupHealth());
    }

    @Test
    @DisplayName("notifyParentLeaderElected - 无父组不做任何操作")
    void testNotifyParentLeaderElectedNoParent() {
        List<String> nodeUrls = Arrays.asList("node-1:8080", "node-2:8080");
        
        RaftGroup group = new RaftGroup("group-1", nodeUrls, config);
        group.start();
        
        RaftNode leaderNode = group.getNodes().get(0);
        
        assertDoesNotThrow(() -> group.notifyParentLeaderElected(leaderNode));
        
        group.shutdown();
    }
}
