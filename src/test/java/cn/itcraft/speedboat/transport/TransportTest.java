package cn.itcraft.speedboat.transport;

import cn.itcraft.speedboat.rpc.HeartbeatRequest;
import cn.itcraft.speedboat.rpc.HeartbeatResponse;
import cn.itcraft.speedboat.rpc.RequestVoteRequest;
import cn.itcraft.speedboat.rpc.RequestVoteResponse;
import cn.itcraft.speedboat.serialize.CustomSerializer;
import cn.itcraft.speedboat.serialize.ProtostuffSerializer;
import org.junit.jupiter.api.*;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class TransportTest {

    private CustomSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new CustomSerializer(new ProtostuffSerializer());
    }

    @Test
    void testNodeEndpointCreation() {
        NodeEndpoint endpoint = new NodeEndpoint("nodeA", "localhost", 9001);
        assertEquals("nodeA", endpoint.getNodeId());
        assertEquals("localhost", endpoint.getHost());
        assertEquals(9001, endpoint.getPort());
    }

    @Test
    void testNodeEndpointParse() {
        NodeEndpoint endpoint = NodeEndpoint.parse("node1:8080");
        assertEquals("node1", endpoint.getNodeId());
        assertEquals("node1", endpoint.getHost());
        assertEquals(8080, endpoint.getPort());
    }

    @Test
    void testRequestVoteHandler() throws Exception {
        int portA = 9101;
        int portB = 9102;
        
        NodeEndpoint endpointA = new NodeEndpoint("nodeA", "localhost", portA);
        NodeEndpoint endpointB = new NodeEndpoint("nodeB", "localhost", portB);

        NettyTransport transportB = new NettyTransport(endpointB, Arrays.asList(endpointA, endpointB), serializer);
        NettyTransport transportA = new NettyTransport(endpointA, Arrays.asList(endpointA, endpointB), serializer);

        try {
            AtomicReference<RequestVoteRequest> receivedRequest = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            transportB.setRequestVoteHandler(request -> {
                receivedRequest.set(request);
                latch.countDown();
                return CompletableFuture.completedFuture(new RequestVoteResponse(request.getTerm(), true));
            });

            transportB.start();
            Thread.sleep(200);
            transportA.start();
            Thread.sleep(500);

            RequestVoteRequest request = new RequestVoteRequest(1L, "nodeA", 1);
            CompletableFuture<RequestVoteResponse> future = transportA.sendRequestVote("nodeB", request);

            RequestVoteResponse response = future.get(3, TimeUnit.SECONDS);
            assertTrue(response.isVoteGranted());
            assertEquals(1L, response.getTerm());
        } finally {
            transportA.shutdown();
            transportB.shutdown();
        }
    }

    @Test
    void testHeartbeatHandler() throws Exception {
        int portA = 9103;
        int portB = 9104;
        
        NodeEndpoint endpointA = new NodeEndpoint("nodeA", "localhost", portA);
        NodeEndpoint endpointB = new NodeEndpoint("nodeB", "localhost", portB);

        NettyTransport transportB = new NettyTransport(endpointB, Arrays.asList(endpointA, endpointB), serializer);
        NettyTransport transportA = new NettyTransport(endpointA, Arrays.asList(endpointA, endpointB), serializer);

        try {
            AtomicReference<HeartbeatRequest> receivedRequest = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            transportB.setHeartbeatHandler(request -> {
                receivedRequest.set(request);
                latch.countDown();
                return CompletableFuture.completedFuture(new HeartbeatResponse(request.getTerm(), true));
            });

            transportB.start();
            Thread.sleep(200);
            transportA.start();
            Thread.sleep(500);

            HeartbeatRequest request = new HeartbeatRequest(1L, "nodeA");
            CompletableFuture<HeartbeatResponse> future = transportA.sendHeartbeat("nodeB", request);

            HeartbeatResponse response = future.get(3, TimeUnit.SECONDS);
            assertTrue(response.isSuccess());
            assertEquals(1L, response.getTerm());
        } finally {
            transportA.shutdown();
            transportB.shutdown();
        }
    }

    @Test
    void testBroadcastHeartbeat() {
        int portA = 9105;
        int portB = 9106;
        
        NodeEndpoint endpointA = new NodeEndpoint("nodeA", "localhost", portA);
        NodeEndpoint endpointB = new NodeEndpoint("nodeB", "localhost", portB);

        NettyTransport transportA = new NettyTransport(endpointA, Arrays.asList(endpointA, endpointB), serializer);

        try {
            transportA.start();
            
            HeartbeatRequest request = new HeartbeatRequest(1L, "nodeA");
            transportA.broadcastHeartbeat(request);
            
            assertTrue(true, "broadcastHeartbeat should not throw");
        } finally {
            transportA.shutdown();
        }
    }

    @Test
    void testSendToInactiveNode() throws Exception {
        int portA = 9107;
        int portB = 9108;
        
        NodeEndpoint endpointA = new NodeEndpoint("nodeA", "localhost", portA);
        NodeEndpoint endpointB = new NodeEndpoint("nodeB", "localhost", portB);

        NettyTransport transportA = new NettyTransport(endpointA, Arrays.asList(endpointA, endpointB), serializer);

        try {
            transportA.start();

            Thread.sleep(500);

            RequestVoteRequest request = new RequestVoteRequest(1L, "nodeA", 1);
            CompletableFuture<RequestVoteResponse> future = transportA.sendRequestVote("nodeB", request);

            RequestVoteResponse response = future.get(3, TimeUnit.SECONDS);
            assertFalse(response.isVoteGranted());
        } finally {
            transportA.shutdown();
        }
    }

    @Test
    void testSendHeartbeatToInactiveNode() throws Exception {
        int portA = 9109;
        int portB = 9110;
        
        NodeEndpoint endpointA = new NodeEndpoint("nodeA", "localhost", portA);
        NodeEndpoint endpointB = new NodeEndpoint("nodeB", "localhost", portB);

        NettyTransport transportA = new NettyTransport(endpointA, Arrays.asList(endpointA, endpointB), serializer);

        try {
            transportA.start();

            Thread.sleep(500);

            HeartbeatRequest request = new HeartbeatRequest(1L, "nodeA");
            CompletableFuture<HeartbeatResponse> future = transportA.sendHeartbeat("nodeB", request);

            HeartbeatResponse response = future.get(3, TimeUnit.SECONDS);
            assertFalse(response.isSuccess());
        } finally {
            transportA.shutdown();
        }
    }

    @Test
    void testBroadcastRequestVote() {
        int portA = 9111;
        int portB = 9112;
        
        NodeEndpoint endpointA = new NodeEndpoint("nodeA", "localhost", portA);
        NodeEndpoint endpointB = new NodeEndpoint("nodeB", "localhost", portB);

        NettyTransport transportA = new NettyTransport(endpointA, Arrays.asList(endpointA, endpointB), serializer);

        try {
            transportA.start();
            
            RequestVoteRequest request = new RequestVoteRequest(1L, "nodeA", 0);
            transportA.broadcastRequestVote(request);
            
            assertTrue(true, "broadcastRequestVote should not throw");
        } finally {
            transportA.shutdown();
        }
    }

    @Test
    void testRequestVoteHandlerNull() throws Exception {
        int portA = 9113;
        int portB = 9114;
        
        NodeEndpoint endpointA = new NodeEndpoint("nodeA", "localhost", portA);
        NodeEndpoint endpointB = new NodeEndpoint("nodeB", "localhost", portB);

        NettyTransport transportB = new NettyTransport(endpointB, Arrays.asList(endpointA, endpointB), serializer);
        NettyTransport transportA = new NettyTransport(endpointA, Arrays.asList(endpointA, endpointB), serializer);

        try {
            transportB.setRequestVoteHandler(null);
            transportB.start();
            Thread.sleep(200);
            transportA.start();
            Thread.sleep(500);

            RequestVoteRequest request = new RequestVoteRequest(1L, "nodeA", 1);
            CompletableFuture<RequestVoteResponse> future = transportA.sendRequestVote("nodeB", request);

            Thread.sleep(500);
        } finally {
            transportA.shutdown();
            transportB.shutdown();
        }
    }

    @Test
    void testHeartbeatHandlerNull() throws Exception {
        int portA = 9115;
        int portB = 9116;
        
        NodeEndpoint endpointA = new NodeEndpoint("nodeA", "localhost", portA);
        NodeEndpoint endpointB = new NodeEndpoint("nodeB", "localhost", portB);

        NettyTransport transportB = new NettyTransport(endpointB, Arrays.asList(endpointA, endpointB), serializer);
        NettyTransport transportA = new NettyTransport(endpointA, Arrays.asList(endpointA, endpointB), serializer);

        try {
            transportB.setHeartbeatHandler(null);
            transportB.start();
            Thread.sleep(200);
            transportA.start();
            Thread.sleep(500);

            HeartbeatRequest request = new HeartbeatRequest(1L, "nodeA");
            CompletableFuture<HeartbeatResponse> future = transportA.sendHeartbeat("nodeB", request);

            Thread.sleep(500);
        } finally {
            transportA.shutdown();
            transportB.shutdown();
        }
    }

    @Test
    void testGetRequestVoteHandler() {
        int port = 9117;
        NodeEndpoint endpoint = new NodeEndpoint("nodeA", "localhost", port);
        NettyTransport transport = new NettyTransport(endpoint, Arrays.asList(endpoint), serializer);
        
        try {
            assertNull(transport.getRequestVoteHandler());
            
            transport.setRequestVoteHandler(req -> CompletableFuture.completedFuture(new RequestVoteResponse(1, true)));
            assertNotNull(transport.getRequestVoteHandler());
        } finally {
            transport.shutdown();
        }
    }

    @Test
    void testGetHeartbeatHandler() {
        int port = 9118;
        NodeEndpoint endpoint = new NodeEndpoint("nodeA", "localhost", port);
        NettyTransport transport = new NettyTransport(endpoint, Arrays.asList(endpoint), serializer);
        
        try {
            assertNull(transport.getHeartbeatHandler());
            
            transport.setHeartbeatHandler(req -> CompletableFuture.completedFuture(new HeartbeatResponse(1, true)));
            assertNotNull(transport.getHeartbeatHandler());
        } finally {
            transport.shutdown();
        }
    }

    @Test
    void testSendToUnknownNode() throws Exception {
        int port = 9119;
        NodeEndpoint endpoint = new NodeEndpoint("nodeA", "localhost", port);
        NettyTransport transport = new NettyTransport(endpoint, Arrays.asList(endpoint), serializer);
        
        try {
            transport.start();
            Thread.sleep(200);
            
            RequestVoteRequest request = new RequestVoteRequest(1L, "nodeA", 1);
            CompletableFuture<RequestVoteResponse> future = transport.sendRequestVote("unknown-node", request);
            
            RequestVoteResponse response = future.get(3, TimeUnit.SECONDS);
            assertFalse(response.isVoteGranted());
        } finally {
            transport.shutdown();
        }
    }
}
