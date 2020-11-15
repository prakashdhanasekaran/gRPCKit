package com.greeth.kit.grpc.client;

import com.greeth.kit.grpc.signal.EventSignalPublisherGrpc;
import com.greeth.kit.grpc.signal.EventSignalRequest;
import com.greeth.kit.grpc.signal.EventSignalResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GClient {
    private static final Logger logger = Logger.getLogger(GClient.class.getName());
    private final ManagedChannel channel;
    private final EventSignalPublisherGrpc.EventSignalPublisherBlockingStub blockingStub;

    public GClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    public GClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = EventSignalPublisherGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void publishEvent(String topicName) {
        EventSignalRequest eventSignalRequest = EventSignalRequest.newBuilder().setTopicName(topicName).build();
        EventSignalResponse eventSignalResponse;
        try {
            eventSignalResponse = blockingStub.publish(eventSignalRequest);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Response from GServer " + eventSignalResponse.getMessage());
    }

    public static void main(String[] args) {
        GClient gClient = new GClient(args[0], 50051);
        gClient.publishEvent(args[1]);
    }
}
