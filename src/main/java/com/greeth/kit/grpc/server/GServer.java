package com.greeth.kit.grpc.server;

import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.greeth.kit.grpc.signal.EventSignalPublisherGrpc;
import com.greeth.kit.grpc.signal.EventSignalRequest;
import com.greeth.kit.grpc.signal.EventSignalResponse;
import com.greeth.kit.marshalling.DataFormatMarshalling;
import com.greeth.kit.marshalling.MarshallingException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import com.amazonaws.services.lambda.AWSLambda;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class GServer {
    private static final Logger logger = Logger.getLogger(GServer.class.getName());
    private Map<String, String> topicToLambdaMap;
    private Server server;
    private static LinkedBlockingQueue<EventSignalRequest> eventSignalRequestQueue = new LinkedBlockingQueue<>();


    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new EventSignalPublisher(getTopicToLambdaMap()))
                .build()
                .start();
//        ExecutorService executor = Executors.newFixedThreadPool(3); //   newSingleThreadExecutor();
//        executor.submit(new EventSignalProcessor(getTopicToLambdaMap(), eventSignalRequestQueue));
//        executor.submit(() -> System.out.println("I am GJ"));
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                GServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if( args.length != 1 ) {
            System.out.println( "Usage: java -jar  falcon-server-1.0.jar <file.yml>" );
            return;
        }
        final GServer server = new GServer();
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try( InputStream in = Files.newInputStream( Paths.get( args[ 0 ] ) ) ) {
            server.setTopicToLambdaMap(mapper.readValue(in, HashMap.class));
        } catch (IOException e) {
            e.printStackTrace();
        }
        server.start();
        server.blockUntilShutdown();
    }

    static class EventSignalPublisher extends EventSignalPublisherGrpc.EventSignalPublisherImplBase {
        Map<String, String> m;
        public EventSignalPublisher(Map<String, String> topicM) {
            m=topicM;
        }

        @Override
        public void publish(EventSignalRequest request, StreamObserver<EventSignalResponse> responseObserver) {
            EventSignalResponse eventSignalResponse = EventSignalResponse.newBuilder().setMessage("received message for topic: " + request.getTopicName()).build();
//            eventSignalRequestQueue.add(request);
            invokeLambda(request, m.get(request.getTopicName()));
            responseObserver.onNext(eventSignalResponse);
            responseObserver.onCompleted();
        }

        private String invokeLambda(EventSignalRequest request, String lambdaName) {
//            AWSLambda lambdaClient = AWSLambdaClientBuilder.defaultClient();
//            InvokeRequest invokeRequest = null;
//            DataFormatMarshalling<EventSignalRequest> dataFormatMarshalling = new DataFormatMarshalling();
//            try {
//                invokeRequest = new InvokeRequest().withFunctionName(lambdaName).withLogType("Tail").withPayload(dataFormatMarshalling.toJsonString(request));
//                invokeRequest.setInvocationType(InvocationType.Event);
//            } catch (MarshallingException e) {
//                e.printStackTrace();
//            }
//            logger.info("Received event for Topic invoke lambda: " + lambdaName);
//            System.out.println("Received event for Topic invoke lambda: " + lambdaName);
//            ByteBuffer payload = lambdaClient.invoke(invokeRequest).getPayload();
//            return new String(payload.array(), Charset.forName("UTF-8"));

              logger.info("Received event for Topic invoke lambda: " + lambdaName);
              return  lambdaName;
        }

    }

    public Map<String, String> getTopicToLambdaMap() {
        return topicToLambdaMap;
    }

    public void setTopicToLambdaMap(Map<String, String> topicToLambdaMap) {
        this.topicToLambdaMap = topicToLambdaMap;
    }
}
