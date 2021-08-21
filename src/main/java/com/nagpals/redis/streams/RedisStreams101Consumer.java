package com.nagpals.redis.streams;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.List;

public class RedisStreams101Consumer {

    public final static String STREAMS_KEY = "weather_sensor:wind";
    public final static String GROUP_NAME = "xreadGroup-group";
    public final static String CONSUMER_NAME = "xreadConsumer-java";

    public static void main(String[] args) {

        RedisURI redisURI = RedisURI.Builder
                .sentinel("127.0.0.1", 26379, "str-redis")
                .withSentinel("127.0.0.1", 26380)
                .withSentinel("127.0.0.1", 26381)
                .build();
        RedisClient redisClient = RedisClient.create(redisURI);

        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();

        try {
            syncCommands.xgroupCreate( XReadArgs.StreamOffset.from(STREAMS_KEY, "0-0"), CONSUMER_NAME  );
        }
        catch (RedisBusyException redisBusyException) {
            System.out.println( String.format("\t Group '%s' already exists", CONSUMER_NAME));
        }


        System.out.println("Waiting for new messages");

        while(true) {

            List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                    Consumer.from(GROUP_NAME, CONSUMER_NAME),
                    XReadArgs.StreamOffset.lastConsumed(STREAMS_KEY)
            );

            if (!messages.isEmpty()) {
                for (StreamMessage<String, String> message : messages) {
                    System.out.println(message);
                    // Confirm that the message has been processed using XACK
                    syncCommands.xack(STREAMS_KEY, GROUP_NAME,  message.getId());
                }
            }


        }

    }

}
