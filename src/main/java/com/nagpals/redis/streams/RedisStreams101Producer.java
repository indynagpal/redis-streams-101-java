package com.nagpals.redis.streams;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.HashMap;
import java.util.Map;

public class RedisStreams101Producer {

    public final static String STREAMS_KEY = "weather_sensor:wind";

    public static void main(String[] args) {
        /* if we pass in commandline argument to send multiple messages, use that. else send 1 message */
        int iMessages = 1;
        if (args != null && args.length != 0 ) {
            iMessages = Integer.valueOf(args[0]);
        }

        System.out.println( String.format("\n Sending %s message(s)", iMessages));

        /* build a URI that is then used for getting a connection. you'll have to pass in your sentinel ips and ports, along with the masterid */
        RedisURI redisURI = RedisURI.Builder
                .sentinel("127.0.0.1", 26379, "str-redis")
                .withSentinel("127.0.0.1", 26380)
                .withSentinel("127.0.0.1", 26381)
                .build();
        RedisClient redisClient = RedisClient.create(redisURI);

        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();

        for (int i = 0 ; i < iMessages ; i++) {

            Map<String, String> messageBody = new HashMap<>();
            messageBody.put("speed", String.valueOf(getRandomNumber(0,60)));
            messageBody.put("direction", String.valueOf(getRandomNumber(0,359)));
            messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));
            messageBody.put("loop_info", String.valueOf( i ));

            String messageId = syncCommands.xadd(
                    STREAMS_KEY,
                    messageBody);

            System.out.println(String.format("\tMessage %s : %s posted", messageId, messageBody));
        }

        System.out.println("\n");

        connection.close();
        redisClient.shutdown();

    }

    /* ===================================================================== */

    private static int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    /* ===================================================================== */

}
