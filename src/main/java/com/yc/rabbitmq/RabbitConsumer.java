package com.yc.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Chuan Yu
 * @date 2021/3/4 下午4:55
 */
public class RabbitConsumer {

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        //Declare DLQ
        channel.exchangeDeclare("dlx.exchange", "topic", true, false, null);
        channel.queueDeclare("dlx.queue", true, false, false, null);
        //可以匹配任意routeKey
        channel.queueBind("dlx.queue", "dlx.exchange", "#");


        channel.exchangeDeclare("test.direct", BuiltinExchangeType.DIRECT, true, false, null);

        Map<String, Object> arguments = new HashMap<>();
        // 设置队列超时时间为10秒
        arguments.put("x-message-ttl", 30000);
        //Add DLQ exchange
        arguments.put("x-dead-letter-exchange", "dlx.exchange");

        channel.queueDeclare("test.direct.queue1", true, false, false, arguments);
        channel.queueBind("test.direct.queue1", "test.direct", "test.direct.key");

        //prefetch count = 5
        channel.basicQos(5);

        //pull
        GetResponse response = channel.basicGet("test.direct.queue", false);
        System.out.println(new String(response.getBody()));
        channel.basicAck(response.getEnvelope().getDeliveryTag(), false);

        //push
        channel.basicConsume("test.direct.queue1", true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println(consumerTag + "\n" +
                        envelope + "\n" +
                        properties + "\n" +
                        new String(body));
            }
        });
    }
}
