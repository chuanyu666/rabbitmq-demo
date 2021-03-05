package com.yc.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * @author Chuan Yu
 * @date 2021/3/4 下午3:41
 */
public class RabbitProducer {

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare("test.direct", BuiltinExchangeType.DIRECT, true, false, null);
            channel.queueDeclare("test.direct.queue", true, false, false, null);
            channel.queueBind("test.direct.queue", "test.direct", "test.direct.key");

            channel.queueDeclare("test.direct.queue1", true, false, false, null);
            channel.queueBind("test.direct.queue1", "test.direct", "test.direct.key");

            //enable confirm callback
            channel.confirmSelect();

            channel.addConfirmListener(new ConfirmListener() {
                //mq broker might do single msg ack or multiple msg ack, it depends on mq broker
                public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println(deliveryTag + "----ack------" + multiple);
                }

                public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                    System.out.println(deliveryTag + "----nack------" + multiple);
                }
            });
            channel.addReturnListener((replyCode, replyText, exchange, routingKey, properties, body) -> System.out.println(replyCode + "\n" + replyText + "\n" + exchange + "\n" + routingKey + "\n" + properties + "\n" + new String(body)));

            AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                                                            .deliveryMode(2)  //persist msg
                                                            .expiration("10000") //消息过期时间
                                                            .build();

            //if mandatory = true and msg is not able to be routed to queue, then call return callback
            channel.basicPublish("test.direct", "test.direct.key", true, properties, "asd".getBytes());

            //Wait until all messages published since the last call have been either ack'd or nack'd by the broker
            //channel.waitForConfirms();
            Thread.sleep(5000);
        }
    }
}
