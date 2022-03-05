package org.apache.rocketmq.test.sachin;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @Author Sachin
 * @Date 2022/3/5
 *
 * 测试广播消息是否能够进行重试
 **/
public class TestBrocadMessageRetry {


    public static void main(String[] args) throws Exception {


        send();

    }

    public static void send() throws  Exception{
        DefaultMQProducer producer=new DefaultMQProducer("b-test");
//        producer.setNamesrvAddr("192.168.56.1:9876");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        Message msg = new Message("b-test-topic", "hello".getBytes(StandardCharsets.UTF_8));
        producer.setSendMsgTimeout(600000);
        producer.send(msg);

        System.out.println("发送完成");
        System.in.read();
    }

    public static void consume() throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("b-c-test");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("b-test-topic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt messageExt : msgs) {
                    String messageBody = new String(messageExt.getBody());
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
                            new Date())+"消费响应：msgId : " + messageExt.getMsgId() + ",  msgBody : " + messageBody);//输出消息内容
                }

                //返回消费状态
                //CONSUME_SUCCESS 消费成功
                //RECONSUME_LATER 消费失败，需要稍后重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

            }
        });
        consumer.start();
    }
}
