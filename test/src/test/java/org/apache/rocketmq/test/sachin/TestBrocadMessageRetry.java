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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author Sachin
 * @Date 2022/3/5
 *
 * 测试广播消息是否能够进行重试
 *
 *
 * 我们需要明确，只有当消费模式为 MessageModel.CLUSTERING(集群模式) 时，Broker 才会自动进行重试，对于广播消息是不会重试的。
 * 为什么广播消息不会消息重试
 *
 **/
public class TestBrocadMessageRetry {

    static AtomicInteger integer = new AtomicInteger();

    public static void main(String[] args) throws Exception {


       //send();
       //consume();
        sendClusterMsg();
        consumeClusterMsg();

    }

    public static void send() throws  Exception{
        DefaultMQProducer producer=new DefaultMQProducer("b-test");
//        producer.setNamesrvAddr("192.168.56.1:9876");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        Message msg = new Message("b-test-topic", "hello-2".getBytes(StandardCharsets.UTF_8));
        producer.setSendMsgTimeout(600000);
        producer.send(msg,600000);

        System.out.println("发送完成");
        //System.in.read();
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

                System.out.println("消息消费次数：" + integer.incrementAndGet());

                //返回消费状态
                //CONSUME_SUCCESS 消费成功
                //RECONSUME_LATER 消费失败，需要稍后重新消费
                throw new RuntimeException("消息消费失败");
              //return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;


            }
        });
        consumer.start();
        System.in.read();
    }


    /**
     * 发送集群消息
     * @throws Exception
     */
    public static void sendClusterMsg() throws  Exception{
        DefaultMQProducer producer=new DefaultMQProducer("b-test-cluster");
//        producer.setNamesrvAddr("192.168.56.1:9876");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        Message msg = new Message("b-test-topic-cluster", "hello-cluster-msg-2".getBytes(StandardCharsets.UTF_8));
        producer.setSendMsgTimeout(600000);
        producer.send(msg,600000);

        System.out.println("发送完成");
        //System.in.read();
    }


    public static void consumeClusterMsg() throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("b-c-test-cluster");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("b-test-topic-cluster", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt messageExt : msgs) {
                    String messageBody = new String(messageExt.getBody());
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
                            new Date())+"消费响应：msgId : " + messageExt.getMsgId() + ",  msgBody : " + messageBody);//输出消息内容
                }

                System.out.println("消息消费次数：" + integer.incrementAndGet());

                //返回消费状态
                //CONSUME_SUCCESS 消费成功
                //RECONSUME_LATER 消费失败，需要稍后重新消费
                throw new RuntimeException("消息消费失败");
                //return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;


            }
        });
        consumer.start();
        System.in.read();
    }
}
