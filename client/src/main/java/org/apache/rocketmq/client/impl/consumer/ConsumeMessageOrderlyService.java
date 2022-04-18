/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    public void start() {
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ConsumeMessageOrderlyService.this.lockMQPeriodically();
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageOrderlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     *
     *  PullMessageService 负责对消息队列进行消息拉取，从远端服务器
     * 拉取消息后将消息存入 Proc巳 ssQueue 消息队列处理队列中，然后调用 Consum 巳Message Ser-
     * vice#submitConsumeRequest 方法进行消息消费，使用线程池来消费消息，确保了消息拉取
     * 与消息消费的解祸
     *
     *
     * @param msgs
     * @param processQueue
     * @param messageQueue
     * @param dispathToConsume
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispathToConsume) {
        if (dispathToConsume) {
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        }
    }

    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
        final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                /**
                 *
                 * 因此为了保证同一个消息队列MessageQueue中的消息被顺序消费，考虑 一个MessageQueue中的两个消息A,B先后被提交到消费者线程池中，
                 * 但是如果消费者线程池中有两个线程，则消息A先被线程1消费，消息B被线程2消费， 可能消息B已经执行完了 线程1 还没有消费完消息A，
                 * 也就说多线程下 我们可以保证 消息被顺序提交到消费者线程池中，但是因为 每个消息消费的时间不确定，因此无法保证消息消费完成的顺序。
                 * 从而要求线程池中只有一个线程才能保证顺序消费。
                 * 除了线程数设置为1这种方法 外 还可以 通过 对MessageQueue加锁的方式 来限制同时多个线程 消费你MessageQueue的消息。 R
                 * ocketMQ中 顺序消息的实现原理就是 通过对MessageQueue加锁。
                 *
                 * 在ConsumeRequest对象的run方法中需要禁止多个线程同时处理同一个MessageQueue中的消息,从而保证同一个MessageQueue中的消息被顺序消费
                 *
                 *
                 * 那顺序消费（重新分配队列，拉取消息，消费消息）时为什么要全局加锁呢？
                 *
                 * 新的consumer上线触发了rebalance（每隔30s重新负载均衡），在不加锁的情况下queue有可能被直接分配给别的consumer了，
                 * 而原来的老的consumer可能还没有触发rebalance，导致在某一时刻某个队列同时存在两个consumer。如果不加锁，在这一段时间内存在并发，可
                 * 能会出现重复消费，如消费者2消费到了第10条消息（当然已经消费了第6条消息），而消费者1才消费到第6条消息，形成可能的的乱序。
                 *
                 */
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    /**
                     *
                     *
                     */
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final long suspendTimeMillis
    ) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    public boolean processConsumeResult(
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeOrderlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        if (context.isAutoCommit()) {
            switch (status) {
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                        consumeRequest.getMessageQueue());
                case SUCCESS:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    } else {
                        commitOffset = consumeRequest.getProcessQueue().commit();
                    }
                    break;
                default:
                    break;
            }
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback();
                    this.submitConsumeRequestLater(
                        consumeRequest.getProcessQueue(),
                        consumeRequest.getMessageQueue(),
                        context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            /**
             * 消息消费成功提交位移， org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore#updateOffset(org.apache.rocketmq.common.message.MessageQueue, long, boolean)
             *
             * 需要注意的是 位移提交仅仅是保存在内存中，没有实时提交给Broker。当consumer关闭 或者RebalanceImpl重分配移除当前MessageQueue的时候才会将位移同步
             * 给Broker（updateConsumeOffsetToBroker），也就是说不是每消费一个消息就提交位移给broker，另外会有一个定时任务定期执行 persistAllConsumerOffset将位移同步给Broker
             *
             *
             * 我是不是可以这么认为， 由于Consumer消费之后 位移提交到本地内存，没有及时同步给Broker， 如果其他进程Consumer获取到了MessageQueue 然后从中拉取消息进行消费，这就会导致重复消费的问题。
             *
             * 如果当先消费者是顺序消费 ，对于当前消费者而言，重分配之后 对于那些需要不再属于当前Consumer的MessageQueue会执行RebalancePushImpl#removeUnnecessaryMessageQueue()，在remove方法中会先
             * 将本地位移同步给Broker updateConsumeOffsetToBroker ，然后再发送请求给Broker释放 MessageQueue上的锁。  那么也就是说其他进程的consumer 获得了这个MessageQueue ，想要从这个MessageQueue中拉取消息需要首先获取到这个MessageQueue的锁然后从
             * MessageQueue中拉取消息的时候 一定是在 当前consumer提交位移之后。 只有是顺序消费的consumer 才会 在拉取messageQueuez中的消息前发送给Broker消息对MessageQueue加锁，移除MessageQueue的时候发送给Broker消息释放锁。
             *
             * 对于非顺序消费的消费者， 一旦其他进程Consumer获得了 MessageQueue，那么就可以从这个MessageQueue中立刻拉取消息，这个时候可能原来的consumer 还没有 移除这个MessageQueue（移除MessageQueue的时候会提交位移给Broker）且还没有到达
             * 提交位移给Broker的时钟周期（消费端有定时任务定时提交位移给broker），那么这种情况下就会存在重复消费。
             *
             *
             */
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                    if (!sendMessageBack(msg)) {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                } else {
                    suspend = true;
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    /**
     * 将消息放置到重试队列
     * @param msg
     * @return
     */
    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }


    /**
     * RocketMQ中有两个ConsumeRequest类
     * org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest
     * org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService.ConsumeRequest
     *
     */
    class ConsumeRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        /**
         *
         * RocketMQ中有两个ConsumeRequest类
         *      * org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest
         *      * org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService.ConsumeRequest
         *
         *
         * 作者：中间件兴趣圈
         * 链接：https://www.zhihu.com/question/30195969/answer/1698410449
         * 来源：知乎
         * 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
         *
         * RokcetMQ的完成顺序性主要是由3把琐来实现的。
         * 下图是RocketMQ顺序消费的工作原理：<img src="https://pic3.zhimg.com/50/v2-e6b3e5fed942e358c3b5df77dddbe565_720w.jpg?source=1940ef5c"
         * data-caption="" data-size="normal" data-rawwidth="2117" data-rawheight="1190" data-default-watermark-src="https://pic2.zhimg.com/50/v2-4c7b4fd509716757f08aea062c1b6961_720w.jpg?source=1940ef5c"
         * class="origin_image zh-lightbox-thumb" width="2117" data-original="https://pic3.zhimg.com/v2-e6b3e5fed942e358c3b5df77dddbe565_r.jpg?source=1940ef5c"/>
         * 1、消费端在启动时首先会进行队列负载机制，遵循一个消费者可以分配多个队列，但一个队列只会被一个消费者消费的原则。
         * 2、消费者根据分配的队列，向 Broker 申请琐，如果申请到琐，则拉取消息，否则放弃消息拉取，等到下一个队列负载周期(20s)再试。
         * 3、拉取到消息后会在消费端的线程池中进行消费，但消费的时候，会对消费队列MessageQueue进行加锁，即同一个消费队列中的多条消息会串行执行。
         * 4、在消费的过程中，会对处理队列(ProccessQueue)进行加锁，保证处理中的消息消费完成，发生队列负载后，其他消费者才能继续消费。
         *
         * 前面2把琐比较好理解，最后一把琐有什么用呢？例如队列 q3 目前是分配给消费者C2进行消费，已将拉取了32条消息在线程池中处理，
         * 然后对消费者进行了扩容，分配给C2的q3队列，被分配给C3了，由于C2已将处理了一部分，位点信息还没有提交，如果C3立马去消费q3队列中的消息，
         * 那存在一部分数据会被重复消费，故在C2消费者在消费q3队列的时候，消息没有消费完成，那负载队列就不能丢弃该队列，就不会在broker端释放琐，
         * 其他消费者就无法从该队列消费，尽最大可能保证了消息的重复消费，保证顺序性语义。
         *
         *
         *
         * 要保证部分消息有序，需要发送端和消费端配合处理。 在发送端，要做到
         * 把同一业务 ID 的消息发送到同一个 Message Queue ；在消费过程中，要做到从
         * 同一个 Message Queue 读取的消息不被并发处理，这样才能达到部分有序 。《Rokcetmq实战与原理解析
         *
         *
         */
        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            /**
             * 获取MessageQueue的锁
             *
             * 因此为了保证同一个消息队列MessageQueue中的消息被顺序消费，考虑 一个MessageQueue中的两个消息A,B先后被提交到消费者线程池中，
             * 但是如果消费者线程池中有两个线程，则消息A先被线程1消费，消息B被线程2消费， 可能消息B已经执行完了 线程1 还没有消费完消息A，
             * 也就说多线程下 我们可以保证 消息被顺序提交到消费者线程池中，但是因为 每个消息消费的时间不确定，因此无法保证消息消费完成的顺序。
             * 从而要求线程池中只有一个线程才能保证顺序消费。
             * 除了线程数设置为1这种方法 外 还可以 通过 对MessageQueue加锁的方式 来限制同时多个线程 消费你MessageQueue的消息。 R
             * ocketMQ中 顺序消息的实现原理就是 通过对MessageQueue加锁。
             *
             * 之所以使用加锁的方式而不是限制消费者线程数量来实现 同一个消息队列中的消息顺序执行 是因为 不同的MessageQueue
             * 之间可以并发执行，因此线程池中可以有多个线程并发执行不同的的MessageQueue中的消息。
             *
             * 在ConsumeRequest对象的run方法中需要禁止多个线程同时处理同一个MessageQueue中的消息,从而保证同一个MessageQueue中的消息被顺序消费。
             *
             * 这个地方有个问题： 比如我现在的messageQueue 中有三个消息MsgA msgB ，MsgC，正确的消费顺序是a->b->c。  这三个消息被顺序提交到 消费者线程池的消费队列中。
             * 假设线程1 首先从队列中获取消息A，然后对MessageQueue加锁处理消息， 这个时候线程2 和线程3 先后从队列中获取消息B和消息C，但是因为他们都无法
             * 对MessageQueue加锁 从而导致 线程阻塞，显然这会影响消息的并发消费。其次 线程2 和线程3 都会被阻塞，那么当线程1 消费完释放messageQueue的锁之后 如何保证
             * 线程2 会比线程3 优先获取到MessageQueue的锁呢？  显然公平锁是比较好的选择，但是从下面的这个代码中我们看到却是使用了synchronized，
             * synchronized是非公平锁，那么 这到底是怎么回事？
             *
             *
             *
             */
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {
                /**
                 * 如果是广播模式，所有的consumer都需要消费该消息.
                 *
                 *
                 *
                 *
                 */
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                    || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    /**
                     * 顺序消息是如何保证消息重试的？
                     * 顺序消费ConsumeRequest是for死循环的方式一直消费，
                     * 退出条件是本次消费任务的运行事件大于最大时间，或者是处理消息执行结果时明确指出不再继续消费。对于第二种情况会在退出之前 往消费者线程池中提交一个消费任务ConsumeRequest。
                     * 顺序消费消息消费失败时，会将消息重新放入到ProcessQueue中时，再次重试次数为 Integer.MAX_VALUE，而且不延迟。
                     */
                    for (boolean continueConsume = true; continueConsume; ) {
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        /**
                         * 消费任务一次运行的最大时间。
                         */
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        final int consumeBatchSize =
                            ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

                        /**
                         * 这个地方有一个问题：  从ProcessQueue中 取出消息，消息消费的时候使用ProcessQueue的锁 锁定，从而禁止
                         * 向Broker释放当前的MessageQueue的lock。防止当前被消费的消息被其他Consumer进程重复消费。
                         * 问题是：如果当前线程 从ProcessQueue中取出了消息，但是尚未获取到ProcessQueue的锁，那么这个时候当前consumer释放
                         * MessageQueue的lock。那么ProcessQueue中的消息以及从ProcessQueue中刚取出来的还未交给listener消费的消息 还会被继续消费处理吗？
                         *  (1)对于那些刚从broker中取出来 还未交给listener执行的消息  会继续 获取ProcessQueue的锁，但是在获取到ProcessQueue锁之后会判断this.processQueue.isDropped()
                         *  如果dropped则退出不再交给listener消费。因此这些消息不会交给listener消费
                         *  （2）对于ProcessQueue中剩下的那些尚未来得及取出的消息， 线程池中可能还存在其他ConsumeRequest，这些ConsumeRequest执行的
                         *  时候run方法中会判断（this.processQueue.isLocked() && !this.processQueue.isLockExpired()） 也就是上面的if。 如果发现MessageQueue的锁已经被释放了那么就不会继续执行。
                         *
                         *
                         */
                        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
                        if (!msgs.isEmpty()) {
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);

                            ConsumeOrderlyStatus status = null;

                            ConsumeMessageContext consumeMessageContext = null;
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext = new ConsumeMessageContext();
                                consumeMessageContext
                                    .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
                                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                                consumeMessageContext.setMq(messageQueue);
                                consumeMessageContext.setMsgList(msgs);
                                consumeMessageContext.setSuccess(false);
                                // init the consume context type
                                consumeMessageContext.setProps(new HashMap<String, String>());
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }

                            long beginTimestamp = System.currentTimeMillis();
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            boolean hasException = false;
                            try {
                                /**
                                 * 对ProcessQueue进行锁定
                                 * 在消费的过程中，会对处理队列(ProccessQueue)进行加锁，保证处理中的消息消费完成，发生队列负载后，其他消费者才能继续消费。前面2把琐比较好理解，
                                 * 最后一把琐有什么用呢？例如队列 q3 目前是分配给消费者C2进行消费，已将拉取了32条消息在线程池中处理，然后对消费者进行了扩容，分配给C2的q3队列，
                                 * 被分配给C3了，由于C2已将处理了一部分，位点信息还没有提交，如果C3立马去消费q3队列中的消息，那存在一部分数据会被重复消费，
                                 * 故在C2消费者在消费q3队列的时候，消息没有消费完成，那负载队列就不能丢弃该队列，就不会在broker端释放琐，其他消费者就无法从该队列消费，尽最大可能保证了消息的重复消费，保证顺序性语义。
                                 *
                                 * 作者：中间件兴趣圈
                                 * 链接：https://www.zhihu.com/question/30195969/answer/1698410449
                                 * 来源：知乎
                                 * 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
                                 *
                                 * 上面这段话是如何体现的呢？  就是说 因为RebalanceImpl重分配的过程中 会判断当前consumer是否是顺序消费，
                                 * 如果是顺序消费，那么必须 对consumer重分配后的每一个MessageQueue 发送请求给这个MessageQueue的broker锁定该MessageQueue。
                                 * 再次重分配后若当前Consumer 在重分配后丢失了某一个MessageQueue，那么当前Consumer需要发送请求给Broker解除对该MessageQueue的锁定。 但是这个
                                 * MessageQueue的ProcessQueue可能还尚未处理完消息，这里尚未处理完消息是指有些消息正在被处理中，ProcessQueue 中的消息会有两种状态（1）尚未被处理（2）正在被处理
                                 * 但是尚未处理完。 这里主要考虑是第二种情况。  如果马上解除对MessageQueue的锁定，那么可能会导致 其他Consumer 重新获取到了ProcessQueue中的消息。 对于当前Consumer进程
                                 * 的ProcessQueue中那些尚未开始处理的消息而言 还好，但是对于那些正在被处理的消息 就可能 被新的Consumer重新消费。 为了解决这个问题，要求顺序消息在消费时（ConsumeMessageOrderlyService
                                 * 的ConsumeRequest的run） 首先  对ProcessQueue进行加锁，表示当前有消息正在处理中。 然后当前Consumer在  向Broker发起 解除 MessageQueue的锁请求的之前
                                 * 会先通过MessageQueue的ProcessQueue尝试获取锁， 如果获取锁成功，则意味着ProcessQueue中的消息当前没有被处理中，可以向Broker发送解除messagequeue的请求。
                                 *如果发现ProcessQueue已经被锁定，则意味着ProcessQueue中有消息正在被处理。 因此不能向Broker发送请求解除对messageQueue的锁。
                                 * 具体在 org.apache.rocketmq.client.impl.consumer.RebalancePushImpl#removeUnnecessaryMessageQueue()
                                 *
                                 *  从上面的分析我们看到 对ProcessQueue加锁 主要是 表示ProcessQueue中当前有消息正在被处理，如果不加锁直接 向Broker发送请求解除MessageQueue，那么新的Consumer就有可能
                                 *  重新拉取到这个当前正在被处理的消息从而造成重复消费。 从理论上分析， 消息处理前对ProcessQueue进行加锁， 那么应该在消息处理后 且 完成提交位移 之后在释放ProcessQueue的锁。
                                 *  这样能保证新的consumer不会重复消费这个消息。 但是在源码中 如下 我们看到 ProcessQueue的释放锁 是在 listener消费完成之后 提交位移之前就释放了。这不会存在问题吗？
                                 *
                                 *
                                 * ==========
                                 *
                                 延伸问题：  在上面  我们分析了 三把锁，其中 第二把锁 synchronized(messageQueue)主要是用来限制
                                 *多个线程同时消费messageQueue中的消息 第三把锁ProcessQueue主要是尽最大努力避免重复消费。
                                 * 因为ProcessQueue 会在 MessageQueue重分配 后释放锁的时候检查其状态。 ProcessQueue的lock实现
                                 * 是ReentrantLock，且是公平锁， synchronized(MessageQueue)也是公平锁，而且MessageQueue
                                 * 和ProcessQueue存在一一对应的关系，那么可以不可以省略synchronized(messsageQueue)而只用ProcessQueue 进行lock呢？
                                 * 答案是不可以： 因为生命周期不同，每次RebalanceImpl 都会创建ProcessQueue，RebalanceImpl#updateProcessQueueTableInRebalance()
                                 * 但是MessageQueue并不是。
                                 *
                                 *
                                 */
                                this.processQueue.getLockConsume().lock();
                                /**
                                 *
                                 */
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                        this.messageQueue);
                                    break;
                                }

                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                    RemotingHelper.exceptionSimpleDesc(e),
                                    ConsumeMessageOrderlyService.this.consumerGroup,
                                    msgs,
                                    messageQueue);
                                hasException = true;
                            } finally {
                                this.processQueue.getLockConsume().unlock();
                            }

                            if (null == status
                                || ConsumeOrderlyStatus.ROLLBACK == status
                                || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                    ConsumeMessageOrderlyService.this.consumerGroup,
                                    msgs,
                                    messageQueue);
                            }

                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            if (null == status) {
                                if (hasException) {
                                    returnType = ConsumeReturnType.EXCEPTION;
                                } else {
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;
                            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                returnType = ConsumeReturnType.FAILED;
                            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                                returnType = ConsumeReturnType.SUCCESS;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }

                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext
                                    .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                            }

                            ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                                .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

                            /**
                             * 因为消息是通过processQueue.takeMessages(consumeBatchSize); 从TreeMap中取出来的，消费失败后为了保证下次消费仍然是这个消息，需要将消息放回
                             * 如果消费失败了，但是为了保证顺序性，会把这条消息从 consumingMsgOrderlyTreeMap 取出，重新放入 msgTreeMap 中，当超过了最大重试次数后，尝试发回 broker
                             *
                             */
                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
                        } else {
                            continueConsume = false;
                        }
                    }
                } else { //集群模式消费
                    /**
                     个地方有一个问题：  从ProcessQueue中 取出消息，消息消费的时候使用ProcessQueue的锁 锁定，从而禁止
                     Broker释放当前的MessageQueue的lock。防止当前被消费的消息被其他Consumer进程重复消费。
                     题是：如果当前线程 从ProcessQueue中取出了消息，但是尚未获取到ProcessQueue的锁，那么这个时候当前consumer释放
                     essageQueue的lock。那么ProcessQueue中的消息以及从ProcessQueue中刚取出来的还未交给listener消费的消息 还会被继续消费处理吗？
                     (1)对于那些刚从broker中取出来 还未交给listener执行的消息  会继续 获取ProcessQueue的锁，但是在获取到ProcessQueue锁之后会判断this.processQueue.isDropped()
                     如果dropped则退出不再交给listener消费。因此这些消息不会交给listener消费
                     （2）对于ProcessQueue中剩下的那些尚未来得及取出的消息， 线程池中可能还存在其他ConsumeRequest，这些ConsumeRequest执行的
                     时候run方法中会判断（this.processQueue.isLocked() && !this.processQueue.isLockExpired()） 也就是上面的if。 如果发现MessageQueue的锁已经被释放了那么就不会继续执行。
                     *
                     *
                     */
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

}
