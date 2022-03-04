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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/***
 *
 * Consumer 消费消息的过程
 *
 *
 *
 从Broker拉取到的消息如何交给Consumer处理？消费者如何做流量控制，消费者消息堆积的时候就不拉取数据

 一个Topic可以有多个MessageQueue，MessageQueue是Broker端的数据结构，用来存放生产者发送给Broker的消息，可以成为消息队列或者消息消费队列。
 ProcessQueue是消费端的数据结构，叫做消息处理队列，消费者并不从这个对象取出数据进行消费。
 每一个MessageQueue都有一个对应的ProcessQueue。
 ProcessQueue的作用是为了做流量控制。


 Consumer创建的时候会创建一个线程池，同时这个线程池指定了使用Consumer对象中的consumeRequestQueue队列。
 消息消费的过程是，PullMessageService线程执行消息拉取会通过pullMessage方法
 org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage
 在PullMessage方法中 对拉取到的消息 会放入到ProcessQueue 消息处理队列中。
 同时会将消息封装一个ConsumeRequest，并将这个ConsumeRequest提交到消费者的消费线程池中处理。因此我们说消息的拉取和消息的处理逻辑是解耦的； 拉取消息之后并不是在同一个线程中对消息进行处理。
 boolean dispatchToConsume =
 processQueue.putMessage(pullResult.getMsgFoundList());

 ConsumeRequest consumeRequest =
 new ConsumeRequest(msgs, processQueue, messageQueue);
 this.consumeExecutor.submit(consumeRequest);


 为什么pull获取到的消息 不直接放入到线程池中，还需要放入到ProcessQueue呢？因为如果直接放入到线程池很难监控，比如如何得知当前消息堆积的数量，如何重复处理某些消息。RocketMQ定义了一个快照类ProcessQueue来解决这个问题，他保存了对应的MessageQueue消息处理快照状态。
 ProcessQueue对象主要内容是一个TreeMap，TreeMap以MessageQueue的offset作为key，消息内容作为value，保存所有从MessageQueue获取到，但是还未被处理的消息。



 PullRequest 请求对象是每一个topic一个还是每一个MessageQueue一个？系统内会有多少个拉取线程？ 拉取消息的时候是根据什么寻找消息的，拉取到的是哪些消息（单个topic还是多个topic？）

 PullRequest请求对象中主要有consumerGroup消费者组、messageQueue待拉取消息队列，processQueue 消息处理队列，nextOffset 待拉取MessageQueue的偏移量。因此PullRequest是针对某一个Topic下的某一个MessageQueue进行拉取。
 如果一个consumer对象处理同一个Topic的两个消息队列MessageQueue 就会有两个PullRequest
 这一点可以从org.apache.rocketmq.client.impl.consumer.RebalanceImpl#updateProcessQueueTableInRebalance
 方法中看出来。
 for (MessageQueue mq : mqSet) {
 ProcessQueue pq = new ProcessQueue();
 PullRequest pullRequest = new PullRequest(）
 pullRequest.setMessageQueue(mq);
 pullRequest.setProcessQueue(pq);
 pullRequestList.add(pullRequest);
 }

 对pullRequestList 中的每一个PullRequest放置到 pullRequestQueue队列中
 org.apache.rocketmq.client.impl.consumer.PullMessageService#pullRequestQueue
 for (PullRequest pullRequest : pullRequestList) {
 this.pullRequestQueue.put(pullRequest);
 }


 *
 *
 *
 *
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    /**
     * 消息推模式实现类
     */
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    /**
     * 消费者对象
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    /**
     *  并发消息业务事件类 。
     */
    private final MessageListenerConcurrently messageListener;
    /**
     * 消息消费任务队列，队列中的对象是ConsumeRequest对象
     *
     *
     *
     *
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;
    /**
     * 消息消费线程池 。
     *
     *
     * Consumer创建的时候会创建一个线程池，同时这个线程池指定了使用Consumer对象中的consumeRequestQueue队列。
     * 消息消费的过程是，PullMessageService线程执行消息拉取会通过pullMessage方法
     * org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage
     * 在PullMessage方法中 对拉取到的消息 会放入到ProcessQueue 消息处理队列中。
     * 同时会将消息封装一个ConsumeRequest，并将这个ConsumeRequest提交到消费者的消费线程池中处理。因此我们说消息的拉取和消息的处理逻辑是解耦的； 拉取消息之后并不是在同一个线程中对消息进行处理。
     *
     *
     */
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    /**
     * 添加消费’任务到 consumeExecutor
     * 延迟调度器
     */
    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        /**
         * 这里为Consumer创建一个线程池，消息处理逻辑在各个线程里同时执行《RocketMQ实战与解析》
         *
         *
         */
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                cleanExpireMsg();
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
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
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // + 1);
        // }
        // log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public void decCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // - 1);
        // }
        // log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    /**
     *
     * 直接消费消息，主要用于通过管道命令接收到消费消息
     *
     * @param msg
     * @param brokerName
     * @return
     */
    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
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
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

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
     * 为了揭示消息消费的完整过程，从服务器拉取到消息后回调 PullCallBack 回调方法后，
     * 先将消息放入到 ProccessQueue 中，然后把消息提交到消费线程池中执行，也就是调用 Con
     * sumeMessageService#submitConsumeRequest 开始进入到消息消费的世界中来 。
     *
     * @param msgs  消息列表，默认一次从服务器最多拉取 32 条 。
     * @param processQueue  消息处理队列 。
     * @param messageQueue ：消息所属消费队列 。
     * @param dispatchToConsume 是否转发到消费线程池，并发消费时忽略该参数。
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispatchToConsume) {
        /**
         * 消息批次，在这里看来就是一次消息消费任务ConsumeRequest中包含的消息条数，默认为1， msgs.size默认最多为32条。 受DefaultMQPushConsumer.pullBatchSize 属性控制
         *
         *
         */
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        /**
         * 如果msg.siz小于 consumeMessageBatchMaxSize 则直接将拉取到的消息放入到ConsumeRequest中，然后将ConsumeRequest提交到消息消费者线程池中
         */
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                /**
                 * ，如果提交过程中出现拒绝提交异常则延迟 5s 再提交，这里其
                 * 实是给出一种标准的拒绝提交实现方式，实际过程中由于消费者线程池使用的任务队列为
                 * LinkedB lo ck in gQu e u e 无界队列，故不会出现拒绝提交异常 。
                 */
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            /**
             * 如果拉取的消息条数大于 consumeMessageBatchMaxSize ， 则对拉取消息进行分
             * 页，每页 consumeMessag巳BatchMaxSize 条消息，创建多个 ConsumeReq uest 任务并提交到
             * 消费线程池 。 Cons um巳Request 的 run 方法封装了具体消息消费逻辑。
             */
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }


    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest
    ) {

        /**
         * ：根据消息监昕器返回的结果 ， 计算 ackIndex ，如果返回 CONSUME_SUCCESS,
         * acklndex 设置为 msgs.size()-1 ，如果返回 RECONSUME _LATER,  acklndex =-1 ， 这是为下
         * 文发送 msg back  ( ACK ）消息做准备的 。
         *
         */
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;


        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        /**
         *   * 消息消费者在消费一批消息后，需要记录该批消息已经消费完毕，否则 当消费者重新
         *                  * 启动时又得从消息消费队列的开始消费，这显然是不能接受的 。 从 5.6.l 节也可以看到，一
         *                  * 次消息消费后会从 ProcesQueue 处理队列中移除该批消息，返回 ProceeQueue 最小偏移量 ，
         *                  * 并存入消息进度表中 。 那消息进度文件存储在哪合适呢？
         *                  *
         *                  * 广播模式 ： 同一个消费组的所有消息消费者都需要消费主题下的所有消息，也就是同
         *                  * 组内的消费者的消息消费行为是对立的，互相不影响，故消息进度需要独立存储，最理想
         *                  * 的存储地方应该是与消费者绑定 。
         *                  * 集群模式：同一个消费组内的所有消息消费者共享消息主题下的所有消息， 同 一条消
         *                  * 息（同一个消息消费队列）在同一时间只会被消费组内的一个消费者消费，并且随着消费队
         *                  * 列的动态变化重新负载，所以消费进度需要保存在一个每个消费者都能访问到的地方。
         */
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                /**
                 * 如果是广播模式， 业务方返回 RECONSUME_LATER ，消息并不会重新被消
                 * 费，只是以警告级别输出到日志文件
                 */
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
                /**
                 *  如果是集群模式， 消息消费成功，由于 acklndex =consumeRequest.getMsgs().size()-1， 故 i=acklndex + 1 等于 consumeRequest.getMsgs().size() ,
                 * 并不会执行 sendMessageBack 。
                 */
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());
                /**
                 * 消息消费成功的时候 acklindex=msgs.size()-1, i=actIndex+1=msgs.size 所以并不会执行下面的for方法
                 *
                 *  只有在业务方返回 RECONSUME_LATER 时（此时ackIndex=-1），该批消息都
                 * 需要发 ACK 消息，如果消息发送 ACK 失败，则直接将本批 ACK 消费发送失败的消息再
                 * 次封装为 ConsumeRequest ，然后延迟 5s后重新消费 。 如果 ACK 消息发送成功，则该消息
                 * 会延迟消费.
                 *
                 * 如果消息监听器返回的消费结果为 RECONSUME LATER ，则需要将这些消息发送
                 * 给 Broker 延迟消息 。 如果发送 ACK 消息失败，将延迟 5s 后提交线程池进行消费。 ACK
                 * 消息发送的网络客户端人口： MQClientAPIImpl#consumerSendMessageBack
                 *
                 */
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    boolean result = this.sendMessageBack(msg, context);
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        /**
         *
         * 从 Process Queue 中 移除这批 消息， 这里 返回的偏移量是移除该批消息后最
         * 小的偏移量，然后用该偏移量更新消息消 费进度 ，以便在消费者重启后能从上一次的消费
         * 进度开始消费，避免消息重复消费 。 值得重点注意的是当消息监听器返回 RECONSUME_LATER ，消息消 费进度也会向前推进，用 Proces sQueue 中 最小的队列偏移量调用消息消费
         * 进度存储器 OffsetStore 更新消费进度，这是因为当返回 RECONSUME_LATER, RocketMQ
         * 会创建一条与原先消息属性相同的消息，拥有一个唯一 的新 msgld ，并存储原消息 ID ，该
         * 消息会存入到 commitlog 文件中，与原先的消息没有任何关联，那该消息当然也会进入到
         * ConsuemeQueue 队列中，将拥有一个全新的队列偏移量 。
         *
         * 对消息消费的其中两个重要步骤进行详细分析， ACK 消息发送与消息消费进度存储
         *
         */
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            /**
             * ：进入具体消息消费时会先检查 processQueue 的 dropped ，如果设置为 true ， 则
             * 停止该队列的消费，在进行消息重新负载时如果该消息 队列 被分配给消费组 内 其他消 费者
             * 后，需要 droped 设置为 true ， 阻止消费者继续消费不属于自己的消息队列 。
             */
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                /**
                 * 执行 hook的before
                 *  hook.consumeMessageBefore(context);
                 *
                 *  通过 consumer.getDefaultMQPushConsumerlmpl().registerConsumeMessageHook (hook ）方
                 * 法消息消费执行钩子函数。
                 */
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                /**
                 * ：执行具体的消息消费，调用应用程序消息监昕器的 consumeMessage 方法，
                 * 进入到具体的消息消费业务逻辑，返回该批消息的消费结果。 最终将返回 C ONSUME
                 * SUCCESS  （消费成功）或 RECONSUME LATER  （需要重新消费） 。
                 *
                 */
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
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
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                /**
                 * ：执行消息消费钩子函数 ConsumeMessageHook#consum巳MessageAfter 函数。
                 */
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            /**
             * ：执行业务消息消费后，在处理结果前再次验证一下 ProcessQueue 的 isDroped
             * 状态值，如果设置为 true，将不对结果进行处理，也就是说如果在消息消费过程中进入到
             * 执行消息消费钩子函数consumeMessageBefore时，如果由于由新的消费者加入或原先的消费者出现若机导致原先分给消费者的队列
             * 在负载之后分配给别的消费者，那么在应用程序的角度来看的话，消息会被重复消费 。
             */
            if (!processQueue.isDropped()) {
                /**
                 * 处理消息消费结果
                 * 消息消费者在消费一批消息后，需要记录该批消息已经消费完毕，否则 当消费者重新
                 * 启动时又得从消息消费队列的开始消费，这显然是不能接受的 。 从 5.6.l 节也可以看到，一
                 * 次消息消费后会从 ProcesQueue 处理队列中移除该批消息，返回 ProceeQueue 最小偏移量 ，
                 * 并存入消息进度表中 。 那消息进度文件存储在哪合适呢？
                 *
                 * 广播模式 ： 同一个消费组的所有消息消费者都需要消费主题下的所有消息，也就是同
                 * 组内的消费者的消息消费行为是对立的，互相不影响，故消息进度需要独立存储，最理想
                 * 的存储地方应该是与消费者绑定 。
                 * 集群模式：同一个消费组内的所有消息消费者共享消息主题下的所有消息， 同 一条消
                 * 息（同一个消息消费队列）在同一时间只会被消费组内的一个消费者消费，并且随着消费队
                 * 列的动态变化重新负载，所以消费进度需要保存在一个每个消费者都能访问到的地方。
                 *
                 *--------------------分隔线------------
                 * 在 processConsumeResult中:(1）从ProcessQueue中移除本批消费的消息（2）更新偏移量
                 *
                 * 第一点： 消费者线程池每处理完一个消息消费任务（ ConsumeRequest ）时会从 ProceeQ u eu e中移除本批消 费 的消息 ，并返回 ProceeQueue 中 最小的偏移量，
                 * 用该偏移量更新消息 队列消费进度，也就是说更新消费进度与消费任务中的消息没什么关系例如现在两个消费
                 * 任务 taskl ( queueOffset 分别为 20,40 ),  task2  ( 50,70 ），并且 ProceeQueue 中当前包含最
                 * 小消息偏移量为 10 的消息 ， 则 task2 消 费结束后，将使用 10 去更新消费进度， 并不会是
                 * 70 。 当 taskl 消费结束后 ，还是 以 10 去更新消费 队列消息进度，消息消费进度的推进取决
                 * 于 ProceeQueue 中偏移量最小的消息消费速度 。 如果偏移量为 10 的消息消费成功后，假如
                 * ProceeQu eue 中 包含消息偏移量为 100 的 消息， 则消 息偏移量为 10 的消息消 费成功后，将
                 * 直接用 100 更新消息消费进度。 那如果在消费消息偏移量为 10 的消息时发送了死锁导致
                 * 一直无法被消费， 那岂不是消 息进度无法 向前推进 。 是的，为 了避免这种情况， RocketMQ
                 * 引入了 一 种消息拉取流 控 措施： DefaultMQPushConsumer#consumeConcurrentlyMaxSpan=2000 ，消息处理队列 ProceeQueue 中
                 * 最大消息偏移与最小偏移量不能超过该值，如超过该值，触发流控，将延迟该消息队列的消息拉取。《RocketMQ技术内蒙5.6.3消息进度管理》
                 *
                 * 第二点： ）触发消息消费进度更新的另外一个是在进行消息负载时，如果消息消费队列被分配
                 * 给其他消 费者 时，此时会将该 ProceeQueue 状态设置为 drop 时，持久化该消息 队列的消 费
                 * 进度，并从内存中移除 。
                 */
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
