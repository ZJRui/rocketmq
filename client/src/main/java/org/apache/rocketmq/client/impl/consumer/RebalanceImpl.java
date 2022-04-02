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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    /**
     * 这个是什么关系？
     */
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    /***
     * 该属性的更新参考方法 updateTopicSubscribeInfo
     */
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();
    /**
     *
     * {
     * 	"28751_PriorCommunication":{
     * 		"tagsSet":[],
     * 		"classFilterMode":false,
     * 		"subString":"*",
     * 		"codeSet":[],
     * 		"topic":"28751_PriorCommunication",
     * 		"expressionType":"TAG",
     * 		"subVersion":1630047954019
     *        },
     * 	"%RETRY%28751_SysPriorCommunicationConsumer":{
     * 		"tagsSet":[],
     * 		"classFilterMode":false,
     * 		"subString":"*",
     * 		"codeSet":[],
     * 		"topic":"%RETRY%28751_SysPriorCommunicationConsumer",
     * 		"expressionType":"TAG",
     * 		"subVersion":1630047954026
     *    }
     * }
     *
     *
     *          * RebalanceImpl的 subscriptionInner属性在 调用消费者PushConsumerImpl的subscribe方法时填充
     *          或者在PullCOnsumerImpl的pullMessage方法重传入MessageQueue，这个MessageQueue中就有topic等信息，也可以构建SubscriptionData
     *
     */
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public void unlock(final MessageQueue mq, final boolean oneway) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);

                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.get(mq.getBrokerName());
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    public boolean lock(final MessageQueue mq) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                /**
                 * 这里发送消息 lock Broker上的messageQueue
                 */
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        /**
                         * 设置MessageQueue 对应的ProcessQueue为锁定状态
                         */
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }

                boolean lockOK = lockedMq.contains(mq);
                log.info("the message queue lock {}, {} {}",
                    lockOK ? "OK" : "Failed",
                    this.consumerGroup,
                    mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    public void lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);

                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    public void doRebalance(final boolean isOrder) {
        /**
         * RebalanceImpl的 subscriptionInner属性在 调用消费者PushConsumerImpl的subscribe方法时填充
         */
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    /**
                     * RocketMQ 是如何针对单个主题进行消息 队列重新负 载。
                     *
                     * 在RebalanceService线程的run方法中  会遍历所有的Consumer，然后针对每一个Consumer调用其doReplace方法做重平衡。
                     * Consumer的rebalance 依赖于RebalanceImpl对象，consumer 对象会调用RebalanceImpl对象的doRebalance对方法，同时将consumer
                     * 接收到MessageListener是否是order传递给RebalanceImpl对象。从下面的两个方法中可以看到
                     *
                     * org.apache.rocketmq.client.impl.factory.MQClientInstance#doRebalance()
                     * org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#doRebalance()
                     *
                     */
                    this.rebalanceByTopic(topic, isOrder);
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    private void rebalanceByTopic(final String topic, final boolean isOrder) {
        switch (messageModel) {
            case BROADCASTING: {
                /**
                 * 获取Topic的MessageQueue，MessageQueue其实就是该Topic下有多少个读队列
                 *
                 * 从主题订阅信息缓存表中获取主题的队列信息；
                 *
                 */
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}",
                            consumerGroup,
                            topic,
                            mqSet,
                            mqSet);
                    }
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: {
                /**
                 * 如果是集群模式，则一个ConsumerGroup中的3个consumer 只能有一个consumer接收到消息。
                 *
                 * 首先获取该topic的 messageQueue
                 */
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                /**
                 * 获取topic的所有consumerId
                 * 发送请求 从broker中该消费组内当前所有的消费者客户端id，主题topic的队列可能分布在多个broker上，那么请求发往哪个broker呢？
                 * rocketMQ从主题的路由信息表中随机选择一个broker。
                 *
                 * broker为什么会存在消费组内所有消费者的信息呢？ 我们不妨回忆下消费者在启动的时候会向MQClientInstance中注册消费者，然后MQClientInstance会向所有的Broker发送心跳包，
                 * 心跳包中包含MQClientInstance的消费者信息。
                 */
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                /**
                 * 如果mqSet或者cidALl 任意一个位空则忽略本次消息队列负载
                 */
                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);

                    /**
                     * 首先对 cidAll,mqAll 排序，这个很重要，同一个消费组内看到的视图保持一致，
                     * 确保同一个消费队列不会被多个消费者分配
                     */
                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    /**
                     *  消息负载算法如果没有特殊的要求，尽量使用 AllocateMeseQueueAveragely 、AllocateMessageQueueAveragelyByCircle ，因为分配算法比较直观 。 消息队列分配遵循
                     * 一个消费者可以分配多个消息队列，但同一个消息队列只会分配给一个消费者，故
                     * 如果消费者个数大于消息队列数量，则有些消费者无法消费消息 。
                     */
                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    List<MessageQueue> allocateResult = null;
                    try {
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}", strategy.getName(),
                            e);
                        return;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }

                    /**
                     * 对比消息队列是否发生变化。
                     * 注意第二个参数 allocateResult， 这个参数是 当前Consumer根据 消息队列分配算法 计算得到的应该分配到的MessageQueue。
                     *
                     * 对于广播模式，每一个Consumer消费Topic的所有消息，因此广播模式下 每个Consumer 负载 Topic的所有messageQueue,因此在上面的方法中我们看到传递的是mqSet
                     */
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    /**
     * RebalanceImpl.rebalanceByTopic(String, boolean)(2 usages)  (org.apache.rocketmq.client.impl.consumer)
     *     RebalanceImpl.doRebalance(boolean)  (org.apache.rocketmq.client.impl.consumer)
     *         DefaultMQPullConsumerImpl.doRebalance()  (org.apache.rocketmq.client.impl.consumer)
     *         DefaultMQPushConsumerImpl.doRebalance()  (org.apache.rocketmq.client.impl.consumer)
     *             DefaultMQPushConsumerImpl.resume()  (org.apache.rocketmq.client.impl.consumer)
     *                 DefaultMQPushConsumer.resume()  (org.apache.rocketmq.client.consumer)
     *                     PushConsumerImpl.resume()  (io.openmessaging.rocketmq.consumer)
     *                 MQClientInstance.resetOffset(String, String, Map<MessageQueue, Long>)  (org.apache.rocketmq.client.impl.factory)
     *                     DefaultMQPushConsumerImpl.resetOffsetByTimeStamp(long)  (org.apache.rocketmq.client.impl.consumer)
     *                     ClientRemotingProcessor.resetOffset(ChannelHandlerContext, RemotingCommand)  (org.apache.rocketmq.client.impl)
     *                         ClientRemotingProcessor.processRequest(ChannelHandlerContext, RemotingCommand)  (org.apache.rocketmq.client.impl)
     *
     *
     *
     *    首先祝贺易RebalanceImpl是在client端的，也就是Producer/Consumer端
     *  （1）ClientRemotingProcessor 的processRequest如果收到的请求code是RESET_CONSUMER_CLIENT_OFFSET，则会执行 resetOffset，意味着如果客户端收到的请求的code是RESET_CONSUMER_CLIENT_OFFSET，则会执行 resetOffset
     *  问题：谁发来的请求？
     *  (2)
     *
     *
     * @param topic
     * @param mqSet
     * @param isOrder
     * @return
     */
    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        /**
         * 首先要明确 参数mqSet是怎么来的
         * 参数mqSet是从consumer的rebalanceImpl的topicSubscribeInfoTable 中根据topic提取出来的。
         * 而topicSubscribeInfoTable中的数据 又是 MQClientInstance 启动的了一个定时任务updateTopicRouteInfoFromNameServer 定时从nameServer读取topic的TopicRouteData
         * 然后分析TopicRouteData 更新到 consumer的topicSubscribeInfoTable属性中，和Producer的TopicPublishInfo属性中
         *
         * 在本方法中实现的逻辑 就是基于  topicSubscribeInfoTable 更新 rebalanceImpl的processQueueTable
         *
         * 遍历processQueueTable 中的每一个key Value，判断processQueueTable 中的MessageQueue是否 还在 topicSubscribeInfoTable 中
         *
         */
        /**
         *
         * 这是第一个遍历：
         * 对比消息队列是否发生变化，主要思路是遍历当前负载队列集合，如果队列不在新分
         * 配队列集合中，需要将该队列停止消费并保存消费进度；
         */
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            /**
             * MessageQueue是当前topic的
             */
            if (mq.getTopic().equals(topic)) {
                /**
                 * 如果mqSet中不包含 该MessageQueue，则将对应的ProcessQUeue设置为dropp。
                 *
                 * mqSet 是从RebalanceImpl的topicSubscribeInfoTable 中取出来的，这里判断的就是 processQueueTable的 某一个key MessageQueue 是否
                 * 还在topicSubscribeInfoTable中。 如果不在则表示 processQueueTable中存在过期数据
                 *
                 *
                 *  processQueueTable ，当前消费者
                 * 负载的消息队列缓存表，如果缓存表中的 MessageQueue 不包含在 mqSet 中，说明经过本
                 * 次消息队列负载后，该 mq 被分配给其他消费者，故需要暂停该消息队列消息的消费，方法
                 * 是将 ProccessQueue 的状态设置为 draped＝位出，该 ProcessQueue 中的消息将不会再被消费，
                 * 调用 removeU nnecessaryMessageQueue 方法判断是否将 MessageQueue 、 ProccessQueue 缓存
                 * 表中移除。
                 *
                 */
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    /**
                     * 判断是否有必要移除 这个MessageQueue
                     *
                     * remove Unnecessary MessageQueue 在 Rebalancelmple 定义为抽象方法。 removeUnnecessaryMessageQueue 方法主要持久化待移除 MessageQueue 消息消费进度。
                     *
                     */
                    if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                        it.remove();
                        changed = true;
                        log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                    }
                } else if (pq.isPullExpired()) {
                    //上面if中判断当前时间 距离ProcessQueue中的上一次拉取时间属性lastPulltimeStamp 是否超过120s，如果是则认为过期
                    switch (this.consumeType()) {
                        case CONSUME_ACTIVELY://PULL
                            break;
                        case CONSUME_PASSIVELY://push
                            pq.setDropped(true);

                            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                                it.remove();
                                changed = true;
                                log.error("[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                    consumerGroup, mq);
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
        }//end while 结束对 processQueueTable的更新

        /**
         * 创建pull请求，问题： 这里是RebalanceImpl类，存在子类RebalancePushImpl 和RebalancePullImpl， 两者都是用了pullRequest
         */
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        /**
         * 注意我们这里遍历mqSet， mqSet 是从RebalanceImpl的topicSubscribeInfoTable 中取出来的
         *
         * topicSubscribeInfoTable又是定时从updateTopicRouteInfoFromNameServer  中更新的
         *
         * 这里是第二个遍历：
         *
         * 遍历已分配的队列，如果队列不
         * 在队列负载表中（ processQueueTable ） 则需要创建该 队列 拉取任务 PullRequest ， 然后添加
         * 到 PullMessageService 线程的 pullRequestQueue 中， Pul IMessageService 才会继续拉取任务
         *
         */
        for (MessageQueue mq : mqSet) {
            /**
             * 如果processQueueTable中不包含mq
             *
             * ：遍历本次负载分配到的队列 集合，如果 processQueueTable 中没有包含该消息
             * 队列，表明这是本次新增加的消息队列， 首先从内 存 中 移除该消息 队列的消费进度，然后
             * 从磁盘中读取该消息队列的消费进度，创建 PullRequest 对象。
             * 这里有一个关键，如果读取到的消费进度小于 0 ，则 需要校对消费进度。 RocketMQ 提供 CONSUME_FROM_LAST_
             * OFFSET 、 CONSUME_FROM_F IRST OFFSET 、 CONSUME_FROM_TIMESTAMP 方式，
             * 在创建消费者时可以通过调用 DefaultMQPushConsumer#s etConsumeFromWhere 方法设置。
             * PullRequest 的 ne x tOffset 计算逻辑位于 ： RebalancePushlmpl# computePullFromWhere
             *
             */
            if (!this.processQueueTable.containsKey(mq)) {
                /**
                 *
                 * 如果没有锁定就不处理。
                 * 会不会出现当消息队列重新负载时，原先由自己处理 的消息队列被分配给另外一个消费者，此时如果还未来得及将ProcessQueue解除锁定，
                 * 就被另外一个消费者添加进去，此时会存在多个消息消费者消费一个消息队列吗？ 答案是不会的，
                 * 因为当一个新的消费队列分配各消费者时，在添加其拉取任务之前必须先向Broker发送对该消息队列的加锁请求，
                 * 只有加锁成功后才能继续拉取消息，否则等到下一次负载后，只有消费队列被原先占有的消费者释放后，才能开始新的拉取任务。
                 *
                 * ==============
                 * 顺序消息消费和并发消息消费的第一个关键区别： 顺序消息在创建消息队列拉取任务时 需要在Broker服务器锁定该消息队列MessageQueue 也就是下面的 this.lock
                 *
                 *
                 */
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    continue;
                }

                this.removeDirtyOffset(mq);
                /**
                 * 创建一个ProcessQueue 与 MessageQueue 进行对应
                 *
                 *
                 */
                ProcessQueue pq = new ProcessQueue();
                /**
                 * ConsumeFromWhere 相关消费 进度校 正 策略只有 在 从磁盘 中 获取消费 进度返回 一 1
                 * 时才会生效 ， 如果从消息进度存 储文件 中 返 回 的消费进度小 于 一 l ， 表 示 偏移量非
                 * 法 ， 则使用偏移量 － 1 去拉取消息 ， 那么会发生什么呢？首先第 一 次 去 消息服务器
                 * 拉取消息 时 无法取到消息 ， 但是会用 一 l 去更新消费进度 ， 然后将消息消费队列丢
                 * 弃， 在下一 次消息队列负载时会再次消费 。
                 */
                long nextOffset = this.computePullFromWhere(mq);
                if (nextOffset >= 0) {
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        /**
                         *之类创建的是 client.consumer 包中的PullRequest
                         */
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }
        }

        /**
         * 将请求交给PullMessageService ，放置到了PullMessageService 的pullRequestQueue 队列中
         * ：将 PullRequest 加入到 PullMessageService 中 ， 以便唤醒 PullMessageService 线程。
         *
         * 跟踪下dispatchPullRequest 你就会发现 只有.RebalancePushImpl#dispatchPullRequest 才会将PullRequest放置到PullMessageService的队列中
         *
         * RebalancePullImpl的dispatchPullRequest方法并没有将PullRequest放置到PullMessageService的队列中。
         *
         * 因此我们在PullMessageService的队列中取出PullRequest，然后根据PullRequest的ConsumerGroup从consumerTable中取出consumer都是PushConsumer
         *
         */
        this.dispatchPullRequest(pullRequestList);

        /**
         *   上面的 dispatchPullRequest方法将会影响   消息拉取 线程堆 消息的拉取。
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
         *
         *
         */



        return changed;
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
