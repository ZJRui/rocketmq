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
package org.apache.rocketmq.client.impl.factory;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class MQClientInstance {
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final InternalLogger log = ClientLogger.getLog();
    private final ClientConfig clientConfig;
    private final int instanceIndex;
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();
    /**
     * 对于新创建的Producer 在 start的时候 org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#start(boolean) 会调用registerProducer，在register的时候会放置到producerTable
     * key是ProducerGroup， 这表示在一个JVM进程内（准确来说应该是在一个MQClientInstance对象内） 一个ProducerGroup就仅有一个唯一的producer
     */
    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();
    /**
     * key是consumerGroup，这表示在一个JVM进程内（准确来说应该是在一个MQClientInstance对象内） 一个ConsumerGroup就仅有一个唯一的consumer
     *
     */
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();
    private final NettyClientConfig nettyClientConfig;
    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();

    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<String, HashMap<Long, String>>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
        new ConcurrentHashMap<String, HashMap<String, Integer>>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });
    private final ClientRemotingProcessor clientRemotingProcessor;
    /**
     * 创建MQClientInstance的时候会创建一个PullMessageService对象，在构造器中。
     */
    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;
    private final DefaultMQProducer defaultMQProducer;
    private final ConsumerStatsManager consumerStatsManager;
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private Random random = new Random();

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        /**
         * 注意 MQClientInstance 内部有一个defaultMQProducer属性，这个属性时RocketMQ 内部的producer，区别于应用程序中使用的producer
         *  CLIENT_INNER_PRODUCER
         */
        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}",
            this.instanceIndex,
            this.clientId,
            this.clientConfig,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        /**
         * 将Topic信息封装成一个路由信息TopicPublishInfo，交给每一个Producer
         * Producer关心的是这个topic中可写队列的信息。
         */
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        /**
         * 如果从NameSever中取到的TopicRouteData 配置该topic是order
         */
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {

            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    /**
                     * 这里说明一个MessageQueue 和一个broker绑定在一起
                     */
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        } else {
            /**
             * 获取topic的QueueData，两个BrokerName 就有两个QueueData
             */
            List<QueueData> qds = route.getQueueDatas();
            /**
             * 根据brokerName进行排序
             */
            Collections.sort(qds);
            for (QueueData qd : qds) {
                /**
                 * 如果该BrokerName存在写权限
                 */
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    /**
                     * 根据brokername找到该Brokername的的BrokerData，brokerData中存储了该BrokerName中的broker节点的主从结构
                     */
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        continue;
                    }

                    /**
                     * 如果在改brokerName的 broker节点中 没有master节点则返回
                     */
                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }
                    /**
                     * 获取该BrokerName中 写队列的数量，然后创建多个MessageQueue，
                     *
                     * 那么也就是说 假设topic有两个BrokerName：A和B
                     * A对应一个QueueData，同时A中有两个broker节点主从。  Queuedata中写队列个数为3
                     * B对应一个QueueData，同时B中有三个broker节点主从。  Queuedata中写队列个数为4
                     * 那么这个Topic的写队列的个数就是7 ，也就是有了7个MessageQueue
                     *
                     *从这里我们可以看到 实际上MmessageQueue 实际上对应着 整个Topic中可以写队列的数量。
                     * 生产者发送小的时候必然是向 有权限写的队列中写入数据。
                     *
                     * 因此Producer通过org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#fetchPublishMessageQueues(java.lang.String)
                     * 方法 获取Topic的publisInfo，在这个PublisInfo中就包含该topic的MessageQueue
                     * 这个MessageQueue就是可以写的队列
                     */
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        /**
         * 提取Topic信息中的可写队列的数量，封装起来交给Consumer，consumer关心的是可读队列
         */
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();
        /**
         * 关于QueueData，一个BrokerName对应一个QueueData
         */
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            /**
             * QueueData具有读权限的是时候获取其可读队列的数量，然后为每一个对垒创建一个MessageQueue
             */
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }

        return mqList;
    }

    /**
     *
     * @throws MQClientException
     */
    public void start() throws MQClientException {

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    /**
                     * 仅仅创建不启动
                     */
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    /**
                     * 创建Netty Client
                     */
                    this.mQClientAPIImpl.start();
                    /**
                     * 启动定时任务 fetchNameServerAddr
                     */
                    // Start various schedule tasks
                    this.startScheduledTask();
                    // Start pull service
                    /**
                     * pullMessageService 继承自ServiceThread 类型为Runnable，这里调用了他的start方法
                     *
                     * ServiceThread内部有一个属性 Thread thread;， 在start方法内会调用thread.start，从而启动了一个线程
                     * 在PullMessageService的run方法中会执行：
                     *  PullRequest pullRequest = this.pullRequestQueue.take();
                     *  this.pullMessage(pullRequest);
                     *
                     *
                     *  不管是PullConsumer还是PushConsumer  都会使用MQClientInstance， MQClientInstance启动的时候会启动PullMessageService
                     *
                     * 也就是说即便你使用的是PushConsumer，那么PushConsumer启动的时候导致MQClientInstance 也start 从而也会启动PullMessageService
                     *
                     */
                    this.pullMessageService.start();
                    // Start rebalance service
                    /**
                     * 这里会启动一个线程执行 RebalanceService的run，在run方法中
                     * 会执行doRebalance，在rebalance中会遍历 MQClientInstance的所有consumer，
                     * 然后针对每一个consumer调用 consumer.doRebalance();
                     *
                     * 不同的consumer ,PullConsumer 和PushConsumer 有不同的doRebalance实现，但他们最终都是调用了rebalanceImpl的doRebalance
                     *
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
                     */
                    this.rebalanceService.start();
                    // Start push service
                    /**
                     * Start push service
                     *
                     * 我们知道Producer和Consumner的启动start方法都会调用到MQClientInstance的start方法，但是MQClientInstance 本身有一个状态属性serviceState
                     *实际上 MQClientInstance的start方法的逻辑只会执行一次。 而且MQClientInstance 对象内部存在一个defaultMQProducer属性，这个属性时MQClient内置的producer
                     * 为了支持MQClient内部的某写功能而自动创建了一个Producer。
                     *
                     *
                     *
                     */
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    private void startScheduledTask() {
        /**
         * 第一个定时任务，fetchNameServer
         */
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        /**
         * 第二个定时任务
         * MQClient 端启动定时任务 更新topic的路由信息。 具体更新哪些路由 会从consumerTable ，producerTable中遍历每一个consumer
         * producer，取出其中的topic，然后更新每一个topic的路由信息
         *
         * MQClientInstance.updateTopicRouteInfoFromNameServer(String, boolean, DefaultMQProducer)  (org.apache.rocketmq.client.impl.factory)
         *     MQClientInstance.updateTopicRouteInfoFromNameServer(String)  (org.apache.rocketmq.client.impl.factory)
         *         MQClientInstance.updateTopicRouteInfoFromNameServer()  (org.apache.rocketmq.client.impl.factory)
         *       这一步     Anonymous in startScheduledTask() in MQClientInstance.run()  (org.apache.rocketmq.client.impl.factory)
         *                 MQClientInstance.start()  (org.apache.rocketmq.client.impl.factory)
         *                     DefaultMQPushConsumerImpl.start()  (org.apache.rocketmq.client.impl.consumer)
         *                         DefaultMQPushConsumer.start()  (org.apache.rocketmq.client.consumer)
         *                     DefaultMQPullConsumerImpl.start()  (org.apache.rocketmq.client.impl.consumer)
         *                     DefaultMQProducerImpl.start(boolean)  (org.apache.rocketmq.client.impl.producer)
         *
         * 这里的定时任务会调用 MQClientInstance的updateTopicRouteInfoFromNameServer
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

        /**
         * 第三个定时任务
         *
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    /**
                     * 清除不在线的broker
                     */
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        /**
         * 第四个定时任务，持久化consumerOffset
         *
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String getClientId() {
        return clientId;
    }

    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<String>();
        /**
         * 收集topic和producer的所有topic
         * 对于新创建的Producer 会在这个方法 org.apache.rocketmq.client.impl.factory.MQClientInstance#registerProducer(java.lang.String, org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl) 中
         * 放置到producerTable 这个map中
         *
         * 对于新创建的Producer 在 start的时候 org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#start(boolean) 会调用registerProducer，在register的时候会放置到producerTable
         *
         *
         */

        // Consumer
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    /**
                     *获取这个Consumer的订阅信息
                     *
                     */
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        // Producer
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        /**
         * 以上是收集所有的topic
         */
        for (String topic : topicList) {
            /**
             * 更新每一个topic，获取topic的路由信心更新到consumer和producer中
             */
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * @param offsetTable
     * @param namespace
     * @return newOffsetTable
     */
    public Map<MessageQueue, Long> parseOffsetTableFromBroker(Map<MessageQueue, Long> offsetTable, String namespace) {
        HashMap<MessageQueue, Long> newOffsetTable = new HashMap<MessageQueue, Long>();
        if (StringUtils.isNotEmpty(namespace)) {
            for (Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
                MessageQueue queue = entry.getKey();
                queue.setTopic(NamespaceUtil.withoutNamespace(queue.getTopic(), namespace));
                newOffsetTable.put(queue, entry.getValue());
            }
        } else {
            newOffsetTable.putAll(offsetTable);
        }

        return newOffsetTable;
    }

    /**
     * Remove offline broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

                    /**
                     * 首先我们这里遍历的是brokerAddrTable，  如何判断一个Broker是否在线是根据当 MQClientInstance对象中的路由信息 topicRouteTable
                     * 来决定的，因为这个路由信息会定期从nameServer更新。
                     */
                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            /**
                             * 使用MQClientInstance的路由信息topicRouteTable 判断这个broker是否在线
                             */
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    public void checkClientInBroker() throws MQClientException {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    try {
                        this.getMQClientAPIImpl().checkClientInBroker(
                            addr, entry.getKey(), this.clientId, subscriptionData, 3 * 1000
                        );
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            throw new MQClientException("Check client in broker error, maybe because you use "
                                + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!"
                                + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                "have use the new features which are not supported by server, please check the log!", e);
                        }
                    }
                }
            }
        }
    }

    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                this.sendHeartbeatToAllBroker();
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed. [{}]", this.clientId);
        }
    }

    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            /**
             * 遍历所有的consumer然后调用consumer的持久化方法
             *
             */
            impl.persistConsumerOffset();
        }
    }

    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> entry = it.next();
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * 向Broker发送心跳
     *
     */
    private void sendHeartbeatToAllBroker() {
        /**
         * 心跳信息中包含当前JVM内所有Consumer和Producer的数据
         *
         */
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
            return;
        }

        if (!this.brokerAddrTable.isEmpty()) {
            long times = this.sendHeartbeatTimesTotal.getAndIncrement();
            Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, HashMap<Long, String>> entry = it.next();
                String brokerName = entry.getKey();
                HashMap<Long, String> oneTable = entry.getValue();
                if (oneTable != null) {
                    for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                        Long id = entry1.getKey();
                        String addr = entry1.getValue();
                        if (addr != null) {
                            if (consumerEmpty) {
                                /**
                                 * 如果消费者为空，且id不是masterId，则不需要发送心跳。
                                 */
                                if (id != MixAll.MASTER_ID)
                                    continue;
                            }

                            try {
                                int version = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                                if (!this.brokerVersionTable.containsKey(brokerName)) {
                                    this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                                }
                                this.brokerVersionTable.get(brokerName).put(addr, version);
                                if (times % 20 == 0) {
                                    log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                    log.info(heartbeatData.toString());
                                }
                            } catch (Exception e) {
                                if (this.isBrokerInNameServer(addr)) {
                                    log.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
                                } else {
                                    log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                        id, addr, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void uploadFilterClassSource() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> next = it.next();
            MQConsumerInner consumer = next.getValue();
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                for (SubscriptionData sub : subscriptions) {
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
        DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    /**
                     * isDefault参数表示是否是默认的 producer，如果isDefault为true，且defaultMQProducer不为空
                     */
                    if (isDefault && defaultMQProducer != null) {
                        /**
                         * 如果isDefault为true，则会执行getDefaultTopicRouteInfoFromNameServer。 否则将会执行getTopicRouteInfoFromNameServer
                         * 从这里我们可以看到isDefaut为true，则会执行很多default操作。
                         *
                         * 注意这里 getDefaultTopic的时候 我们传入的topic key是defaultMQProducer.getCreateTopicKey()，这个返回值是 org.apache.rocketmq.common.topic.TopicValidator#AUTO_CREATE_TOPIC_KEY_TOPIC
                         *
                         *  注意这里  不是向nameServer查询 方法参数中的topic，而是查询了defaultMQProducer的 createTopicKey
                         */
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                            1000 * 3);
                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        /**
                         * 当isDefault为false的情况下将会执行这里的逻辑
                         * 从getTopicRouteInfoFromNameServer方法我们看到，如果nameServer返回的 topicRouteInfo为null 则getTopicRouteInfoFromNameServer
                         * 方法直接会抛出异常。 但是 该方法最外层有一个try catch，因此当topic不存在的时候尽管会抛出异常的，但是外部会捕获
                         */
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }

                    /**
                     * 从上面的内容我们知道变量 topicRouteData有两种情况：
                     * （1）isDefault为false，执行 从nameServer查询指定的topic， 如果nameServer不知道该topic，则会返回TOPIC_NOT_EXIST。
                     * （2） isDefault为true，且defaultMQProducer不为null， 则会向nameServer 查询 topic AUTO_CREATE_TOPIC_KEY_TOPIC的路由信息
                     *
                     */
                    if (topicRouteData != null) {
                        /**
                         * 如果topicRouteTable 中没有topic的信息， 则old为null，
                         * 这个时候topicRouteData 变量就是defaultMQProducer的auto_create_key_topic的路由信息。
                         *
                         * 然后topicRouteDataIsChange 会返回true
                         */
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        if (!changed) {
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        /**
                         * changed为true 则clone 路由信息
                         * （1）比如 topic没有创建过。此时 Produer发送消息的时候来查询Topic的路由信息，显然根据topic到NameServer中查询会返回topic不存在
                         * 然后我们就会使用默认的defaultMQProducer的auto_create_key 这个topic的路由信息，然后将这个路由信息clone一份作为新的topic的路由信息
                         *
                         */
                        if (changed) {
                            /**
                             * 克隆一份作为 topic的路由信息
                             */
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            /**
                             * 遍历该Topic的 BrokerName 中的Broker主从节点的地址信息
                             */
                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                /**
                                 * <brokerNameA,(masterIp,broker2ip,broker3Ip   )>
                                 *
                                 * 将Broker的地址更新到brokerAddrTable
                                 */
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update Pub info
                            {
                                /**
                                 * 将TopicRouteData转为TopicPublishInfo。
                                 *
                                 *  注意在上面通过和NameServer进行交互我们得到了Topic的信息数据TopicRouteData，在这个TopicRouteData中 包含了这个topic内有几个BrokerName
                                 *  每一个BrokerName内有几个读队列，有几个写队列。
                                 *
                                 *  对于生产者Producer而言，他关心的是整个Topic内有几个写队列，因为生产者的消息要写入到消息队列中。 对于消费者而言他关心的是有几个读队列。
                                 *
                                 *  因此下面topicRouteData2TopicPublishInfo 方法分析topicRouteData 中写队列的数量，为每一个写队列创建一个MessageQueue对象，放置到TopicPublisInfo中，
                                 *  然后将这个TopicPublisInfo   通过producer的updateTopicPublishInfo方法放置到每一个Producer的topicPublishInfoTable 属性中。
                                 *
                                 *  下面topicRouteData2TopicSubscribeInfo 方法会分析topicRoutedata 中 读队列的信息，并为每一个读队列创建一个MessageQueue对象，然后将读队列
                                 *  信息通过Consumer的updateTopicSubscribeInfo方法更新到每一个Consumer的topicSubscribeInfoTable 属性中。 这样就完成了将topic信息告知Producer和Consumer。
                                 *
                                 *
                                 */
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                /**
                                 * 将这个路由信息 放置到每一个Producer中
                                 *
                                 * 注意这里遍历的是MQClientInstance的producerTable ，因此不同的MQClientInstance 的producerTable是隔离的
                                 */
                                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQProducerInner> entry = it.next();
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {

                                        /**
                                         * 将路由信息放置到每一个Producer中。
                                         */
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // Update sub info
                            {
                                /**
                                 * 将这个路由信息放置到每一个Consumer中
                                 * 注意这里遍历的是MQClientInstance的producerTable ，因此不同的MQClientInstance 的producerTable是隔离的
                                 *
                                 */
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQConsumerInner> entry = it.next();
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        /**
                                         *  Consumer和Producer启动的时候会执行MQClientInstance.start() ，然后 MQClientInstance的start中启动了一个定时任务startScheduledTask
                                         *  这个定时任务会执行 updateTopicRouteInfoFromNameServer 最终会执行到该方法 updateTopicRouteInfoFromNameServer
                                         *
                                         *  针对 consumer如何做更新路由信息呢？ 如下调用了updateTopicSubscribeInfo
                                         *
                                         *
                                         */
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            /**
                             * 将路由信息放置到 当前对象的MQClientInstance的路由信息中。
                             *
                             *  关于MQClientInstance的唯一性： 是否一个JVM进程内只有一个MQClientInstance呢？
                             */
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]", topic, this.clientId);
                    }
                } catch (MQClientException e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    throw new IllegalStateException(e);
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms. [{}]", LOCK_TIMEOUT_MILLIS, this.clientId);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID
        heartbeatData.setClientID(this.clientId);

        // Consumer
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // Producer
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> itNext = it.next();
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    /**
     * This method will be removed in the version 5.0.0,because filterServer was removed,and method
     * <code>subscribe(final String topic, final MessageSelector messageSelector)</code> is recommended.
     */
    @Deprecated
    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName,
        final String topic,
        final String filterClassSource) throws UnsupportedEncodingException {
        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                fullClassName,
                RemotingHelper.exceptionSimpleDesc(e1));
        }

        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null
            && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<String>> next = it.next();
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                            5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                            topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                consumerGroup, topic, fullClassName);
        }
    }

    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();

                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }

    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            if (this.lockHeartbeat.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed. [{}]", this.clientId);
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }

    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    String addr = entry1.getValue();
                    if (addr != null) {
                        try {
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, entry1.getKey(), addr);
                        } catch (RemotingException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (InterruptedException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (MQBrokerException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        }
                    }
                }
            }
        }
    }

    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        /**
         *  一个JVM内针对一个ProducerGroup只能有一个Producer，如果再创建Producer则会提示
         * Caused by: org.apache.rocketmq.client.exception.MQClientException: The producer group[28751_UpgradeTenantPaasMetaDataGroup] has been created before, specify another name please.
         * See http://rocketmq.apache.org/docs/faq/ for further details.
         */
        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }

    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    public void doRebalance() {
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    impl.doRebalance();
                } catch (Throwable e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                    } else {
                        slave = true;
                    }
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    public FindBrokerResult findBrokerAddressInSubscribe(
        final String brokerName,
        final long brokerId,
        final boolean onlyThisBroker
    ) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && slave) {
                brokerAddr = map.get(brokerId + 1);
                found = brokerAddr != null;
            }

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        //To do need to fresh the version
        return 0;
    }

    public List<String> findConsumerIdList(final String topic, final String group) {
        /**
         * 随机选择一个broker地址
         */
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                /**
                 * 从broker地址 对应的broker查询 ConsumerGroup中的consumer列表
                 */
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }

    public String findBrokerAddrByTopic(final String topic) {
        /**
         * 找到topic的路由信息， 随机全出一个BrokerData（也就是一个Brokername）,
         * 从被选中的BrokerName（该Brokername内存在多个Broker节点构成主从结构）中随机选择一个Broker节点返回其地址
         */
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();

            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
            }

            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg,
        final String consumerGroup,
        final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (mqConsumerInner == null) {
            return null;
        }

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }
}
