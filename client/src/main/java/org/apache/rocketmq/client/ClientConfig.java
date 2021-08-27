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
package org.apache.rocketmq.client;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

/**
 * Client Common configuration
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
    /**
     * 配置说明：客户端IP
     *
     * 默认值：RemotingUtil.getLocalAddress()
     *
     *
     *
     * 这个值有两个用处：
     *
     * 对于默认的instanceName(见前面部分），如果没有显示设置，会使用ip+进程号，其中的ip便是这里的配置值
     * 对于Producer发送消息的时候，消息本身会存储本值到bornHost，用于标记消息从哪台机器产生的
     */
    private String clientIP = RemotingUtil.getLocalAddress();
    /**
     *
     * 配置说明：客户端实例名称
     *
     * 默认值：从-D系统参数rocketmq.client.name获取，否则就是DEFAULT
     *
     *
     * 这个值虽然默认写是DEFAULT，但在启动的时候，如果我们没有显示修改还是维持其DEFAULT的话，RocketMQ会更新为当前的进程号：
     *
     * public void changeInstanceNameToPID() {
     *     if (this.instanceName.equals("DEFAULT")) {
     *         this.instanceName = String.valueOf(UtilAll.getPid());
     *     }
     * }
     * RocketMQ用一个叫ClientID的概念，来唯一标记一个客户端实例，一个客户端实例对于Broker而言会开辟一个Netty的客户端实例。
     * 而ClientID是由ClientIP+InstanceName构成，故如果一个进程中多个实例（无论Producer还是Consumer）ClientIP和InstanceName都一样,他们将公用一个内部实例（同一套网络连接，线程资源等）
     *
     * 此外，此ClientID在对于Consumer负载均衡的时候起到唯一标识的作用，一旦多个实例（无论不同进程、不通机器、还是同一进程）的多个Consumer实例有一样的ClientID，负载均衡的时候必然RocketMQ任然会把两个实例当作一个client（因为同样一个clientID）。
     *
     * 故为了避免不必要的问题，ClientIP+instance Name的组合建议唯一，除非有意需要共用连接、资源。
     *
     *
     *
     * 注：如果一个进程内需要连接多个集群（不同NameServer）时候，必须要设置不同的instance name，否则会出现“窜乱”的现象，因为内部管理的时候，一个Client实例只能有一个NameServer的地址，例如：消费者C从自身团队A集群监听消息，然后又作为生产者P发送消息给另外一个团队管理的B集群，这时候新建C和P的时候，必须要显示设置不同的instanceName（建议标识中带上进程号），如下：
     *
     * consumer.setInstanceName("AConsumer"+UtilAll.getPid());
     * ...
     * producer.setInstanceName("BProducer"+UtilAll.getPid());
     *
     *
     */
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    /**
     * 配置说明：客户端通信层接收到网络请求的时候，处理器的核数
     *
     * 默认值： Runtime.getRuntime().availableProcessors()
     *
     *
     * 虽然大部分指令的发起方是客户端而处理方是broker/NameServer端，但客户端有时候也需要处理远端对发送给自己的命令，最常见的是一些运维指令
     * 如GET_CONSUMER_RUNNING_INFO，或者消费实例上线/下线的推送指令NOTIFY_CONSUMER_IDS_CHANGED，这些指令的处理都在一个线程池处理，clientCallbackExecutorThreads控制这个线程池的核数。
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    protected String namespace;
    protected AccessChannel accessChannel = AccessChannel.LOCAL;

    /**
     * Pulling topic information interval from the named server
     * 配置说明：轮询从NameServer获取路由信息的时间间隔
     *
     * 客户端依靠NameServer做服务发现（具体请看：RocketMQ原理（1）--服务端组件介绍 - 知乎专栏），这个间隔决定了新服务上线/下线，客户端最长多久能探测得到。默认是30秒，就是说如果做broker扩容，最长需要30秒客户端才能感知得到新broker的存在。
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * Heartbeat interval in microseconds with message broker
     * 配置说明：定期发送注册心跳到broker的间隔
     *
     客户端依靠心跳告诉broker“我是谁（clientID，ConsumerGroup/ProducerGroup）”，“自己是订阅了什么topic"，“要发送什么topic”。以此，broker会记录并维护这些信息。客户端如果动态更新这些信息，最长则需要这个心跳周期才能告诉broker。
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     *
     *配置说明：作用于Consumer，持久化消费进度的间隔
     * RocketMQ采取的是定期批量ack的机制以持久化消费进度。也就是说每次消费消息结束后，并不会立刻ack，而是定期的集中的更新进度。
     * 由于持久化不是立刻持久化的，所以如果消费实例突然退出（如断电）或者触发了负载均衡分consue queue重排，有可能会有已经消费过的消费进度没有及时更新而导致重新投递。故本配置值越小，重复的概率越低，但同时也会增加网络通信的负担。
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    private long pullTimeDelayMillsWhenException = 1000;
    private boolean unitMode = false;
    private String unitName;
    /**
     * 是否启用vip netty通道以发送消息
     * broker的netty server会起两个通信服务。两个服务除了服务的端口号不一样，其他都一样。其中一个的端口（配置端口-2）作为vip通道，客户端可以启用本设置项把发送消息此vip通道。
     */
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));

    private boolean useTLS = TlsSystemConfig.tlsEnable;

    private LanguageCode language = LanguageCode.JAVA;

    /**
     * clientId 的生成策略如下：
     *
     * clientIp：客户端的 IP 地址。
     * instanceName：实例名称，默认值为 DEFAULT，但在真正 clientConfig 的 getInstanceName 方法时如果实例名称为 DEFAULT，会自动将其替换为进程的 PID。
     * unitName：单元名称，如果不为空，则会追加到 clientId 中。
     *
     * *
     *          *  经过测试发现如下： 首先 方法getOrCreateMQClientInstance  的参数是 ClientConfig。
     *          *  在RocketMQ中 config 有几种情况： DefaultMQProducer  DefaultMQPullConsumer 、DefaultMQPushConsumer等等。
     *          *  一般我们创建一个Producer 或者Consumer 之后 会调用器start方法。
     *          *
     *          *  在Producer 和Consumer中都会有一个属性 MQClientInstance mQClientFactory
     *          *
     *          *  比如Producer的start方法中：
     *          *  this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);
     *          *
     *          *  Consumer的start方法中：
     *          *   this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);
     *          *
     *          *   Consumer中和Producer中 通过MQClientManager.getInstance() 拿到的MQClientManager是一样 的，整个JVM内只有一个MQClientManager。
     *          *
     *          *   但是Consumer和Producer拿到的MQClientInstance是不同的，因为 两者的参数ClientConfig不同的，导致 生成的clientId是不同的。
     *
     *
     *          对于Consumer而言，在生成clientId之前 会先判断 instanceName是否是default，如果是则将其修改为pid。 具体参考Consumer的start
     *          对于Producer而言，在生成clientid之前会 先判断instanceName是否是 RocketMQ内置的一个ProducerGroup 如果是才将instanceName修改为Pid，具体参考Producer的start
     *          因此假设你创建一个默认值的Consumer和默认值的Producer，则会生成两个不同的clientid:
     *          consumer的clientid: currentIp@pid@namserverAddr -- 根据这个clientId创建一个MQClientInstance
     *          Producer的clientId currentId@Default@NameServerAddr ---根据这个clientId创建一个MQClientInstance
     *
     *
     * @return
     */
    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        return sb.toString();
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void changeInstanceNameToPID() {

        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = String.valueOf(UtilAll.getPid());
        }
    }

    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<String>();
        for (String resource : resourceSet) {
            resourceWithNamespace.add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    public String withoutNamespace(String resource) {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<String>();
        for (String resource : resourceSet) {
            resourceWithoutNamespace.add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    public MessageQueue queueWithNamespace(MessageQueue queue) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queue;
        }
        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
    }

    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        Iterator<MessageQueue> iter = queues.iterator();
        while (iter.hasNext()) {
            MessageQueue queue = iter.next();
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.namespace = cc.namespace;
        this.language = cc.language;
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.namespace = namespace;
        cc.language = language;
        return cc;
    }

    public String getNamesrvAddr() {
        if (StringUtils.isNotEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
            return namesrvAddr.substring(NameServerAddressUtils.ENDPOINT_PREFIX.length());
        }
        return namesrvAddr;
    }

    /**
     * Domain name mode access way does not support the delimiter(;), and only one domain name can be set.
     *
     * @param namesrvAddr name server address
     */
    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public int getPollNameServerInterval() {
        return pollNameServerInterval;
    }

    public void setPollNameServerInterval(int pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public long getPullTimeDelayMillsWhenException() {
        return pullTimeDelayMillsWhenException;
    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(final boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public String getNamespace() {
        if (StringUtils.isNotEmpty(namespace)) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(this.namesrvAddr)) {
            if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr)) {
                return NameServerAddressUtils.parseInstanceIdFromEndpoint(namesrvAddr);
            }
        }
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public AccessChannel getAccessChannel() {
        return this.accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }


    @Override
    public String toString() {
        return "ClientConfig [namesrvAddr=" + namesrvAddr + ", clientIP=" + clientIP + ", instanceName=" + instanceName
            + ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads + ", pollNameServerInterval=" + pollNameServerInterval
            + ", heartbeatBrokerInterval=" + heartbeatBrokerInterval + ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval
            + ", pullTimeDelayMillsWhenException=" + pullTimeDelayMillsWhenException + ", unitMode=" + unitMode + ", unitName=" + unitName + ", vipChannelEnabled="
            + vipChannelEnabled + ", useTLS=" + useTLS + ", language=" + language.name() + ", namespace=" + namespace + "]";
    }
}
