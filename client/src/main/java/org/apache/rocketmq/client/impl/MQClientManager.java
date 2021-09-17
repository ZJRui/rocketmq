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
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

public class MQClientManager {
    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * 一个JVM内只有一个ClientManager，MQClientlnstance 封装了 RocketMQ 网络处理 API ，是消息生产者（ Producer ）、消息消费者
     * ( Consumer ）与 NameServer 、 Broker 打交道的网络通道。
     *
     */
    private static MQClientManager instance = new MQClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    /**
     * 整个JVM示例中只存在一个MQClientManager示例，维护一个MQClientInstance缓存表，key 是ClientId，Value是MQClientInstance，也就是同一个ClientID只会创建一个
     * MQClientInstance。
     *
     * 一个MQClientInstance内 会有consumerTable，producerTable，topicRouteTable
     */
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
        new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {

    }

    /**
     * 创建 MQC li e ntln s tan ce 实例 。 整个 NM 实例中只存在一个 MQC !ientManager 实
     * 例，维护一个 MQClientlnstanc e 缓存表 ConcurrentMap < String／ ＊巳lientld 灯， MQClientinstanc巳＞
     * facto叩Table =new  ConcurrentHashMap<S tri 吨， MQ C !i e ntln s t ance＞（）， 也 就是 同 一个 cli entld 只
     * 会创建一个 MQClientinstance。
     *
     * @return
     */
    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }

    /**
     * 关于MQClientInstance的唯一性： 是否一个JVM进程内只有一个MQClientInstance呢？
     * 一般来说一个JVM就是一个MQClient，这个Client中有多个Producer 多个Consumer
     * @param clientConfig
     * @param rpcHook
     * @return
     */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        /**
         * 对于不同的clientId会创建不同的MQClientInstance
         *
         * 当前类是MQClientManager，在一个JVM内只有一个MQClientManager，因为MQClientManager中如下定义静态对象
         *  private static MQClientManager instance = new MQClientManager();
         *
         *  然后对于不同的clientId 会创建一个MQClientInstance
         *  一个JVM内可能有多个MQClientInstance。
         *  MQClientInstance中有一个producerTable、ConsumerTable、topicRouteTable
         *
         *
         *  经过测试发现如下： 首先 方法getOrCreateMQClientInstance  的参数是 ClientConfig。
         *  在RocketMQ中 config 有几种情况： DefaultMQProducer  DefaultMQPullConsumer 、DefaultMQPushConsumer等等。
         *  一般我们创建一个Producer 或者Consumer 之后 会调用器start方法。
         *
         *  在Producer 和Consumer中都会有一个属性 MQClientInstance mQClientFactory
         *
         *  比如Producer的start方法中：
         *  this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);
         *
         *  Consumer的start方法中：
         *   this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);
         *
         *   Consumer中和Producer中 通过MQClientManager.getInstance() 拿到的MQClientManager是一样 的，整个JVM内只有一个MQClientManager。
         *
         *   但是Consumer和Producer拿到的MQClientInstance是不同的，因为 两者的参数ClientConfig不同的，导致 生成的clientId是不同的。
         *
         *
         * 比如：
         {192.168.102.98@6323@172.102.12.22:9876=org.apache.rocketmq.client.impl.factory.MQClientInstance@5c01e5c0,
         192.168.102.98@DEFAULT@172.102.12.22:9876=org.apache.rocketmq.client.impl.factory.MQClientInstance@90118cf}
         *
         *
         */
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
