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
package org.apache.rocketmq.client.consumer.store;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Local storage implementation
 * 广播模式消息消 费进度存储在消 费者本地
 *
 * 消息消费进度的存储，广播模式与消费组无关，集群模式下以主题与消费组为键保存
 * 该主题所有队列的消费进度
 */
public class LocalFileOffsetStore implements OffsetStore {
    /**
     * 消 息 进度存储 目 录， 可以 通 过－Drocketmq.client.
     * localOffsetStoreDir ， 如果未指定 ，则默认为用户主目录 ／ . rocketmq_offsets 。
     */
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
        "rocketmq.client.localOffsetStoreDir",
        System.getProperty("user.home") + File.separator + ".rocketmq_offsets");
    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * 消息客户端。
     */
    private final MQClientInstance mQClientFactory;
    /**
     *  消息消 费组 。
     */
    private final String groupName;
    /**
     * 消息进度存储文件， LOCAL_OFFSET_ STORE  DIR/.rocketmq_ offsets/
     * { mQC!ientFactory.getC!ientld()} / groupNam e/ offse t s .json 。
     */
    private final String storePath;
    /**
     *  消息消费进度（ 内 存） 。
     *
     *  {
     * 	"MessageQueue [topic=kkClearCache, brokerName=localhost, queueId=0]":2099852,
     * 	"MessageQueue [topic=kkClearCache, brokerName=localhost, queueId=1]":2100054,
     * 	"MessageQueue [topic=kkClearCache, brokerName=localhost, queueId=2]":2100121,
     * 	"MessageQueue [topic=kkClearCache, brokerName=localhost, queueId=3]":2100096
     * }
     *
     * 消息消费进度的存储，广播模式与消费组无关，所以上面没有存储ConsumerGroup的信息。
     *
     * 我有一个问题： 创建两个consumer，指定同一个topic但是consumerGroup不同，且都是广播模式，正常情况下这两个consumer应该都能够收到这个topic的信息。
     *
     *  那也就是说我如何判断某一个consumer 有没有消费过这个 topic的信息呢，显然是需要根据topic+consumerGroupname 查找位移。 所以为什么说广播模式与消费组无关呢？
     *
     *
     *
     * 集群模式下以主题与消费组为键保存
     * 该主题所有队列的消费进度。
     *
     */
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public LocalFileOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator +
            this.mQClientFactory.getClientId() + File.separator +
            this.groupName + File.separator +
            "offsets.json";
    }

    @Override
    public void load() throws MQClientException {
        /**
         * 首先看一下 OffsetSerialize Wrapper 内部就是 ConcurrentMap <MessageQueue , A tomicLong>
         * offsetTable 数据结构的封装， readLocakOffset 方法首先从 storeP ath 中尝试加载， 如果从该文
         * 件读取到内容为空，尝试从 storePa出＋” .bak ” 中 尝试加载， 如果还是未找到，则返回 null 。
         * 为了对消息进度有一个更直观的了解 ，消息进度文件存储内容如 图 5-1 5 所示 。
         *
         */
        OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
        if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
            offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

            for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                log.info("load consumer's offset, {} {} {}",
                    this.groupName,
                    mq,
                    offset.get());
            }
        }
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    /**
     *
     * 消息消费进度的存储，广播模式与消费组无关，集群模式下以主题与消费组为键保存 该主题所有队列的消费进度。
     *
     * 对于这句话中 广播模式与消费组无关有些不理解。 我们知道广播模式下 同一个ConsumerGroup内的所有consumer都会收到消息。
     * 在RokcetMQ的实现中 广播模式的消息 使用本地文件存储，LocalFileOffsetStore，这个类里面有一个属性    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<MessageQueue, AtomicLong>();
     * 这里面的数据大致上是这样的结构：
     *   {
     *       	"MessageQueue [topic=topicA, brokerName=localhost, queueId=0]":2099852,
     *      	"MessageQueue [topic=topicA, brokerName=localhost, queueId=1]":2100054,
     *       	"MessageQueue [topic=topicA, brokerName=localhost, queueId=2]":2100121,
     *       	"MessageQueue [topic=topicA, brokerName=localhost, queueId=3]":2100096
     *   }
     *
     * 这个数据会被持久化到文件中，从这个数据来看 offsetTable确实没有存储 consumerGroup信息。
     *
     * 那么问题来了： 同一个JVM内 同一个MQClientInstance下， 创建两个Consumer，这两个consumer都是广播模式消费，topic相同，ConsumerGroup不同。
     * 那么我如何判断某一个Consumer是否消费了某一个消息，或者换句话说我如何计算某一个Consumer的当前消费的offset。
     * 比如consumerA 消费完消息后更新offset为100，ConsumerB消费完之后更新offset为90，这个不会覆盖吗？
     *
     * 又或者consumerA将位移更新为90，consumerB位移更新为100，这不就导致consumerA 读取位移的时候发现是100，从而漏掉部分消息
     *
     *或者是这种实现：
     *
     * @param mq
     * @param type
     * @return
     */
    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    OffsetSerializeWrapper offsetSerializeWrapper;
                    try {
                        offsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return -1;
                    }
                    if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                        AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            this.updateOffset(mq, offset.get(), false);
                            return offset.get();
                        }
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        /**
         * 广播模式消 费进度与消费组没啥关系 ，直接保存 MessageQueue : Offset 。
         *
         * 持久化消息进度就 是将 ConcurrentMap <MessageQueue, AtomicLong>  offsetTable 序
         * 列化到磁盘文件中 。 代码不容易理解，关键是什么时候持久化消息消费进度 。 原来在
         * MQC!ientlnstance 中会启动一个定时任务，默认每 Ss 持久化一 次，可通过 persistConsumer­
         * Offsetlnterval 设置。
         *
         * 消息消费进度的存储，广播模式与消费组无关，集群模式下以主题与消费组为键保存
         * 该主题所有队列的消费进度
         *
         */
        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            if (mqs.contains(entry.getKey())) {
                AtomicLong offset = entry.getValue();
                offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        String jsonString = offsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                MixAll.string2File(jsonString, this.storePath);
            } catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
    }

    @Override
    public void removeOffset(MessageQueue mq) {

    }

    @Override
    public void updateConsumeOffsetToBroker(final MessageQueue mq, final long offset, final boolean isOneway)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());

        }
        return cloneOffsetTable;
    }

    private OffsetSerializeWrapper readLocalOffset() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath);
        } catch (IOException e) {
            log.warn("Load local offset store file exception", e);
        }
        if (null == content || content.length() == 0) {
            return this.readLocalOffsetBak();
        } else {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                return this.readLocalOffsetBak();
            }

            return offsetSerializeWrapper;
        }
    }

    private OffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = null;
        try {
            content = MixAll.file2String(this.storePath + ".bak");
        } catch (IOException e) {
            log.warn("Load local offset store bak file exception", e);
        }
        if (content != null && content.length() > 0) {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                    OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                    + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION),
                    e);
            }
            return offsetSerializeWrapper;
        }

        return null;
    }
}
