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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    /**
     * TopicPublishInfo 对象的创建是在 topicRouteData2TopicPublishInfo 方法中，在改方法中会组装TopicPublishInfo中的各种数据属性
     * 是否是顺序消息 。
     */
    private boolean orderTopic = false;
    private boolean haveTopicRouterInfo = false;
    /**
     * 该主题队列的消息队列 。
     */
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    /**
     * 每选择一次消息 队列， 该值会自增 l ，如果 I nte ger.MAX_VALUE,
     * 则重置为 0 ，用于选择消息 队列 。
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    /**
     * TopicRouteData中存储了topic的所有信息，该信息是从nameServer中获取到的。
     * 首先Topic 下划分BrokerName，一个Topic可以有两个BrokerName， 每一个BrokerName内可以有多个Broker节点，这些broker节点构成主从结构。
     * 每一个brokerName对应一个QueueData， topic有两个BrokerName则 TopicRouteData中的queueDatas 大小就是2.
     *
     * 一个BrokerName中的所有broker节点信息构成一个BrokerData。因此TopicRouteData中的brokerDatas 的大小也是取决于该topic的brokerName数量
     */
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        /**
         * 首先在一次消息发送过程中，可能会多次执行选择消息队列这个方法， lastBrokerName
         * 就是上 一 次选择的执行发送消息失败的 Broker 。
         *
         *  第一次执行消息队列选择时，lastBrokerName 为 null ，此时 直接用 sendWhichQueue 自增再获取值 ， 与当前路由 表 中消息
         * 队列个数取模， 返回该位置 的 MessageQueue(selectOneMessageQueue （） 方法），如果消息发
         * 送再失败的话 ， 下次进行消息队列选择时规避上次 MesageQueue 所在的 Broker， 否则还是
         * 很有可能再次失败.
         *
         * 该算法在一次消息发送过程中能成功规避故障的 B roker，但如果 Bro ker 若机，由于路
         * 由算法中的消息队列 是按 Broker 排序 的，如果上一次根据路由算法选择的是若机的 Broker
         * 的第一个队列 ，那么随后的下 次选择的是若机 Broker 的第二个队列，消息发送很有可能会
         * 失败，再次引发重试，带来不必要的性能损耗，那么有什么方法在一次消息发送失败后，
         * 暂时将该 Broker 排除在消息队列选择范围外呢？或许有朋友会问， Broker 不可用后 ，路由
         * 信息中为什么还会包含该 Brok町的路由信息呢？其实这不难解释：首先， NameServer 检
         * 测 Broker 是否可用是有延迟的，最短为一次心跳检测间 隔（ 1 Os ）； 其次， NameServer 不会
         * 检测到 Broker 岩机后马上推送消息给消息生产者，而是消息生产者每隔 30s 更新一次路由
         * 信息，所以消息生产者最快感知 Broker 最新的路由信息也需要 30s 。 如果能引人一种机制，
         * 在 Broker 若机期间，如果一次消息发送失败后，可以将该 Broker 暂时排除在消息队列的选
         * 择范围中 。这就是Producer端的Broker故障延迟机制
         *
         *
         */
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int index = this.sendWhichQueue.getAndIncrement();
                int pos = Math.abs(index) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);

                /**
                 *   *    考虑到lastBrokerName为null，且MQFaultStrategy的sendLatencyFaultEnable 属性为false的情况下，会执行TopicPublisInfo的selectOneMessageQueue选择队列
                 *                  *     第一次执行selectOneMessageQueue时lastBrokerName为null，此时选择了某一个MessageQueue（MessageQueue中有brokerName）进行发送，结果发送失败了，
                 *                  *     然后第二次执行electOneMessageQueue 传入了第一次选择失败的brokerName。然后我们会根据brokerName规避 在第一次消息发送过程中故障的broker
                 */
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
