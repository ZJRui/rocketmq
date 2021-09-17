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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;
    /**
     * latency Max ，根据 currentLatency 本次消息 发送延迟，从 latency Max 尾部向前找到
     * 第一个比 currentLatency 小的索引 index，如果没有找到，返回 0 。 然后 根据这个 索 引从
     * notAvailableDuration 数组中取出对应的时间 ，在这个时长 内 ， Broker 将设置为不可用 。
     */

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        /**
         *  /**
         *                  *
         *                  *根据路由信息选择消息队列，返回的消息队列按照 broker 、序号排序 。 举例说明，如
         *                  * 果 topicA 在 broker-a, broker” b 上分别创建了 4 个队列 ， 那么返回的消息队列：［｛ “ broker-
         *                  * Name ”:” broker-a ”,” queueld ”:O}, {“ brokerName ”:” broker-a ”,” queue Id ” .1},
         *                  * {“brokerName ”:” broker-a ’,,” queueld ”:2}, ｛飞rokerName ”．” broker-a ”，” queueld ” ：匀 ，
         *                  * {“ brokerName ” . ” broker-b ”,” queueld ”:0}, {“ brokerName ”.” broker-
         *                  * b ” J’ queueld ” ·I }, {“ broker Name ”: ” broker-b ”,” queueld ”:2},  {“ brokerName ” :
         *                  * "  broker-b ” ， ” queueld ” ： 3 汀 ，那 RocketMQ 如何选择消息队列呢？
         *                  *
         *                  *首先消息发送端采用重试机制 ，由 retryTimesWhenSendFailed 指 定 同步方式重试次
         *                  * 数 ，异步重试机制在收到消息发送结构后执行回调之前进行重试。 由 retryTimesWhenSendAsyncFailed 指定，接下来就是循环执行 ， 选择消息队列 、发送消息，发送成功则返回，收
         *                  * 到异常则重试。 选择消息队列有两种方式 。
         *                  * 1 )  sendLatencyFaultEnable=false ，默认不启用 Broker故障延迟机制 。
         *                  * 2 )  sendLatencyFaultEnable=true ，启用 Broker 故障延迟机制 。
         *                  *
         *                  *
         *
         *
         */
        if (this.sendLatencyFaultEnable) {
            try {
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                /**
                 * ）根据对消息队列进行轮询获取一个消息队列 。
                 */
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    /**
                     * ）验证该消息队列 是否 可用， latencyFaultTolerance.isAvailable(mq.getBrokerName())
                     * 是关键。
                     */
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }

                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }
        /**
         * sendLatency FaultEnable=false ，调用 TopicPublishlnfo的SelectOneMessageQueue 。
         *
         * 注意TopicPublisInfo 和 MQFaultStrategy 都有selectOneMessageQueue方法。
         * 但是Producer中使用的是MQFaultStrategy对象的selectOneMessageQueue
         *
         * MQFaultStrategy 内部有一个属性latencyFaultTolerance 表示是否开启broker故障延迟机制。如果
         * 该属性为false，则表示不开启，MQFaultStrategy对象会直接使用TopicPublisInfo的selectOneMessageQueue方法
         *
         * 如果该属性为true，则MQFaultStrategy会执行Broker故障延迟机制。
         *
         * 那么我们要问什么是故障延迟机制？ 这就要分析TopicPublisInfo的selectOneMessageQueue方法存在什么缺陷，这个方法在一次消息发送过程中能成功规避故障的 Broker，但如果 Broker 若机，由于路
         * 由算法中的消息队列 是按 Broker 排序 的，如果上一次根据路由算法选择的是若机的 Broker
         * 的第一个队列 ，那么随后的下 次选择的是若机 Broker 的第二个队列，消息发送很有可能会
         * 失败，再次引发重试，带来不必要的性能损耗，那么有什么方法在一次消息发送失败后，
         * 暂时将该 Broker 排除在消息队列选择范围外呢？或许有朋友会问， Broker 不可用后 ，路由
         * 信息中为什么还会包含该 Brok町的路由信息呢？其实这不难解释：首先， NameServer 检
         * 测 Broker 是否可用是有延迟的，最短为一次心跳检测间 隔（ 1 Os ）； 其次， NameServer 不会
         * 检测到 Broker 岩机后马上推送消息给消息生产者，而是消息生产者每隔 30s 更新一次路由
         * 信息，所以消息生产者最快感知 Broker 最新的路由信息也需要 30s 。 如果能引人一种机制，
         * 在 Broker 若机期间，如果一次消息发送失败后，可以将该 Broker 暂时排除在消息队列的选
         * 择范围中 。
         *
         *
         */
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     *
     *                          * 上述代码如果发送过程中抛出了异常，调用 DefaultMQProducerlmpl#updateFaultlt巳m,
     *                          * 该方法则直接调用 MQFaultStrategy#updateFaultltem 方法，关注一下各个参数的含义 。
     *                          *
     *                          * 第一个参数 ： broker 名称。
     *                          * 第二个参数：本次消息发送延迟时间 currentLatency 。
     *                          * 第三个参数 ： isolation ，是否隔离，该参数的含义如果为 true ，则使用默认时长 30s 来
     *                          * 计算 Broker 故 障规避 时长 ，如果为 false ， 则使用本次消息发送延迟时间来计算 Broker 故障规避时长
     *                          *
     *
     *     --------------------
     *             * broker故障延迟机制：
     *                          * 在不启用故障延迟的情况下会有什么问题呢？ 发送消息的时候我们需要选择一个MessageQueue，如果因为Broker宕机导致第一次发送失败了，
     *                          * 那么第二次选择MessageQueue的时候要规避同一个Broker的MessageQueue。 在RocketMQ的 selectOneMessageQueue(lastBrokerName);
     *                          * 方法中进行选择MessageQueue，其中参数lastBrokerName表示上一次发送失败的BrokerName，因此第一次发送的时候为null,
     *                          * 第二次发送的时候为第一次发送失败的MessageQueue所在的BrokerName， 如果第二次发送失败了，那么我们第三次选择的时候
     *                          * lastBrokerName就是第二法发送失败的BrokerName，那么这个时候第三次选择MessageQueue的过程中无法规避选中第一次发送失败的BrokerName的MessageQueue。
     *                          *
     *                          * 消息发送很有可能会失败，再次引发重试，带来不必要的性能损耗，那么有什么方法在一次消息发送失败后，暂时将该 Broker 排除在消息队列选择范围外呢？
     *
     *                          * 或许有朋友会问， Broker 不可用后 ，路由信息中为什么还会包含该 Brok町的路由信息呢？其实这不难解释：首先，
     *                          * NameServer 检测 Broker 是否可用是有延迟的，最短为一次心跳检测间 隔（ 1 0s ）； 其次， NameServer 不会检测到 Broker
     *                          * 岩机后马上推送消息给消息生产者，而是消息生产者每隔 30s 更新一次路由信息，所以消息生产者最快感知 Broker 最新的路由信息也需要 30s 。
     *                          * 如果能引人一种机制，在 Broker 若机期间，如果一次消息发送失败后，可以将该 Broker 暂时排除在消息队列的选择范围中 。
     *                          *
     *                          * 消息发送失败的时候创建一条失败记录， 指定broker名称，本次消息发送延迟时间（本次消息失败时的时间减去发送前开始时间）
     *                          * 第三个参数isolation 表示是否隔离，该参数的含义如果为true，则使用默认时长30s来计算broker故障规避时长；如果为false，则使用本次消息发送
     *                          * 延迟时间来计算Broker故障规避时长。
     *                          *
     *                          * 故障延迟机制的原理就是：消息发送失败的是时候 创建一条发送失败记录，记录broker和故障延迟时间，指定该broker在未来的故
     *                          * 障延迟时间内不能发送消息。这个故障延迟时间 可以使用固定的30秒，也可以根据消息发送开始时间到消息发送失败时的差值时间 来计
     *                          * 算一个时间作为故障延迟时间，差值时间越大计算得到的故障延迟时间越大，故障延迟时间就是broker要规避 的时长。接下来多久的时间内该 Broker 将不
     *                          * 参与消息发送队列负载。此时消息发送时会顺序选择MessageQueue，然后判断这个MessageQueue是否可用，判断的依据就是根据发送失败记录。
     *                          *
     *
     *
     * @param brokerName
     * @param currentLatency
     * @param isolation
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        /**
         * 如果 isolation 为 true ，则 使用 30s 作为 computeNotAvailableDuration 方法的参数；如
         * 果 isolation 为 false ，则 使用本次消息发送 时 延作为 computeNotAvailableDuration 方法的
         * 参数，那 computeNotAvailableDuration 的作用 是计算因本次消息发送故障需要将 Broker
         * 规避的时长，也就是接下来多久的时间内该 Broker 将不参与消息发送队列负载。 具体算
         * 法 ：从 latencyMax 数组尾部开始寻找，找 到 第一个比 currentLatency 小的下标， 然后从
         * notAvailableDuration 数组中获取需要规避 的时长，该方法最终调用 LatencyFaultTolerance
         * 的 updateFaultltem 。
         */
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
