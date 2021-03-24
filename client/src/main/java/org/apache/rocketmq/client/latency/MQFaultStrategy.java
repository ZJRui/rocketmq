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
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;

/**
 * 消息失败策略，延迟实现的门面类
 */
public class MQFaultStrategy {
    private final static Logger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

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

    //lastBrokerName上一次选择的执行发送消息失败的broker
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //sendLatencyFaultEnable=false：默认不启用Broker故障延迟机制
        /**
         * 什么是故障延迟机制？ 比如一个Topic有4个队列，有两个Broker，所以每一个Broker上存在两个队列， 这个消息被发送到哪个队列是
         * 根据ThreadLocalIndex生成的数字对队列取模得到的队列， 如果某次选择的队列其所在的Broker是宕机的，那么下一次选择队列的时候还有可能
         * 选择到宕机Broker的队列。 因此引入了一种机制，在broker宕机期间，如果一次消息发送失败后，可以将该Broker暂时排除在消息队列的选择范围中
         *
         */
        if (this.sendLatencyFaultEnable) {
            try {
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //验证该消息队列是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        //这个地方为什么判断equal的是时候返回mq，不是说排除这个队列吗，为什么这个地方是equal
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                //尝试从规避的Broker中选择一个额可用的Broker，如果没有找到返回null
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
        //sendlatencyFaultEnable=true，启用Broker故障延迟机制
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 在updateFaultItem中 使用到了computeNotAvailableDuration 方法来计算 当前broker的规避时长（也就是在未来多长时间内不可用）
     * (1)发送之前记录start，发送之后记录end，同时如果出现了异常也需要记录end。
     * （2）如果是发送失败了，设置isolation为true，此时传入computeNotAvailableDuration中的参数就是 30000,此时
     * compute返回值是600000
     * （3）发送成功的情况下，发送的时间越短，规避时长越小， 发送时间越大，规避时间越大。因此我们这里使用了两个数组
     * private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
     * private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};
     * <p>
     * latencyMax递增，我们从末尾开始 往前找，找到第一个小于当前发送时长的 位置，然后位置越靠前，则说明当前发送的时间越小，
     * 此时我们得到位置i，然后取出notAvailableDuration中的位置i的数据作为规避时长
     * 在发送失败的情况下，使用30秒作为compute的参数，compute返回的是60000，也就是60秒作为规避时长
     *
     * @param brokerName
     * @param currentLatency
     * @param isolation
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //得到规避时长之后更新 数据
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

    public static void main(String[] args) {
        MQFaultStrategy strategy = new MQFaultStrategy();
        long l = strategy.computeNotAvailableDuration(30000);
        System.out.println(l);
    }
}
