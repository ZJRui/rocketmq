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

/**
 * $Id: QueueData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.route;

import java.util.Queue;

public class QueueData implements Comparable<QueueData> {
    /**
     * QueueData中存在BrokerName属性
     * 实际上一个在集群中两个BrokerName，则这个Topic就会有两个QueueData，然后每一个QueueData中存在brokerName属性
     *
     * RouteInfoManager#createAndUpdateQueueData(java.lang.String, org.apache.rocketmq.common.TopicConfig)
     * 在createAndUpdateQueueData方法中 会为这个Topic的每一个BrokerName创建一个QueueData。
     * 如果这个BrokerName不存在QueueData则创建一个，如果已经存在QueueData则更新，也就是说一个Topic的一个BrokerName对应一个QueueData
     *
     * 在一个BrokerName中可以有多个broker节点构成主从结构
     */
    private String brokerName;
    /**
     * 顶一个一个BrokerName内有几个可读队列 有几个可写队列。
     *
     * 对于可读队列readQueueNums的使用是在 MQClientInstance#topicRouteData2TopicSubscribeInfo在这个方法中，会取出该BrokerName中可读队列的数量，为每一个可读队列创建一个
     * MessageQueue，然后将创建的 MessageQueue通过Consumer的updateTopicSubscribeInfo方法更新到consumer的订阅列表中。因为Consumer作为读取消息，是读取可读队列
     *
     * 对于writeQueueNums ，是在MQClientInstance的topicRouteData2TopicPublishInfo 方法中读取BrokerName的可读队列的数量，为每一个可读队列创建一个MessageQueue，
     * 然后将MessageQueue 使用 producer的 updateTopicPublishInfo更新到Producer的topicPublishInfoTable ，毕竟Producer作为生产者他的消息都是要发送到可写队列中。
     *
     *
     */
    private int readQueueNums;
    private int writeQueueNums;
    /**
     * 读写权限
     */
    private int perm;
    /**
     * topic同步标记，具体参考TopicSysFlag
     */
    private int topicSynFlag;
    public QueueData(){

    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSynFlag() {
        return topicSynFlag;
    }

    public void setTopicSynFlag(int topicSynFlag) {
        this.topicSynFlag = topicSynFlag;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSynFlag;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (perm != other.perm)
            return false;
        if (readQueueNums != other.readQueueNums)
            return false;
        if (writeQueueNums != other.writeQueueNums)
            return false;
        if (topicSynFlag != other.topicSynFlag)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + perm + ", topicSynFlag=" + topicSynFlag
            + "]";
    }

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
