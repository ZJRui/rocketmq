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

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;

public class RebalanceService extends ServiceThread {
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        /**
         * MQClientInstance 对象中会创建一个 RebalanceService和PullMessageService
         *
         * 然后MQClientInstance的start中会调用  this.rebalanceService.start();
         * 也就启动了RebalanceService 这个线程
         *
         *
         *
         *
         *
         */
        while (!this.isStopped()) {
            /**
             * 设置等待间隔， 默认是20s。RebalanceService 线程默认每隔20秒执行一次 doRebalance。 可以使用rocketmq.client.rebalance.waitInterval来改变默认值
             *
             */
            this.waitForRunning(waitInterval);
            /**
             * *  在run方法中会执行doRebalance，在rebalance中会遍历 MQClientInstance的所有consumer，
             *          *                      * 然后针对每一个consumer调用 consumer.doRebalance();
             *          *                      *
             *          *                      * 不同的consumer ,PullConsumer 和PushConsumer 有不同的doRebalance实现，但他们最终都是调用了rebalanceImpl的doRebalance
             *          *                      *
             *          *                      * RebalanceImpl.rebalanceByTopic(String, boolean)(2 usages)  (org.apache.rocketmq.client.impl.consumer)
             *          *                      *     RebalanceImpl.doRebalance(boolean)  (org.apache.rocketmq.client.impl.consumer)
             *          *                      *         DefaultMQPullConsumerImpl.doRebalance()  (org.apache.rocketmq.client.impl.consumer)
             *          *                      *         DefaultMQPushConsumerImpl.doRebalance()  (org.apache.rocketmq.client.impl.consumer)
             *          *                      *             DefaultMQPushConsumerImpl.resume()  (org.apache.rocketmq.client.impl.consumer)
             *          *                      *                 DefaultMQPushConsumer.resume()  (org.apache.rocketmq.client.consumer)
             *          *                      *                     PushConsumerImpl.resume()  (io.openmessaging.rocketmq.consumer)
             *          *                      *                 MQClientInstance.resetOffset(String, String, Map<MessageQueue, Long>)  (org.apache.rocketmq.client.impl.factory)
             *          *                      *                     DefaultMQPushConsumerImpl.resetOffsetByTimeStamp(long)  (org.apache.rocketmq.client.impl.consumer)
             *          *                      *                     ClientRemotingProcessor.resetOffset(ChannelHandlerContext, RemotingCommand)  (org.apache.rocketmq.client.impl)
             *          *                      *                         ClientRemotingProcessor.processRequest(ChannelHandlerContext, RemotingCommand)  (org.apache.rocketmq.client.impl)
             *          *                      *
             *          mqClientFactory是MQClientInstance对象
             */
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
