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

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.utils.ThreadUtils;

public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    private final MQClientInstance mQClientFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "PullMessageServiceScheduledThread");
            }
        });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            /**
             * 延迟将请求放置到队列中
             */
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    /**
     * 将pull请求放置到队列中
     * @param pullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * 消息消费有两种模式： 所谓拉模式，是消费端主动发起拉请求，而推模式是消息到达消息服务器后，推送给消息消费者
     *
     *  RocketMQ 消息推模式的实现基于拉模式，在拉模式上包装一层，一个拉取任务完成后开始下一个拉取任务 。
     */
    private void pullMessage(final PullRequest pullRequest) {
        /**
         * 找导对应的consumer
         */
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            /**
             * 注意pull Message取出consumer将consumer强制转为 PushConsumer，这里为什么可以强转？
             *
             * 根据消 费组名 从 MQClientlnstance 中获取消费者内部实现类 MQConsumerlnner ，令人
             * 意外的 是这里将 consumer 强制转换为 DefaultMQPushConsumerlmpl ，也就是 PullMessage
             * Service ，该线程只为 PUSH 模式服务， 那拉模式如何拉取消息呢？其实 细想也不难理解，
             * PULL 模式 ， RocketMQ 只需要提供拉取消息 API 即可， 具体由应用程序显示调用拉取 API 。
             *
             * 我觉得：RocketMQ的推模式不是标准意义上的推模式。
             *
             * 可以这样理解推拉模式： 对于拉模式需要客户端主动调用API获取消息， 对于推模式，客户端无感知能够拿去到消息，而不需要自己主动调用API，rocketMQ
             * 的推模式是通过一个线程 不断的拉取数据 将这个数据伪装成broker推送过来的。
             *
             * RocketMQ 并没有真正实现推模式，而是消费者主动向消息服务器拉取消息， RocketMQ 推模式是循环向消息服务端发送消息拉取请求，《RocketMQ技术内幕 5.4.3 消息拉取长轮询机制分析》
             *
             *=======================
             * 对于PushConsumer ，我们在创建Consumer的时候没有指定topic，但是通过PushConsumer的subscribe方法指定了主题。 PullConsumer中并没有subscribe方法
             *
             DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("testVA");
             consumer.setNamesrvAddr("192.168.102.73:9876");
             consumer.setMessageModel(MessageModel.BROADCASTING);
             consumer.subscribe("topicTestVA", "*");

             *
             * ================
             *  对于拉模式 如何体现客户端 主动拉取呢？
             *         //创建PullConsumer的时候没有指定topic，而是在创建MessageQueue的时候才指定了Topic
             *         DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");
             *         consumer.setNamesrvAddr("127.0.0.1:9876");
             *         consumer.start();
             *
             *         try {
             *             MessageQueue mq = new MessageQueue();
             *             mq.setQueueId(0);
             *             mq.setTopic("TopicTest3");
             *             mq.setBrokerName("vivedeMacBook-Pro.local");
             *
             *             long offset = 26;
             *
             *             long beginTime = System.currentTimeMillis();
             *             PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, offset, 32);
             *             System.out.printf("%s%n", System.currentTimeMillis() - beginTime);
             *             System.out.printf("%s%n", pullResult);
             *         } catch (Exception e) {
             *             e.printStackTrace();
             *         }
             *
             * 咋这段代码中我们创建了一个Consumer，然后创建了一个MessageQueue，手动调用API拉取这个MessageQUeue 得到一个PullResult
             *
             * ================================
             *
             * 另外 不管是PullConsumer 还是PushConsumer 他们的start方法都会调用 MQClientInstance的start方法，
             * 在MQClientInstance的start方法内会启动 PullMessageService线程，也就是说不管是Pull 还是Push。 PullMessageService都会存在。
             * 同时启动的Consumer 还会 注册到MQClientInstance， key是ProducerGroup，value是Consumer，也就是说consumerTable中既有
             *
             *MQClientInstance.consumerTable.putIfAbsent(group, consumer);
             *=======================
             * 如何保证 下面的强转不会出错？ 也就是如何保证从consumerTable中 根据group取出的 Consumer一定是 PushConsumer？
             *
             *  （1）问题一： 我们能创建一个PullConsumer 和一个PushConsumer，但是这两个consumer的group相同吗？ 答案不可以，一个ConsumerGroup在MQClientInstance中只能有一个Consumer
             *   （2）pullMessage 的请求参数 PullRequest是怎么来的？ 也就是PullRequest对象是怎么来的 主要有两个地方
             *                  * (1)一个是在RocketMQ根据PullRequest拉取任务执行完一次消息拉取任务后，又将PullRequest对象放入到PullRequestQueue
             *                  * （2）第二个是在RebalanceImpl中创建， 这里是PullRequest对象真正创建的地方org.apache.rocketmq.client.impl.consumer.RebalanceImpl#updateProcessQueueTableInRebalance(java.lang.String, java.util.Set, boolean)
             *
             *     在RebalanceImpl的updateProcessQueueTableInRebalance 方法中会创建PullRequest， 将这些PullRequest手机起来 然后调用  this.dispatchPullRequest(pullRequestList);其中this就是RebalanceImpl对象
             *
             *
             *     对于RebalancePushImpl的dispatchPullRequest 实现如下， 使用持有的PushConsumer的executePullRequest 方法 将每一个PullRequest放置到 PullMessageService的队列中
             *       @Override
             *     public void dispatchPullRequest(List<PullRequest> pullRequestList) {
             *         for (PullRequest pullRequest : pullRequestList) {
             *             this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
             *             log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
             *         }
             *     }
             *
             *     对于RebalancePullImpl的dispatchPullRequest的实现如下： 没有做任何处理， 这就保证了 放置到PullMessageService的队列中的PullRequest 全部都是 RebalancePushImpl的dispatchPullRequest 方法使用PushConsumer 放入的 PullRequest
             *      @Override
             *     public void dispatchPullRequest(List<PullRequest> pullRequestList) {
             *     }
             *
             *
             *
             */
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;

            /**
             * PullRequest中持有MessageQueue和ProcessQueue，在pullmessage 方法中 会更新ProcessQueue的上次拉取时间为当前时间
             */
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        /**
         * 消息消费有两种模式： 所谓拉模式，是消费端主动发起拉请求，而推模式是消息到达消息服务器后，推送给消息消费者。
         *
         * 消息啦模式主要由客户端手动调用消息拉取API，消息推模式是消息服务器主动将消息推送到消息消费端。
         *
         */
        while (!this.isStopped()) {
            try {
                /**
                 * 线程不断取出pull请求然后 拉消息
                 * 那 PullRequest 是什么时候添加的呢？
                 * 主要有两个地方
                 * (1)一个是在RocketMQ根据PullRequest拉取任务执行完一次消息拉取任务后，又将PullRequest对象放入到PullRequestQueue
                 * （2）第二个是在RebalanceImpl中创建， 这里是PullRequest对象真正创建的地方org.apache.rocketmq.client.impl.consumer.RebalanceImpl#updateProcessQueueTableInRebalance(java.lang.String, java.util.Set, boolean)
                 */
                PullRequest pullRequest = this.pullRequestQueue.take();
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}
