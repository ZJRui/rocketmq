---
typora-copy-images-to: images
typora-root-url: images
---





## 第二章

1，nameServer动态路由发现与剔除机制

Broker消息服务器在启动时向所有的NameServer注册；NameServer与每台Broker服务器保持长连接，并间隔30秒检测Broker是否存活，如果检测到Broker宕机，则从路由表中将其移除。但是路由变化不会马上通知消息生产者。为什么要这样设计？主要是为了降低NameServer实现的负载型，在消息发送端提供容错机制来保证消息发送的高可用性



2， 消息服务器（Broker）根据订阅信息（路由信息）将消息推送到消费者（PUSH模式）或者消息消费者主动向消息服务器拉取消息（PULL 模式）从而实现生产者与消息消费者解耦。



3，消息生产者如何知道消息要发往哪台消息服务器呢？

消息生产者在发送消息之前先从NameServer获取Broker服务器地址列表，然后根据负载均衡算法从列表中选择一台消息服务器进行消息发送。





4，如何避免NameServer的单点故障，提供高可用性呢



NameServer本身的高可用可通过部署多台NameServer服务器来实现，但彼此之间互不通信，也就是nameServer服务器之间在某一时刻的数据并不会完全相同，但这对消息发送不会造成任何影响，这也是RocketMQ nameServer设计的一个亮点，RocketMQNameServer设计追求简单高效。



5，如果某一台消息服务器Broker宕机了，那么生产者如何在不重启服务的情况下感知呢？



NameServer主要是为了解决（1,3,5）问题





====================================

NameServer主要作用是为消息生产者和消息消费者提供关于Topic的路由信息，NameServer需要存储路由的基础信息，还要能够管理Broker节点，包括路由注册、路由删除（故障剔除）



------



数据结构解释：



![image-20210916002806925](.\image-20210916002806925.png)

![image-20210916003059161](/image-20210916003059161.png)

![image-20210916003122595](/image-20210916003122595.png)

在这里重点关注的是QueueData， 从上面图片中我们看到  这个Topic有几个BrokerName对应就会有几个QueueData。

因为在每一个BrokerName的master-slave结构中，只有master节点能够写入数据，因此master节点上为该Topic创建的写队列的个数就是QueueData中的writeQueueNums。

slave节点 和master节点都支持对外提供读，因此都有读队列。



问题： slave Broker节点 上读队列的数量是如何确定的？  是不是说如果master上有3个读队列，每一个slave上就会有三个读队列？



---------------

2.3.2 节 继续 2021/091/16





