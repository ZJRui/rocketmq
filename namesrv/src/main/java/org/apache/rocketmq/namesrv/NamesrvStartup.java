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
package org.apache.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;
import org.slf4j.LoggerFactory;

/***
 *
 * 1，nameServer动态路由发现与剔除机制
 *
 * Broker消息服务器在启动时向所有的NameServer注册；NameServer与每台Broker服务器保持长连接，并间隔30秒检测Broker是否存活，
 * 如果检测到Broker宕机，则从路由表中将其移除。但是路由变化不会马上通知消息生产者。为什么要这样设计？主要是为了降低NameServer实现的负载型，在消息发送端提供容错机制来保证消息发送的高可用性
 *
 *
 *
 * 2， 消息服务器（Broker）根据订阅信息（路由信息）将消息推送到消费者（PUSH模式）或者消息消费者主动向消息服务器拉取消息（PULL 模式）从而实现生产者与消息消费者解耦。
 *
 *
 *
 * 3，消息生产者如何知道消息要发往哪台消息服务器呢？
 *
 * 消息生产者在发送消息之前先从NameServer获取Broker服务器地址列表，然后根据负载均衡算法从列表中选择一台消息服务器进行消息发送。
 *
 *
 *
 *
 *
 * 4，如何避免NameServer的单点故障，提供高可用性呢
 *
 *
 *
 * NameServer本身的高可用可通过部署多台NameServer服务器来实现，但彼此之间互不通信，也就是nameServer服务器之间在某一时刻的数据并不会完全相同，
 * 但这对消息发送不会造成任何影响，这也是RocketMQ nameServer设计的一个亮点，RocketMQNameServer设计追求简单高效。
 *
 *
 *
 * 5，如果某一台消息服务器Broker宕机了，那么生产者如何在不重启服务的情况下感知呢？
 *
 *
 *
 * NameServer主要是为了解决（1,3,5）问题
 *
 *
 */
public class NamesrvStartup {

    private static InternalLogger log;
    private static Properties properties = null;
    private static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {

        try {
            NamesrvController controller = createNamesrvController(args);
            /***
             * start 方法中会调用 controller.initialize
             */
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static NamesrvController createNamesrvController(String[] args) throws IOException, JoranException {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
        //PackageConflictDetect.detectFastjson();

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
            return null;
        }

        final NamesrvConfig namesrvConfig = new NamesrvConfig();
        /**
         * Netty 配置文件
         *
         */
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);
        if (commandLine.hasOption('c')) {
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(new FileInputStream(file));
                properties = new Properties();
                properties.load(in);
                MixAll.properties2Object(properties, namesrvConfig);
                MixAll.properties2Object(properties, nettyServerConfig);

                namesrvConfig.setConfigStorePath(file);

                System.out.printf("load config properties file OK, %s%n", file);
                in.close();
            }
        }

        if (commandLine.hasOption('p')) {
            InternalLogger console = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_NAME);
            MixAll.printObjectProperties(console, namesrvConfig);
            MixAll.printObjectProperties(console, nettyServerConfig);
            System.exit(0);
        }

        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");

        log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

        MixAll.printObjectProperties(log, namesrvConfig);
        MixAll.printObjectProperties(log, nettyServerConfig);

        /**
         * 这里 创建NameServerController
         */
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

        // remember all configs to prevent discard
        controller.getConfiguration().registerConfig(properties);

        return controller;
    }

    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        /**
         *
         * 这里调用了 initialize方法触发NamesrvController的启动：
         * 1，加载kv配置
         * 2，启动NettyServer
         * 3，开启两个定时任务， 每隔10s扫描一次Broker，移除处于不激活状态的Broker
         * 每隔10s打印一次kv配置
         * 4，注册JVM钩子函数
         *
         */
        boolean initResult = controller.initialize();
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        /**
         * 注册 JVM 钩子函数并启动服务器， 以便监昕 Broker 、消息生产者的网络请求 。
         *
         * 这里主要是向读者展示一种常用的编程技巧，如果代码中使用了线程池，一种优雅停
         * 机的方式就是注册一个 JVM 钩子函数， 在 JVM 进程关闭之前，先将线程池关闭 ，及时释
         * 放资源 。
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                /**
                 * JVM关闭之前将线程池关闭，及时释放资源
                 *
                 */
                controller.shutdown();
                return null;
            }
        }));

        controller.start();

        return controller;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}
