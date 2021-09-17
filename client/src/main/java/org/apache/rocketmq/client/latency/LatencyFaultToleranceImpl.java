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

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.common.ThreadLocalIndex;

public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);

    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration) {
        /**
         * 如果 isolation 为 true ，则 使用 30s 作为 computeNotAvailableDuration 方法的参数；如
         * 果 isolation 为 false ，则 使用本次消息发送 时 延作为 computeNotAvailableDuration 方法的
         * 参数，那 computeNotAvailableDuration 的作用 是计算因本次消息发送故障需要将 Broker
         * 规避的时长，也就是接下来多久的时间内该 Broker 将不参与消息发送队列负载。 具体算
         * 法 ：从 latencyMax 数组尾部开始寻找，找 到 第一个比 currentLatency 小的下标， 然后从
         * notAvailableDuration 数组中获取需要规避 的时长，该方法最终调用 LatencyFaultTolerance
         * 的 updateFaultltem 。
         */
        FaultItem old = this.faultItemTable.get(name);
        /**
         * 根据 broker 名称从 缓存表中获 取 Faultitem ，如果找到则更新 Faultltem ，否则创 建
         * Faultltem 。 这里有两个关键点 。
         * 1 )  currentLatency 、 startTimeStamp 被 volatile 修饰。
         * 2  )  startTimeStamp 为当前系统时间加上需要规避的时长。 startTimeStamp 是 判断
         * brok er 当前是否可用 的直接一句，请看 F aultltem# isAv ai lable 方法。
         */
        if (null == old) {
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            faultItem.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);

            old = this.faultItemTable.putIfAbsent(name, faultItem);
            if (old != null) {
                old.setCurrentLatency(currentLatency);
                old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
            }
        } else {
            old.setCurrentLatency(currentLatency);
            old.setStartTimestamp(System.currentTimeMillis() + notAvailableDuration);
        }
    }

    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    @Override
    public String pickOneAtLeast() {
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<FaultItem>();
        while (elements.hasMoreElements()) {
            final FaultItem faultItem = elements.nextElement();
            tmpList.add(faultItem);
        }

        if (!tmpList.isEmpty()) {
            Collections.shuffle(tmpList);

            Collections.sort(tmpList);

            final int half = tmpList.size() / 2;
            if (half <= 0) {
                return tmpList.get(0).getName();
            } else {
                final int i = this.whichItemWorst.getAndIncrement() % half;
                return tmpList.get(i).getName();
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
            "faultItemTable=" + faultItemTable +
            ", whichItemWorst=" + whichItemWorst +
            '}';
    }

    class FaultItem implements Comparable<FaultItem> {
        /**
         * Faultltem ： 失败条目 （ 规避规则条 目） 。
         * 1 )  final  String name 条目唯一键，这里为 brokerName 。
         * 2 )  private volatile Jong currentLatency 本次消息发送延迟 。
         * 3 )  private volatile long startTimestamp 故障规避开始时间 。
         */
        private final String name;
        private volatile long currentLatency;
        private volatile long startTimestamp;

        public FaultItem(final String name) {
            this.name = name;
        }

        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable())
                    return -1;

                if (other.isAvailable())
                    return 1;
            }

            if (this.currentLatency < other.currentLatency)
                return -1;
            else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp)
                return -1;
            else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }

            return 0;
        }

        public boolean isAvailable() {
            /**
             * 根据 broker 名称从 缓存表中获 取 Faultitem ，如果找到则更新 Faultltem ，否则创 建
             * Faultltem 。 这里有两个关键点 。
             * 1 )  currentLatency 、 startTimeStamp 被 volatile 修饰。
             * 2  )  startTimeStamp 为当前系统时间加上需要规避的时长。 startTimeStamp 是 判断
             * brok er 当前是否可用 的直接一句，请看 F aultltem# isAv ai lable 方法。
             */
            return (System.currentTimeMillis() - startTimestamp) >= 0;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof FaultItem))
                return false;

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency())
                return false;
            if (getStartTimestamp() != faultItem.getStartTimestamp())
                return false;
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;

        }

        @Override
        public String toString() {
            return "FaultItem{" +
                "name='" + name + '\'' +
                ", currentLatency=" + currentLatency +
                ", startTimestamp=" + startTimestamp +
                '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
