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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;

public class ManyPullRequest {
    private final ArrayList<PullRequest> pullRequestList = new ArrayList<>();

    public synchronized void addPullRequest(final PullRequest pullRequest) {
        this.pullRequestList.add(pullRequest);
    }

    public synchronized void addPullRequest(final List<PullRequest> many) {
        this.pullRequestList.addAll(many);
    }

    /**
     *  值
     * 得注意的是该方法使用了 synchronized ，说明该数据结 构 会存在并发访问，该属性是
     * Pul!Reque stH old Serv ic e 线程的私有属性，会存在并发？答案是存在并发，下文重点提到的
     * ReputMessageServ ice 内 部将持有 PullRequestHoldService ， 也会唤醒挂起线程从而执行消息
     * 拉取尝试
     * @return
     */
    public synchronized List<PullRequest> cloneListAndClear() {
        if (!this.pullRequestList.isEmpty()) {
            List<PullRequest> result = (ArrayList<PullRequest>) this.pullRequestList.clone();
            this.pullRequestList.clear();
            return result;
        }

        return null;
    }
}
