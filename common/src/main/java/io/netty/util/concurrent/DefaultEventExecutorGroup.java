/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * Default implementation of {@link MultithreadEventExecutorGroup} which will use {@link DefaultEventExecutor} instances
 * to handle the tasks.
 */
/*
 * 默认的事件执行器组实现，继承了多线程事件执行器组类，其实大部分功能都在父类中实现了，
 *  该类只是对父类进行初始化，同时实现了唯一的newChild方法。
 */
public class DefaultEventExecutorGroup extends MultithreadEventExecutorGroup {
    /**
     * @see #DefaultEventExecutorGroup(int, ThreadFactory)
     */
    public DefaultEventExecutorGroup(int nThreads) {
        this(nThreads, null);
    }

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param threadFactory     the ThreadFactory to use, or {@code null} if the default should be used.
     */
    public DefaultEventExecutorGroup(int nThreads, ThreadFactory threadFactory) {
        this(nThreads, threadFactory, SingleThreadEventExecutor.DEFAULT_MAX_PENDING_EXECUTOR_TASKS,
                RejectedExecutionHandlers.reject());
    }

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param threadFactory     the ThreadFactory to use, or {@code null} if the default should be used.
     * @param maxPendingTasks   the maximum number of pending tasks before new tasks will be rejected.
     * @param rejectedHandler   the {@link RejectedExecutionHandler} to use.
     */
    public DefaultEventExecutorGroup(int nThreads, ThreadFactory threadFactory, int maxPendingTasks,
                                     RejectedExecutionHandler rejectedHandler) {
        super(nThreads, threadFactory, maxPendingTasks, rejectedHandler);
    }

    @Override
    protected EventExecutor newChild(Executor executor, Object... args) throws Exception {
        return new DefaultEventExecutor(this, executor, (Integer) args[0], (RejectedExecutionHandler) args[1]);
    }
}
