/*
 * Copyright 2019 The Netty Project
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
package io.netty.util.internal;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * Allow to retrieve the {@link EventExecutor} for the calling {@link Thread}.
 */
public final class ThreadExecutorMap {

    // SNIPPET_START mappings
    private static final FastThreadLocal<EventExecutor> mappings = new FastThreadLocal<EventExecutor>();
    // SNIPPET_END mappings

    private ThreadExecutorMap() { }

    /**
     * Returns the current {@link EventExecutor} that uses the {@link Thread}, or {@code null} if none / unknown.
     */
    public static EventExecutor currentExecutor() {
        return mappings.get();
    }

    /**
     * Set the current {@link EventExecutor} that is used by the {@link Thread}.
     */
    // SNIPPET_START setCurrentEventExecutor
    private static void setCurrentEventExecutor(EventExecutor executor) {
        mappings.set(executor);
    }
    // SNIPPET_END setCurrentEventExecutor

    /**
     * Decorate the given {@link Executor} and ensure {@link #currentExecutor()} will return {@code eventExecutor}
     * when called from within the {@link Runnable} during execution.
     */
    // SNIPPET_START apply_1
    public static Executor apply(final Executor executor, final EventExecutor eventExecutor) {
        ObjectUtil.checkNotNull(executor, "executor");
        ObjectUtil.checkNotNull(eventExecutor, "eventExecutor");
        /*
         * 在执行器外面封装一层，为什么要封装一层？有什么意义？
         * 目的是为了实现对所要执行的任务进行封装。
         */
        return new Executor() {
            @Override
            public void execute(final Runnable command) {
                executor.execute(
                        // 对任务进行封装，在任务执行前，将执行器设置到线程本地存储中。
                        apply(command, eventExecutor));
            }
        };
    }
    // SNIPPET_END apply_1

    /**
     * Decorate the given {@link Runnable} and ensure {@link #currentExecutor()} will return {@code eventExecutor}
     * when called from within the {@link Runnable} during execution.
     */
    // SNIPPET_START apply_2
    public static Runnable apply(final Runnable command, final EventExecutor eventExecutor) {
        ObjectUtil.checkNotNull(command, "command");
        ObjectUtil.checkNotNull(eventExecutor, "eventExecutor");
        return new Runnable() {
            @Override
            public void run() {
                // 在任务执行前将事件执行器设置到线程本地存储中
                setCurrentEventExecutor(eventExecutor);
                try {
                    // 执行任务，这样在任务执行过程中就可以通过线程本地存储获取到执行器。
                    command.run();
                } finally {
                    // 任务执行完后，清空设置到线程本地存储中的事件执行器
                    setCurrentEventExecutor(null);
                }
            }
        };
    }
    // SNIPPET_END apply_2

    /**
     * Decorate the given {@link ThreadFactory} and ensure {@link #currentExecutor()} will return {@code eventExecutor}
     * when called from within the {@link Runnable} during execution.
     */
    public static ThreadFactory apply(final ThreadFactory threadFactory, final EventExecutor eventExecutor) {
        ObjectUtil.checkNotNull(threadFactory, "command");
        ObjectUtil.checkNotNull(eventExecutor, "eventExecutor");
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return threadFactory.newThread(apply(r, eventExecutor));
            }
        };
    }
}
