/*
 * Copyright 2013 The Netty Project
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

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueueNode;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class ScheduledFutureTask<V> extends PromiseTask<V> implements ScheduledFuture<V>, PriorityQueueNode {
    // set once when added to priority queue
    private long id;

    // 调度延迟
    private long deadlineNanos;
    /* 0 - no repeat, >0 - repeat at fixed rate, <0 - repeat with fixed delay */
    // 调度周期，如果是0说明不是周期性调度
    private final long periodNanos;

    private int queueIndex = INDEX_NOT_IN_QUEUE;

    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Runnable runnable, long nanoTime) {

        super(executor, runnable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Runnable runnable, long nanoTime, long period) {

        super(executor, runnable);
        deadlineNanos = nanoTime;
        periodNanos = validatePeriod(period);
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Callable<V> callable, long nanoTime, long period) {

        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = validatePeriod(period);
    }

    ScheduledFutureTask(AbstractScheduledEventExecutor executor,
            Callable<V> callable, long nanoTime) {

        super(executor, callable);
        deadlineNanos = nanoTime;
        periodNanos = 0;
    }

    private static long validatePeriod(long period) {
        if (period == 0) {
            throw new IllegalArgumentException("period: 0 (expected: != 0)");
        }
        return period;
    }

    ScheduledFutureTask<V> setId(long id) {
        if (this.id == 0L) {
            this.id = id;
        }
        return this;
    }

    @Override
    protected EventExecutor executor() {
        return super.executor();
    }

    public long deadlineNanos() {
        return deadlineNanos;
    }

    void setConsumed() {
        // Optimization to avoid checking system clock again
        // after deadline has passed and task has been dequeued
        if (periodNanos == 0) {
            // 必须确保当前时间大于等于任务的截止时间，不能提前被执行
            assert scheduledExecutor().getCurrentTimeNanos() >= deadlineNanos;
            deadlineNanos = 0L;
        }
    }

    public long delayNanos() {
        return delayNanos(scheduledExecutor().getCurrentTimeNanos());
    }

    static long deadlineToDelayNanos(long currentTimeNanos, long deadlineNanos) {
        return deadlineNanos == 0L ? 0L : Math.max(0L, deadlineNanos - currentTimeNanos);
    }

    public long delayNanos(long currentTimeNanos) {
        return deadlineToDelayNanos(currentTimeNanos, deadlineNanos);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(delayNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (this == o) {
            return 0;
        }

        ScheduledFutureTask<?> that = (ScheduledFutureTask<?>) o;
        // 先按照时间先后来排序
        long d = deadlineNanos() - that.deadlineNanos();
        if (d < 0) {
            return -1;
        } else if (d > 0) {
            return 1;
        } else if (id < that.id) { // 如果时间都一样，则按照id的大小排序
            return -1;
        } else {
            assert id != that.id;
            return 1;
        }
    }

    @Override
    public void run() {
        // 确保在时间循环中
        assert executor().inEventLoop();
        try {
            // 如果还没有到调度任务的截止时间
            if (delayNanos() > 0L) {
                // Not yet expired, need to add or remove from queue
                if (isCancelled()) { // 如果任务已被取消
                    // 将其从调度队列中移除
                    scheduledExecutor().scheduledTaskQueue().removeTyped(this);
                } else {
                    // 添加到调度队列中，因为还没到应该执行的时间，会重新为任务分配一个id
                    scheduledExecutor().scheduleFromEventLoop(this);
                }
                return;
            }

            // 执行到这里说明已经到了任务的执行时机了

            if (periodNanos == 0) { // 如果是单次调度任务
                // 将任务设置为不可取消的
                if (setUncancellableInternal()) {
                    // 运行任务
                    V result = runTask();
                    // 设置执行结果
                    setSuccessInternal(result);
                }
            } else { // 固定周期调度或者固定延迟调度
                // check if is done as it may was cancelled
                if (!isCancelled()) { // 如果任务没被取消
                    // 运行任务
                    runTask();
                    if (!executor().isShutdown()) { // 如果执行器还没被关闭
                        if (periodNanos > 0) { // 固定周期调度
                            // 加上固定周期
                            deadlineNanos += periodNanos;
                        } else { // 固定延迟调度
                            // 加上固定延迟（虽然是减，但是periodNanos为负）
                            deadlineNanos = scheduledExecutor().getCurrentTimeNanos() - periodNanos;
                        }
                        // 如果任务没被取消
                        if (!isCancelled()) {
                            // 添加到调度队列中
                            scheduledExecutor().scheduledTaskQueue().add(this);
                        }
                    }
                }
            }
        } catch (Throwable cause) {
            setFailureInternal(cause);
        }
    }

    private AbstractScheduledEventExecutor scheduledExecutor() {
        return (AbstractScheduledEventExecutor) executor();
    }

    /**
     * {@inheritDoc}
     *
     * @param mayInterruptIfRunning this value has no effect in this implementation.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean canceled = super.cancel(mayInterruptIfRunning);
        if (canceled) {
            scheduledExecutor().removeScheduled(this);
        }
        return canceled;
    }

    boolean cancelWithoutRemove(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }

    @Override
    protected StringBuilder toStringBuilder() {
        StringBuilder buf = super.toStringBuilder();
        buf.setCharAt(buf.length() - 1, ',');

        return buf.append(" deadline: ")
                  .append(deadlineNanos)
                  .append(", period: ")
                  .append(periodNanos)
                  .append(')');
    }

    @Override
    public int priorityQueueIndex(DefaultPriorityQueue<?> queue) {
        return queueIndex;
    }

    @Override
    public void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i) {
        queueIndex = i;
    }
}
