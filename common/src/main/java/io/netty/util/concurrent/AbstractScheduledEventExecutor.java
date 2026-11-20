/*
 * Copyright 2015 The Netty Project
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
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PriorityQueue;

import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for {@link EventExecutor}s that want to support scheduling.
 */
/*
 * 除了提供对ScheduleExecutorService的实现外，还定义了周期性任务队列和周期性任务调度功能方法的实现。
 */
public abstract class AbstractScheduledEventExecutor extends AbstractEventExecutor {
    private static final Comparator<ScheduledFutureTask<?>> SCHEDULED_FUTURE_TASK_COMPARATOR =
            new Comparator<ScheduledFutureTask<?>>() {
                @Override
                public int compare(ScheduledFutureTask<?> o1, ScheduledFutureTask<?> o2) {
                    return o1.compareTo(o2);
                }
            };

    private static final long START_TIME = System.nanoTime();

    static final Runnable WAKEUP_TASK = new Runnable() {
       @Override
       public void run() { } // Do nothing
    };

    // 任务调度队列，按照所要执行的时间的先后来排序
    PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue;

    long nextTaskId;

    protected AbstractScheduledEventExecutor() {
    }

    // SNIPPET_START constructor_2
    protected AbstractScheduledEventExecutor(EventExecutorGroup parent) {
        super(parent);
    }
    // SNIPPET_END constructor_2

    /**
     * Get the current time in nanoseconds by this executor's clock. This is not the same as {@link System#nanoTime()}
     * for two reasons:
     *
     * <ul>
     *     <li>We apply a fixed offset to the {@link System#nanoTime() nanoTime}</li>
     *     <li>Implementations (in particular EmbeddedEventLoop) may use their own time source so they can control time
     *     for testing purposes.</li>
     * </ul>
     */
    protected long getCurrentTimeNanos() {
        return defaultCurrentTimeNanos();
    }

    /**
     * @deprecated Use the non-static {@link #getCurrentTimeNanos()} instead.
     */
    @Deprecated
    protected static long nanoTime() {
        return defaultCurrentTimeNanos();
    }

    static long defaultCurrentTimeNanos() {
        return System.nanoTime() - START_TIME;
    }

    static long deadlineNanos(long nanoTime, long delay) {
        // 当前时间加上延迟
        long deadlineNanos = nanoTime + delay;
        // Guard against overflow
        return deadlineNanos < 0 ? Long.MAX_VALUE : deadlineNanos;
    }

    /**
     * Given an arbitrary deadline {@code deadlineNanos}, calculate the number of nano seconds from now
     * {@code deadlineNanos} would expire.
     * @param deadlineNanos An arbitrary deadline in nano seconds.
     * @return the number of nano seconds from now {@code deadlineNanos} would expire.
     */
    protected static long deadlineToDelayNanos(long deadlineNanos) {
        return ScheduledFutureTask.deadlineToDelayNanos(defaultCurrentTimeNanos(), deadlineNanos);
    }

    /**
     * The initial value used for delay and computations based upon a monatomic time source.
     * @return initial value used for delay and computations based upon a monatomic time source.
     */
    protected static long initialNanoTime() {
        return START_TIME;
    }

    PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue() {
        if (scheduledTaskQueue == null) {
            // 创建任务调度队列
            scheduledTaskQueue = new DefaultPriorityQueue<ScheduledFutureTask<?>>(
                    SCHEDULED_FUTURE_TASK_COMPARATOR,
                    // Use same initial capacity as java.util.PriorityQueue
                    11);
        }
        return scheduledTaskQueue;
    }

    private static boolean isNullOrEmpty(Queue<ScheduledFutureTask<?>> queue) {
        return queue == null || queue.isEmpty();
    }

    /**
     * Cancel all scheduled tasks.
     *
     * This method MUST be called only when {@link #inEventLoop()} is {@code true}.
     */
    protected void cancelScheduledTasks() {
        assert inEventLoop();
        PriorityQueue<ScheduledFutureTask<?>> scheduledTaskQueue = this.scheduledTaskQueue;
        if (isNullOrEmpty(scheduledTaskQueue)) {
            return;
        }

        final ScheduledFutureTask<?>[] scheduledTasks =
                scheduledTaskQueue.toArray(new ScheduledFutureTask<?>[0]);

        for (ScheduledFutureTask<?> task: scheduledTasks) {
            task.cancelWithoutRemove(false);
        }

        scheduledTaskQueue.clearIgnoringIndexes();
    }

    /**
     * @see #pollScheduledTask(long)
     */
    protected final Runnable pollScheduledTask() {
        return pollScheduledTask(getCurrentTimeNanos());
    }

    /**
     * Return the {@link Runnable} which is ready to be executed with the given {@code nanoTime}.
     * You should use {@link #getCurrentTimeNanos()} to retrieve the correct {@code nanoTime}.
     */
    // SNIPPET_START pollScheduledTask
    protected final Runnable pollScheduledTask(long nanoTime) {
        assert inEventLoop();

        // 取出任务
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        // 如果没有任务，或者还没到任务执行的时间，
        if (scheduledTask == null || scheduledTask.deadlineNanos() - nanoTime > 0) {
            // 则返回null
            return null;
        }
        // 将任务从调度队列中移除
        scheduledTaskQueue.remove();
        scheduledTask.setConsumed();
        // 返回任务
        return scheduledTask;
    }
    // SNIPPET_END pollScheduledTask

    /**
     * Return the nanoseconds until the next scheduled task is ready to be run or {@code -1} if no task is scheduled.
     */
    protected final long nextScheduledTaskNano() {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        return scheduledTask != null ? scheduledTask.delayNanos() : -1;
    }

    /**
     * Return the deadline (in nanoseconds) when the next scheduled task is ready to be run or {@code -1}
     * if no task is scheduled.
     */
    protected final long nextScheduledTaskDeadlineNanos() {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        return scheduledTask != null ? scheduledTask.deadlineNanos() : -1;
    }

    final ScheduledFutureTask<?> peekScheduledTask() {
        Queue<ScheduledFutureTask<?>> scheduledTaskQueue = this.scheduledTaskQueue;
        return scheduledTaskQueue != null ? scheduledTaskQueue.peek() : null;
    }

    /**
     * Returns {@code true} if a scheduled task is ready for processing.
     */
    protected final boolean hasScheduledTasks() {
        ScheduledFutureTask<?> scheduledTask = peekScheduledTask();
        return scheduledTask != null && scheduledTask.deadlineNanos() <= getCurrentTimeNanos();
    }

    // 延时调度，针对Runnable的方法
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        ObjectUtil.checkNotNull(command, "command");
        ObjectUtil.checkNotNull(unit, "unit");
        if (delay < 0) {
            delay = 0;
        }
        // 空操作，可忽略
        validateScheduled0(delay, unit);

        // 将任务封装为调度任务对象并进行调度
        return schedule(new ScheduledFutureTask<Void>(
                this,
                command,
                // 计算任务截止执行的时间，即当前时间加上延迟
                deadlineNanos(getCurrentTimeNanos(), unit.toNanos(delay))));
    }

    // 延时调度，针对Callable的方法
    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        ObjectUtil.checkNotNull(callable, "callable");
        ObjectUtil.checkNotNull(unit, "unit");
        if (delay < 0) {
            delay = 0;
        }
        // 空操作，可忽略
        validateScheduled0(delay, unit);

        // 将任务封装为调度任务对象并进行调度
        return schedule(new ScheduledFutureTask<V>(
                this, callable,
                // 计算任务截止执行的时间，即当前时间加上延迟
                deadlineNanos(getCurrentTimeNanos(), unit.toNanos(delay))));
    }

    // 固定周期调度：在固定的一个周期内，执行1次。
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        ObjectUtil.checkNotNull(command, "command");
        ObjectUtil.checkNotNull(unit, "unit");
        if (initialDelay < 0) {
            throw new IllegalArgumentException(
                    String.format("initialDelay: %d (expected: >= 0)", initialDelay));
        }
        if (period <= 0) {
            throw new IllegalArgumentException(
                    String.format("period: %d (expected: > 0)", period));
        }
        validateScheduled0(initialDelay, unit);
        validateScheduled0(period, unit);

        // 将任务封装为调度任务对象并进行调度
        return schedule(new ScheduledFutureTask<Void>(
                this, command,
                // 计算首次执行的截止时间
                deadlineNanos(getCurrentTimeNanos(), unit.toNanos(initialDelay)),
                // 将调度周期转为纳秒
                unit.toNanos(period)));
    }

    // 固定延时调度：在本次执行后的一段时间后再次执行
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        ObjectUtil.checkNotNull(command, "command");
        ObjectUtil.checkNotNull(unit, "unit");
        if (initialDelay < 0) {
            throw new IllegalArgumentException(
                    String.format("initialDelay: %d (expected: >= 0)", initialDelay));
        }
        if (delay <= 0) {
            throw new IllegalArgumentException(
                    String.format("delay: %d (expected: > 0)", delay));
        }

        validateScheduled0(initialDelay, unit);
        validateScheduled0(delay, unit);

        // 将任务封装为调度任务对象并进行调度
        return schedule(new ScheduledFutureTask<Void>(
                this, command,
                // 计算首次执行的截止时间
                deadlineNanos(getCurrentTimeNanos(), unit.toNanos(initialDelay)),
                // 为什么这里把固定延迟时长的负数当作周期？？？
                -unit.toNanos(delay)));
    }

    @SuppressWarnings("deprecation")
    private void validateScheduled0(long amount, TimeUnit unit) {
        validateScheduled(amount, unit);
    }

    /**
     * Sub-classes may override this to restrict the maximal amount of time someone can use to schedule a task.
     *
     * @deprecated will be removed in the future.
     */
    @Deprecated
    protected void validateScheduled(long amount, TimeUnit unit) {
        // NOOP
    }

    final void scheduleFromEventLoop(final ScheduledFutureTask<?> task) {
        // nextTaskId a long and so there is no chance it will overflow back to 0
        /*
         * 给任务设置id，并添加到调度队列中，
         * 第一次执行时还会触发队列创建操作
         */
        scheduledTaskQueue().add(task.setId(++nextTaskId));
    }

    private <V> ScheduledFuture<V> schedule(final ScheduledFutureTask<V> task) {
        if (inEventLoop()) {// 如果在事件循环中
            // 将任务添加到调度队列中
            scheduleFromEventLoop(task);
        } else { // 如果不在事件循环中
            // 获取当前任务需要执行的截止时间
            final long deadlineNanos = task.deadlineNanos();
            // task will add itself to scheduled task queue when run if not expired
            // 当前任务的截止时间在时间循环下次能执行任务的时间之前
            if (beforeScheduledTaskSubmitted(deadlineNanos)) {
                // 执行调度任务
                execute(task);
            } else { // 在截止时间之前还可以被执行到
                // 执行调度任务
                lazyExecute(task);
                // Second hook after scheduling to facilitate race-avoidance
                // 当前任务的截止时间在时间循环下次能执行任务的时间之前，NioEventLoop中的这两个方法实现是一样的，与before和after无关。
                if (afterScheduledTaskSubmitted(deadlineNanos)) {
                    // 这里有什么意义？执行空操作？
                    execute(WAKEUP_TASK);
                }
            }
        }

        return task;
    }

    final void removeScheduled(final ScheduledFutureTask<?> task) {
        assert task.isCancelled();
        if (inEventLoop()) {
            scheduledTaskQueue().removeTyped(task);
        } else {
            // task will remove itself from scheduled task queue when it runs
            lazyExecute(task);
        }
    }

    /**
     * Called from arbitrary non-{@link EventExecutor} threads prior to scheduled task submission.
     * Returns {@code true} if the {@link EventExecutor} thread should be woken immediately to
     * process the scheduled task (if not already awake).
     * <p>
     * If {@code false} is returned, {@link #afterScheduledTaskSubmitted(long)} will be called with
     * the same value <i>after</i> the scheduled task is enqueued, providing another opportunity
     * to wake the {@link EventExecutor} thread if required.
     *
     * @param deadlineNanos deadline of the to-be-scheduled task
     *     relative to {@link AbstractScheduledEventExecutor#getCurrentTimeNanos()}
     * @return {@code true} if the {@link EventExecutor} thread should be woken, {@code false} otherwise
     */
    protected boolean beforeScheduledTaskSubmitted(long deadlineNanos) {
        return true;
    }

    /**
     * See {@link #beforeScheduledTaskSubmitted(long)}. Called only after that method returns false.
     *
     * @param deadlineNanos relative to {@link AbstractScheduledEventExecutor#getCurrentTimeNanos()}
     * @return  {@code true} if the {@link EventExecutor} thread should be woken, {@code false} otherwise
     */
    protected boolean afterScheduledTaskSubmitted(long deadlineNanos) {
        return true;
    }
}
