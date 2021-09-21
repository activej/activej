/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.eventloop.inspector;

import io.activej.common.initializer.WithInitializer;
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.time.Stopwatch;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.activej.eventloop.Eventloop.DEFAULT_SMOOTHING_WINDOW;
import static io.activej.jmx.stats.JmxHistogram.POWERS_OF_TWO;

@SuppressWarnings("unused")
public final class EventloopStats extends AbstractInspector<EventloopInspector> implements EventloopInspector, WithInitializer<EventloopStats> {
	private final EventStats loops;
	private final ValueStats selectorSelectTimeout;
	private final ValueStats selectorSelectTime;
	private final ValueStats businessLogicTime;
	private final Tasks tasks;
	private final Keys keys;
	private final ExceptionStats fatalErrors;
	private final Map<Class<? extends Throwable>, ExceptionStats> fatalErrorsMap;
	private final EventStats idleLoops;
	private final EventStats idleLoopsWaitingExternalTask;
	private final EventStats selectOverdues;

	private EventloopStats() {
		loops = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
		selectorSelectTimeout = ValueStats.create(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(new int[]{-256, -128, -64, -32, -16, -8, -4, -2, -1, 0, 1, 2, 4, 8, 16, 32}).withUnit("milliseconds");
		selectorSelectTime = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO).withUnit("milliseconds");
		businessLogicTime = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO).withUnit("milliseconds");
		tasks = new Tasks();
		keys = new Keys();
		fatalErrors = ExceptionStats.create();
		fatalErrorsMap = new HashMap<>();
		idleLoops = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
		idleLoopsWaitingExternalTask = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
		selectOverdues = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	}

	public static EventloopStats create() {
		return new EventloopStats();
	}

	// region updating
	@Override
	public void onUpdateBusinessLogicTime(boolean taskOrKeyPresent, boolean externalTaskPresent, long businessLogicTime) {
		loops.recordEvent();
		if (taskOrKeyPresent) {
			this.businessLogicTime.recordValue((int) businessLogicTime);
		} else {
			if (!externalTaskPresent) {
				idleLoops.recordEvent();
			} else {
				idleLoopsWaitingExternalTask.recordEvent();
			}
		}
	}

	@Override
	public void onUpdateSelectorSelectTime(long selectorSelectTime) {
		this.selectorSelectTime.recordValue((int) selectorSelectTime);
	}

	@Override
	public void onUpdateSelectorSelectTimeout(long selectorSelectTimeout) {
		this.selectorSelectTimeout.recordValue((int) selectorSelectTimeout);
		if (selectorSelectTimeout < 0) selectOverdues.recordEvent();
	}

	@Override
	public void onUpdateSelectedKeyDuration(@NotNull Stopwatch sw) {
		keys.oneKeyTime.recordValue((int) sw.elapsed(TimeUnit.MICROSECONDS));
	}

	@Override
	public void onUpdateSelectedKeysStats(int lastSelectedKeys, int invalidKeys, int acceptKeys,
			int connectKeys, int readKeys, int writeKeys, long loopTime) {
		keys.all.recordEvents(lastSelectedKeys);
		keys.invalid.recordEvents(invalidKeys);
		keys.acceptPerLoop.recordValue(acceptKeys);
		keys.connectPerLoop.recordValue(connectKeys);
		keys.readPerLoop.recordValue(readKeys);
		keys.writePerLoop.recordValue(writeKeys);
		if (lastSelectedKeys != 0) keys.loopTime.recordValue((int) loopTime);
	}

	private void updateTaskDuration(ValueStats counter, DurationRunnable longestCounter, Runnable runnable, @Nullable Stopwatch sw) {
		if (sw != null) {
			int elapsed = (int) sw.elapsed(TimeUnit.MICROSECONDS);
			counter.recordValue(elapsed);
			if (elapsed > longestCounter.getDuration()) {
				longestCounter.update(runnable, elapsed);
			}
		}
	}

	@Override
	public void onUpdateLocalTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(tasks.local.oneTaskTime, tasks.local.longestTask, runnable, sw);
	}

	@Override
	public void onUpdateLocalTasksStats(int localTasks, long loopTime) {
		if (localTasks != 0) tasks.local.loopTime.recordValue((int) loopTime);
		tasks.local.tasksPerLoop.recordValue(localTasks);
	}

	@Override
	public void onUpdateConcurrentTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(tasks.concurrent.oneTaskTime, tasks.concurrent.longestTask, runnable, sw);
	}

	@Override
	public void onUpdateConcurrentTasksStats(int newConcurrentTasks, long loopTime) {
		if (newConcurrentTasks != 0) tasks.concurrent.loopTime.recordValue((int) loopTime);
		tasks.concurrent.tasksPerLoop.recordValue(newConcurrentTasks);
	}

	@Override
	public void onUpdateScheduledTaskDuration(@NotNull Runnable runnable, @Nullable Stopwatch sw, boolean background) {
		if (background) {
			updateTaskDuration(tasks.background.getOneTaskTime(), tasks.background.getLongestTask(), runnable, sw);
		} else {
			updateTaskDuration(tasks.scheduled.getOneTaskTime(), tasks.scheduled.getLongestTask(), runnable, sw);
		}
	}

	@Override
	public void onUpdateScheduledTasksStats(int scheduledTasks, long loopTime, boolean background) {
		if (background) {
			if (scheduledTasks != 0) tasks.background.getLoopTime().recordValue((int) loopTime);
			tasks.background.getTasksPerLoop().recordValue(scheduledTasks);
		} else {
			if (scheduledTasks != 0) tasks.scheduled.getLoopTime().recordValue((int) loopTime);
			tasks.scheduled.getTasksPerLoop().recordValue(scheduledTasks);
		}
	}

	@Override
	public void onFatalError(@NotNull Throwable e, Object context) {
		fatalErrors.recordException(e, context);

		Class<? extends Throwable> type = e.getClass();
		ExceptionStats stats = fatalErrorsMap.computeIfAbsent(type, k -> ExceptionStats.create());
		stats.recordException(e, context);
	}

	@Override
	public void onScheduledTaskOverdue(int overdue, boolean background) {
		if (background) {
			tasks.background.overdues.recordValue(overdue);
		} else {
			tasks.scheduled.overdues.recordValue(overdue);
		}
	}
	// endregion

	// region root attributes
	@JmxAttribute
	public EventStats getLoops() {
		return loops;
	}

	@JmxAttribute(extraSubAttributes = "histogram")
	public ValueStats getSelectorSelectTime() {
		return selectorSelectTime;
	}

	@JmxAttribute(extraSubAttributes = "histogram")
	public ValueStats getSelectorSelectTimeout() {
		return selectorSelectTimeout;
	}

	@JmxAttribute(extraSubAttributes = "histogram")
	public ValueStats getBusinessLogicTime() {
		return businessLogicTime;
	}

	@JmxAttribute
	public Tasks getTasks() {
		return tasks;
	}

	@JmxAttribute
	public Keys getKeys() {
		return keys;
	}

	@JmxAttribute
	public ExceptionStats getFatalErrors() {
		return fatalErrors;
	}

	@JmxAttribute
	public Map<Class<? extends Throwable>, ExceptionStats> getFatalErrorsMap() {
		return fatalErrorsMap;
	}

	@JmxAttribute
	public EventStats getIdleLoops() {
		return idleLoops;
	}

	@JmxAttribute
	public EventStats getIdleLoopsWaitingExternalTask() {
		return idleLoopsWaitingExternalTask;
	}

	@JmxAttribute
	public EventStats getSelectOverdues() {
		return selectOverdues;
	}
	// endregion

	// region helper classes for stats grouping
	public static final class Tasks {
		private final TaskStats local;
		private final TaskStats concurrent;
		private final ScheduledTaskStats scheduled;
		private final ScheduledTaskStats background;

		Tasks() {
			local = new TaskStats();
			concurrent = new TaskStats();
			scheduled = new ScheduledTaskStats();
			background = new ScheduledTaskStats();
		}

		@JmxAttribute
		public TaskStats getLocal() {
			return local;
		}

		@JmxAttribute
		public TaskStats getConcurrent() {
			return concurrent;
		}

		@JmxAttribute
		public ScheduledTaskStats getScheduled() {
			return scheduled;
		}

		@JmxAttribute
		public ScheduledTaskStats getBackground() {
			return background;
		}
	}

	@SuppressWarnings("WeakerAccess")
	public static class TaskStats {
		private final ValueStats tasksPerLoop;
		private final ValueStats loopTime;
		private final ValueStats oneTaskTime;
		private final DurationRunnable longestTask;

		TaskStats() {
			this.tasksPerLoop = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO);
			this.loopTime = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO).withUnit("milliseconds");
			this.oneTaskTime = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO).withUnit("microseconds");
			this.longestTask = new DurationRunnable();
		}

		@JmxAttribute(name = "perLoop", extraSubAttributes = "histogram")
		public ValueStats getTasksPerLoop() {
			return tasksPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getLoopTime() {
			return loopTime;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getOneTaskTime() {
			return oneTaskTime;
		}

		@JmxAttribute
		public DurationRunnable getLongestTask() {
			return longestTask;
		}

		@JmxAttribute(reducer = JmxReducerSum.class)
		public int getCount() {
			return (int) tasksPerLoop.getLastValue();
		}
	}

	public static final class ScheduledTaskStats extends TaskStats {
		private final ValueStats overdues;

		ScheduledTaskStats() {
			overdues = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO).withRate().withUnit("milliseconds");
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getOverdues() {
			return overdues;
		}
	}

	public static final class Keys {
		private final EventStats all;
		private final EventStats invalid;
		private final ValueStats acceptPerLoop;
		private final ValueStats connectPerLoop;
		private final ValueStats readPerLoop;
		private final ValueStats writePerLoop;
		private final ValueStats loopTime;
		private final ValueStats oneKeyTime;

		public Keys() {
			all = EventStats.create(DEFAULT_SMOOTHING_WINDOW).withRateUnit("keys");
			invalid = EventStats.create(DEFAULT_SMOOTHING_WINDOW).withRateUnit("keys");
			acceptPerLoop = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO);
			connectPerLoop = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO);
			readPerLoop = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO);
			writePerLoop = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO);
			loopTime = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO).withUnit("milliseconds");
			oneKeyTime = ValueStats.create(DEFAULT_SMOOTHING_WINDOW).withHistogram(POWERS_OF_TWO).withUnit("microseconds");
		}

		@JmxAttribute
		public EventStats getAll() {
			return all;
		}

		@JmxAttribute
		public EventStats getInvalid() {
			return invalid;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getAcceptPerLoop() {
			return acceptPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getConnectPerLoop() {
			return connectPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getReadPerLoop() {
			return readPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getWritePerLoop() {
			return writePerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getLoopTime() {
			return loopTime;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public ValueStats getOneKeyTime() {
			return oneKeyTime;
		}
	}

	private static final class StackTrace {
		private final StackTraceElement[] stackTraceElements;

		public StackTrace(StackTraceElement[] stackTraceElements) {
			this.stackTraceElements = stackTraceElements;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof StackTrace)) return false;

			StackTrace that = (StackTrace) o;

			return Arrays.equals(stackTraceElements, that.stackTraceElements);
		}

		@Override
		public int hashCode() {
			return stackTraceElements != null ? Arrays.hashCode(stackTraceElements) : 0;
		}
	}

	public static final class DurationRunnable implements JmxStats<DurationRunnable>, JmxStatsWithReset {
		private long duration;
		private @Nullable Runnable runnable;

		@Override
		public void resetStats() {
			duration = 0;
			runnable = null;
		}

		void update(Runnable runnable, long duration) {
			this.duration = duration;
			this.runnable = runnable;
		}

		@JmxAttribute(name = "duration(μs)")
		public long getDuration() {
			return duration;
		}

		@JmxAttribute
		public String getClassName() {
			return (runnable == null) ? "" : runnable.getClass().getName();
		}

		@Override
		public void add(DurationRunnable another) {
			if (another.duration > this.duration) {
				this.duration = another.duration;
				this.runnable = another.runnable;
			}
		}
	}
	// endregion
}
