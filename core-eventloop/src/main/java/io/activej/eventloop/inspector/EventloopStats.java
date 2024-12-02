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

import io.activej.common.inspector.AbstractInspector;
import io.activej.common.time.Stopwatch;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.*;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.activej.eventloop.Eventloop.DEFAULT_SMOOTHING_WINDOW;
import static io.activej.jmx.stats.JmxHistogram.POWERS_OF_TWO;

@SuppressWarnings("unused")
public final class EventloopStats extends AbstractInspector<EventloopInspector> implements EventloopInspector {
	private final EventStats loops;
	private final LongValueStats selectorSelectTimeout;
	private final LongValueStats selectorSelectTime;
	private final LongValueStats businessLogicTime;
	private final Tasks tasks;
	private final Keys keys;
	private final ExceptionStats fatalErrors;
	private final Map<Class<? extends Throwable>, ExceptionStats> fatalErrorsMap;
	private final EventStats idleLoops;
	private final EventStats idleLoopsWaitingExternalTask;
	private final EventStats selectOverdues;

	private EventloopStats() {
		loops = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
		selectorSelectTimeout = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
			.withHistogram(new long[]{-256, -128, -64, -32, -16, -8, -4, -2, -1, 0, 1, 2, 4, 8, 16, 32})
			.withUnit("milliseconds")
			.build();
		selectorSelectTime = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
			.withHistogram(POWERS_OF_TWO)
			.withUnit("milliseconds")
			.build();
		businessLogicTime = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
			.withHistogram(POWERS_OF_TWO)
			.withUnit("milliseconds")
			.build();
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
			this.businessLogicTime.recordValue(businessLogicTime);
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
		this.selectorSelectTime.recordValue(selectorSelectTime);
	}

	@Override
	public void onUpdateSelectorSelectTimeout(long selectorSelectTimeout) {
		this.selectorSelectTimeout.recordValue(selectorSelectTimeout);
		if (selectorSelectTimeout < 0) selectOverdues.recordEvent();
	}

	@Override
	public void onUpdateSelectedKeyDuration(Stopwatch sw) {
		keys.oneKeyTime.recordValue(sw.elapsed(TimeUnit.MICROSECONDS));
	}

	@Override
	public void onUpdateSelectedKeysStats(
		int lastSelectedKeys, int invalidKeys, int acceptKeys, int connectKeys, int readKeys, int writeKeys,
		long loopTime
	) {
		keys.all.recordEvents(lastSelectedKeys);
		keys.invalid.recordEvents(invalidKeys);
		keys.acceptPerLoop.recordValue(acceptKeys);
		keys.connectPerLoop.recordValue(connectKeys);
		keys.readPerLoop.recordValue(readKeys);
		keys.writePerLoop.recordValue(writeKeys);
		if (lastSelectedKeys != 0) keys.loopTime.recordValue(loopTime);
	}

	private void updateTaskDuration(LongValueStats counter, DurationRunnable longestCounter, Runnable runnable, @Nullable Stopwatch sw) {
		if (sw != null) {
			long elapsed = sw.elapsed(TimeUnit.MICROSECONDS);
			counter.recordValue(elapsed);
			if (elapsed > longestCounter.getDuration()) {
				longestCounter.update(runnable, elapsed);
			}
		}
	}

	@Override
	public void onUpdateLocalTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(tasks.local.oneTaskTime, tasks.local.longestTask, runnable, sw);
	}

	@Override
	public void onUpdateLocalTasksStats(int localTasks, long loopTime) {
		if (localTasks != 0) tasks.local.loopTime.recordValue(loopTime);
		tasks.local.tasksPerLoop.recordValue(localTasks);
	}

	@Override
	public void onUpdateConcurrentTaskDuration(Runnable runnable, @Nullable Stopwatch sw) {
		updateTaskDuration(tasks.concurrent.oneTaskTime, tasks.concurrent.longestTask, runnable, sw);
	}

	@Override
	public void onUpdateConcurrentTasksStats(int newConcurrentTasks, long loopTime) {
		if (newConcurrentTasks != 0) tasks.concurrent.loopTime.recordValue(loopTime);
		tasks.concurrent.tasksPerLoop.recordValue(newConcurrentTasks);
	}

	@Override
	public void onUpdateScheduledTaskDuration(Runnable runnable, @Nullable Stopwatch sw, boolean background) {
		if (background) {
			updateTaskDuration(tasks.background.getOneTaskTime(), tasks.background.getLongestTask(), runnable, sw);
		} else {
			updateTaskDuration(tasks.scheduled.getOneTaskTime(), tasks.scheduled.getLongestTask(), runnable, sw);
		}
	}

	@Override
	public void onUpdateScheduledTasksStats(int scheduledTasks, long loopTime, boolean background) {
		if (background) {
			if (scheduledTasks != 0) tasks.background.getLoopTime().recordValue(loopTime);
			tasks.background.getTasksPerLoop().recordValue(scheduledTasks);
		} else {
			if (scheduledTasks != 0) tasks.scheduled.getLoopTime().recordValue(loopTime);
			tasks.scheduled.getTasksPerLoop().recordValue(scheduledTasks);
		}
	}

	@Override
	public void onFatalError(Throwable e, Object context) {
		fatalErrors.recordException(e, context);

		Class<? extends Throwable> type = e.getClass();
		ExceptionStats stats = fatalErrorsMap.computeIfAbsent(type, k -> ExceptionStats.create());
		stats.recordException(e, context);
	}

	@Override
	public void onScheduledTaskOverdue(long overdue, boolean background) {
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
	public LongValueStats getSelectorSelectTime() {
		return selectorSelectTime;
	}

	@JmxAttribute(extraSubAttributes = "histogram")
	public LongValueStats getSelectorSelectTimeout() {
		return selectorSelectTimeout;
	}

	@JmxAttribute(extraSubAttributes = "histogram")
	public LongValueStats getBusinessLogicTime() {
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
		private final LongValueStats tasksPerLoop;
		private final LongValueStats loopTime;
		private final LongValueStats oneTaskTime;
		private final DurationRunnable longestTask;

		TaskStats() {
			this.tasksPerLoop = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.build();
			this.loopTime = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.withUnit("milliseconds")
				.build();
			this.oneTaskTime = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.withUnit("microseconds")
				.build();
			this.longestTask = new DurationRunnable();
		}

		@JmxAttribute(name = "perLoop", extraSubAttributes = "histogram")
		public LongValueStats getTasksPerLoop() {
			return tasksPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public LongValueStats getLoopTime() {
			return loopTime;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public LongValueStats getOneTaskTime() {
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
		private final LongValueStats overdues;

		ScheduledTaskStats() {
			overdues = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.withRate()
				.withUnit("milliseconds")
				.build();
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public LongValueStats getOverdues() {
			return overdues;
		}
	}

	public static final class Keys {
		private final EventStats all;
		private final EventStats invalid;
		private final LongValueStats acceptPerLoop;
		private final LongValueStats connectPerLoop;
		private final LongValueStats readPerLoop;
		private final LongValueStats writePerLoop;
		private final LongValueStats loopTime;
		private final LongValueStats oneKeyTime;

		public Keys() {
			all = EventStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withRateUnit("keys")
				.build();
			invalid = EventStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withRateUnit("keys")
				.build();
			acceptPerLoop = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.build();
			connectPerLoop = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.build();
			readPerLoop = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.build();
			writePerLoop = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.build();
			loopTime = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.withUnit("milliseconds")
				.build();
			oneKeyTime = LongValueStats.builder(DEFAULT_SMOOTHING_WINDOW)
				.withHistogram(POWERS_OF_TWO)
				.withUnit("microseconds")
				.build();
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
		public LongValueStats getAcceptPerLoop() {
			return acceptPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public LongValueStats getConnectPerLoop() {
			return connectPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public LongValueStats getReadPerLoop() {
			return readPerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public LongValueStats getWritePerLoop() {
			return writePerLoop;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public LongValueStats getLoopTime() {
			return loopTime;
		}

		@JmxAttribute(extraSubAttributes = "histogram")
		public LongValueStats getOneKeyTime() {
			return oneKeyTime;
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
