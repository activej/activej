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

package io.activej.eventloop.jmx;

import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.JmxBeanAdapterWithRefresh;
import io.activej.jmx.api.JmxRefreshable;
import io.activej.jmx.stats.ValueStats;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.activej.common.Checks.*;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;

public final class EventloopJmxBeanAdapter implements JmxBeanAdapterWithRefresh {
	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(1);

	private final Map<Eventloop, List<JmxRefreshable>> eventloopToJmxRefreshables = new ConcurrentHashMap<>();
	private final Set<JmxRefreshable> allRefreshables = Collections.newSetFromMap(new IdentityHashMap<>());
	private final Map<Object, Eventloop> beanToEventloop = new IdentityHashMap<>();

	private volatile Duration refreshPeriod;
	private int maxRefreshesPerCycle;

	@Override
	public synchronized void execute(Object bean, Runnable command) {
		Eventloop eventloop = beanToEventloop.get(bean);
		checkNotNull(eventloop, () -> "Unregistered bean " + bean);
		eventloop.execute(wrapContext(bean, command));
	}

	@Override
	public void setRefreshParameters(@NotNull Duration refreshPeriod, int maxRefreshesPerCycle) {
		checkArgument(refreshPeriod.toMillis() > 0);
		checkArgument(maxRefreshesPerCycle > 0);
		this.refreshPeriod = refreshPeriod;
		this.maxRefreshesPerCycle = maxRefreshesPerCycle;
		for (Map.Entry<Eventloop, List<JmxRefreshable>> entry : eventloopToJmxRefreshables.entrySet()) {
			entry.getKey().execute(() -> ((ValueStats) entry.getValue().get(0)).resetStats());
		}
	}

	@Override
	public synchronized void registerRefreshableBean(Object bean, List<JmxRefreshable> beanRefreshables) {
		checkNotNull(refreshPeriod, "Not initialized");

		Eventloop eventloop = ensureEventloop(bean);
		if (!eventloopToJmxRefreshables.containsKey(eventloop)) {
			Duration smoothingWindows = eventloop.getSmoothingWindow();
			if (smoothingWindows == null) {
				smoothingWindows = DEFAULT_SMOOTHING_WINDOW;
			}
			ValueStats refreshStats = ValueStats.create(smoothingWindows).withRate().withUnit("ns");
			List<JmxRefreshable> list = new ArrayList<>();
			list.add(refreshStats);
			eventloopToJmxRefreshables.put(eventloop, list);
			eventloop.execute(() -> refresh(eventloop, list, 0, refreshStats));
		}

		Set<JmxRefreshable> beanRefreshablesFiltered = new HashSet<>();
		for (JmxRefreshable refreshable : beanRefreshables) {
			if (allRefreshables.add(refreshable)) {
				beanRefreshablesFiltered.add(refreshable);
			}
		}

		eventloop.submit(() -> {
			List<JmxRefreshable> refreshables = eventloopToJmxRefreshables.get(eventloop);
			refreshables.addAll(beanRefreshablesFiltered);
		});
	}

	private Eventloop ensureEventloop(Object bean) {
		Eventloop eventloop = beanToEventloop.get(bean);
		if (eventloop == null) {
			try {
				eventloop = (Eventloop) bean.getClass().getMethod("getEventloop").invoke(bean);
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new IllegalStateException("Class annotated with @EventloopJmxBean should have a 'getEventloop()' method");
			}
			checkNotNull(eventloop);
			beanToEventloop.put(bean, eventloop);
		}
		return eventloop;
	}

	@Override
	public List<String> getRefreshStats() {
		List<String> result = new ArrayList<>();
		if (eventloopToJmxRefreshables.size() > 1) {
			int count = 0;
			ValueStats total = ValueStats.createAccumulator().withRate().withUnit("ms");
			for (List<JmxRefreshable> refreshables : eventloopToJmxRefreshables.values()) {
				total.add((ValueStats) refreshables.get(0));
				count += refreshables.size();
			}
			result.add(getStatsString(count, total));
		}
		for (List<JmxRefreshable> refreshables : eventloopToJmxRefreshables.values()) {
			result.add(getStatsString(refreshables.size(), (ValueStats) refreshables.get(0)));
		}
		return result;
	}

	private void refresh(@NotNull Eventloop eventloop, @NotNull List<JmxRefreshable> list, int startIndex, ValueStats refreshStats) {
		checkState(eventloop.inEventloopThread());

		long timestamp = eventloop.currentTimeMillis();

		int index = startIndex < list.size() ? startIndex : 0;
		int endIndex = Math.min(list.size(), index + maxRefreshesPerCycle);
		long currentNanos = System.nanoTime();
		while (index < endIndex) {
			list.get(index++).refresh(timestamp);
		}

		long refreshTimeNanos = System.nanoTime() - currentNanos;
		refreshStats.recordValue(refreshTimeNanos);

		long nextTimestamp = timestamp + computeEffectiveRefreshPeriod(list.size());
		eventloop.scheduleBackground(nextTimestamp, () -> refresh(eventloop, list, endIndex, refreshStats));
	}

	private long computeEffectiveRefreshPeriod(int totalCount) {
		return maxRefreshesPerCycle >= totalCount ?
				refreshPeriod.toMillis() :
				refreshPeriod.toMillis() * maxRefreshesPerCycle / totalCount;
	}

	private static String getStatsString(int numberOfRefreshables, ValueStats stats) {
		return "# of refreshables: " + numberOfRefreshables + "  " + stats;
	}

}
