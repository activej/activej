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

package io.activej.reactor.jmx;

import io.activej.jmx.api.JmxBeanAdapterWithRefresh;
import io.activej.jmx.api.JmxRefreshable;
import io.activej.jmx.stats.ValueStats;
import io.activej.reactor.Reactor;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkNotNull;

public final class ReactorJmxBeanAdapter implements JmxBeanAdapterWithRefresh {
	private static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(1);

	private final Map<Reactor, List<JmxRefreshable>> reactorToJmxRefreshables = new ConcurrentHashMap<>();
	private final Set<JmxRefreshable> allRefreshables = Collections.newSetFromMap(new IdentityHashMap<>());
	private final Map<Object, Reactor> beanToReactor = new IdentityHashMap<>();

	private volatile Duration refreshPeriod;
	private int maxRefreshesPerCycle;

	@Override
	public synchronized void execute(Object bean, Runnable command) {
		Reactor reactor = beanToReactor.get(bean);
		checkNotNull(reactor, () -> "Unregistered bean " + bean);
		reactor.execute(command);
	}

	@Override
	public void setRefreshParameters(Duration refreshPeriod, int maxRefreshesPerCycle) {
		checkArgument(refreshPeriod.toMillis() > 0);
		checkArgument(maxRefreshesPerCycle > 0);
		this.refreshPeriod = refreshPeriod;
		this.maxRefreshesPerCycle = maxRefreshesPerCycle;
		for (Map.Entry<Reactor, List<JmxRefreshable>> entry : reactorToJmxRefreshables.entrySet()) {
			entry.getKey().execute(() -> ((ValueStats) entry.getValue().get(0)).resetStats());
		}
	}

	@Override
	public synchronized void registerRefreshableBean(Object bean, List<JmxRefreshable> beanRefreshables) {
		checkNotNull(refreshPeriod, "Not initialized");

		Reactor reactor = ensureReactor(bean);
		if (!reactorToJmxRefreshables.containsKey(reactor)) {
			Duration smoothingWindows = reactor instanceof ReactiveJmxBeanWithStats reactorJmxBeanWithStats ?
					reactorJmxBeanWithStats.getSmoothingWindow() :
					null;
			if (smoothingWindows == null) {
				smoothingWindows = DEFAULT_SMOOTHING_WINDOW;
			}
			ValueStats refreshStats = ValueStats.builder(smoothingWindows)
					.withRate()
					.withUnit("ns")
					.build();
			List<JmxRefreshable> list = new ArrayList<>();
			list.add(refreshStats);
			reactorToJmxRefreshables.put(reactor, list);
			reactor.execute(() -> refresh(reactor, list, 0, refreshStats));
		}

		Set<JmxRefreshable> beanRefreshablesFiltered = new HashSet<>();
		for (JmxRefreshable refreshable : beanRefreshables) {
			if (allRefreshables.add(refreshable)) {
				beanRefreshablesFiltered.add(refreshable);
			}
		}

		reactor.submit(() -> {
			List<JmxRefreshable> refreshables = reactorToJmxRefreshables.get(reactor);
			refreshables.addAll(beanRefreshablesFiltered);
		});
	}

	private Reactor ensureReactor(Object bean) {
		Reactor reactor = beanToReactor.get(bean);
		if (reactor == null) {
			try {
				reactor = (Reactor) bean.getClass().getMethod("getReactor").invoke(bean);
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new IllegalStateException("Class annotated with @ReactorJmxBean should have a 'getReactor()' method");
			}
			checkNotNull(reactor);
			beanToReactor.put(bean, reactor);
		}
		return reactor;
	}

	@Override
	public List<String> getRefreshStats() {
		List<String> result = new ArrayList<>();
		if (reactorToJmxRefreshables.size() > 1) {
			int count = 0;
			ValueStats total = ValueStats.accumulatorBuilder()
					.withRate()
					.withUnit("ms")
					.build();
			for (List<JmxRefreshable> refreshables : reactorToJmxRefreshables.values()) {
				total.add((ValueStats) refreshables.get(0));
				count += refreshables.size();
			}
			result.add(getStatsString(count, total));
		}
		for (List<JmxRefreshable> refreshables : reactorToJmxRefreshables.values()) {
			result.add(getStatsString(refreshables.size(), (ValueStats) refreshables.get(0)));
		}
		return result;
	}

	private void refresh(Reactor reactor, List<JmxRefreshable> list, int startIndex, ValueStats refreshStats) {
		assert reactor.inReactorThread();

		long timestamp = reactor.currentTimeMillis();

		int index = startIndex < list.size() ? startIndex : 0;
		int endIndex = Math.min(list.size(), index + maxRefreshesPerCycle);
		long currentNanos = System.nanoTime();
		while (index < endIndex) {
			list.get(index++).refresh(timestamp);
		}

		long refreshTimeNanos = System.nanoTime() - currentNanos;
		refreshStats.recordValue(refreshTimeNanos);

		long nextTimestamp = timestamp + computeEffectiveRefreshPeriod(list.size());
		reactor.scheduleBackground(nextTimestamp, () -> refresh(reactor, list, endIndex, refreshStats));
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
