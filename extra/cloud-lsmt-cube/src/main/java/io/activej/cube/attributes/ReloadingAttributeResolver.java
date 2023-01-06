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

package io.activej.cube.attributes;

import io.activej.async.service.ReactiveService;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBean;
import io.activej.reactor.schedule.ScheduledRunnable;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.activej.common.Utils.nullify;

public abstract class ReloadingAttributeResolver<K, A> extends AbstractAttributeResolver<K, A>
		implements ReactiveService, ReactiveJmxBean {
	private long timestamp;
	private long reloadPeriod;
	private long retryPeriod = 1000L;
	private @Nullable ScheduledRunnable scheduledRunnable;
	private final Map<K, A> cache = new HashMap<>();
	private int reloads;
	private int reloadErrors;
	private int resolveErrors;
	private K lastResolveErrorKey;
	private final ValueStats reloadTime = ValueStats.create(Duration.ofHours(1)).withRate("reloads").withUnit("milliseconds");

	protected ReloadingAttributeResolver(Reactor reactor) {
		super(reactor);
	}

	@Override
	protected final @Nullable A resolveAttributes(K key) {
		A result = cache.get(key);
		if (result == null) {
			resolveErrors++;
			lastResolveErrorKey = key;
		}
		return result;
	}

	protected abstract Promise<Map<K, A>> reload(long lastTimestamp);

	private void doReload() {
		checkInReactorThread();
		reloads++;
		scheduledRunnable = nullify(scheduledRunnable, ScheduledRunnable::cancel);
		long reloadTimestamp = reactor.currentTimeMillis();
        reload(timestamp)
                .whenResult(result -> {
                    reloadTime.recordValue((int) (reactor.currentTimeMillis() - reloadTimestamp));
                    cache.putAll(result);
                    timestamp = reloadTimestamp;
                })
                .whenException(e -> reloadErrors++)
				.whenComplete(() -> scheduleReload(retryPeriod));
    }

	private void scheduleReload(long period) {
		scheduledRunnable = reactor.delayBackground(period, this::doReload);
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread();
		if (reloadPeriod == 0) return Promise.complete();
		long reloadTimestamp = reactor.currentTimeMillis();
		return reload(timestamp)
				.whenResult(result -> {
					reloadTime.recordValue((int) (reactor.currentTimeMillis() - reloadTimestamp));
					cache.putAll(result);
					timestamp = reloadTimestamp;
					scheduleReload(reloadPeriod);
				})
				.toVoid();
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread();
		scheduledRunnable = nullify(scheduledRunnable, ScheduledRunnable::cancel);
		return Promise.complete();
	}

	@JmxOperation
	public void reload() {
		doReload();
	}

	@JmxAttribute
	public long getReloadPeriod() {
		return reloadPeriod;
	}

	@JmxAttribute
	public void setReloadPeriod(long reloadPeriod) {
		this.reloadPeriod = reloadPeriod;
	}

	@JmxAttribute
	public long getRetryPeriod() {
		return retryPeriod;
	}

	@JmxAttribute
	public void setRetryPeriod(long retryPeriod) {
		this.retryPeriod = retryPeriod;
	}

	@JmxAttribute
	public int getReloads() {
		return reloads;
	}

	@JmxAttribute
	public int getReloadErrors() {
		return reloadErrors;
	}

	@JmxAttribute
	public int getResolveErrors() {
		return resolveErrors;
	}

	@JmxAttribute
	public @Nullable String getLastResolveErrorKey() {
		return lastResolveErrorKey == null ? null : lastResolveErrorKey.toString();
	}

	@JmxAttribute
	public ValueStats getReloadTime() {
		return reloadTime;
	}
}
