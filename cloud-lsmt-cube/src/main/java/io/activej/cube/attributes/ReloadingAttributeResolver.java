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

import io.activej.async.service.EventloopService;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.ScheduledRunnable;
import io.activej.eventloop.jmx.EventloopJmxBean;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.activej.common.Utils.nullify;
import static io.activej.eventloop.RunnableWithContext.wrapContext;

public abstract class ReloadingAttributeResolver<K, A> extends AbstractAttributeResolver<K, A> implements EventloopService, EventloopJmxBean {
	protected final Eventloop eventloop;

	private long timestamp;
	private long reloadPeriod;
	private long retryPeriod = 1000L;
	@Nullable
	private ScheduledRunnable scheduledRunnable;
	private final Map<K, A> cache = new HashMap<>();
	private int reloads;
	private int reloadErrors;
	private int resolveErrors;
	private K lastResolveErrorKey;
	private final ValueStats reloadTime = ValueStats.create(Duration.ofHours(1)).withRate("reloads").withUnit("milliseconds");

	protected ReloadingAttributeResolver(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@Nullable
	@Override
	protected final A resolveAttributes(K key) {
		A result = cache.get(key);
		if (result == null) {
			resolveErrors++;
			lastResolveErrorKey = key;
		}
		return result;
	}

	protected abstract Promise<Map<K, A>> reload(long lastTimestamp);

	private void doReload() {
		reloads++;
		scheduledRunnable = nullify(scheduledRunnable, ScheduledRunnable::cancel);
		long reloadTimestamp = eventloop.currentTimeMillis();
		reload(timestamp).whenComplete((result, e) -> {
			if (e == null) {
				reloadTime.recordValue((int) (eventloop.currentTimeMillis() - reloadTimestamp));
				cache.putAll(result);
				timestamp = reloadTimestamp;
				scheduleReload(reloadPeriod);
			} else {
				reloadErrors++;
				scheduleReload(retryPeriod);
			}
		});
	}

	private void scheduleReload(long period) {
		scheduledRunnable = eventloop.delayBackground(period, wrapContext(this, this::doReload));
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		if (reloadPeriod == 0) return Promise.complete();
		long reloadTimestamp = eventloop.currentTimeMillis();
		return reload(timestamp)
				.whenResult(result -> {
					reloadTime.recordValue((int) (eventloop.currentTimeMillis() - reloadTimestamp));
					cache.putAll(result);
					timestamp = reloadTimestamp;
					scheduleReload(reloadPeriod);
				})
				.toVoid();
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
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

	@Nullable
	@JmxAttribute
	public String getLastResolveErrorKey() {
		return lastResolveErrorKey == null ? null : lastResolveErrorKey.toString();
	}

	@JmxAttribute
	public ValueStats getReloadTime() {
		return reloadTime;
	}
}
