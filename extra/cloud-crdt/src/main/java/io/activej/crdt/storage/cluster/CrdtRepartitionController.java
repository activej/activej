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

package io.activej.crdt.storage.cluster;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncRunnables;
import io.activej.common.initializer.WithInitializer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

public final class CrdtRepartitionController<K extends Comparable<K>, S, P extends Comparable<P>> implements EventloopJmxBeanWithStats, WithInitializer<CrdtRepartitionController<K, S, P>> {
	private final P localPartitionId;
	private final CrdtStorageCluster<K, S, P> cluster;

	private final AsyncRunnable repartition = AsyncRunnables.reuse(this::doRepartition);

	private CrdtRepartitionController(CrdtStorageCluster<K, S, P> cluster, P localPartitionId) {
		this.cluster = cluster;
		this.localPartitionId = localPartitionId;
	}

	public static <K extends Comparable<K>, S, P extends Comparable<P>> CrdtRepartitionController<K, S, P> create(
			CrdtStorageCluster<K, S, P> cluster, P localPartitionId) {
		return new CrdtRepartitionController<>(cluster, localPartitionId);
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return cluster.getEventloop();
	}

	// region JMX
	private final PromiseStats repartitionPromise = PromiseStats.create(Duration.ofMinutes(5));
	// endregion

	public Promise<Void> repartition() {
		return repartition.run();
	}

	private @NotNull Promise<Void> doRepartition() {
		return cluster.repartition(localPartitionId)
				.whenComplete(repartitionPromise.recordStats());
	}

	// region JMX
	@JmxAttribute
	public PromiseStats getRepartitionPromise() {
		return repartitionPromise;
	}
	// endregion
}
