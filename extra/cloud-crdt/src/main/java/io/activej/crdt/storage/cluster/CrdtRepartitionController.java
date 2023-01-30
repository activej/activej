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
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;

import java.time.Duration;

import static io.activej.reactor.Reactive.checkInReactorThread;

public final class CrdtRepartitionController<K extends Comparable<K>, S, P> extends AbstractReactive
		implements ReactiveJmxBeanWithStats {
	private final P localPartitionId;
	private final ClusterCrdtStorage<K, S, P> cluster;

	private final AsyncRunnable repartition = AsyncRunnables.reuse(this::doRepartition);

	private CrdtRepartitionController(Reactor reactor,
			ClusterCrdtStorage<K, S, P> cluster, P localPartitionId) {
		super(reactor);
		this.cluster = cluster;
		this.localPartitionId = localPartitionId;
	}

	public static <K extends Comparable<K>, S, P> CrdtRepartitionController<K, S, P> create(Reactor reactor,
			ClusterCrdtStorage<K, S, P> cluster, P localPartitionId) {
		return new CrdtRepartitionController<>(reactor, cluster, localPartitionId);
	}

	private final PromiseStats repartitionPromise = PromiseStats.create(Duration.ofMinutes(5));

	public Promise<Void> repartition() {
		checkInReactorThread(this);
		return repartition.run();
	}

	private Promise<Void> doRepartition() {
		return cluster.repartition(localPartitionId)
				.whenComplete(repartitionPromise.recordStats());
	}

	@JmxAttribute
	public PromiseStats getRepartitionPromise() {
		return repartitionPromise;
	}
}
