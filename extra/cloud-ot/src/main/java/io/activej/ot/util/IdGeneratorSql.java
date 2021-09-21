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

package io.activej.ot.util;

import io.activej.async.function.AsyncRunnable;
import io.activej.common.initializer.WithInitializer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;

import static io.activej.async.function.AsyncRunnables.reuse;
import static io.activej.common.Checks.checkState;
import static io.activej.promise.PromisePredicates.isResultOrException;
import static io.activej.promise.Promises.retry;

public final class IdGeneratorSql implements IdGenerator<Long>, EventloopJmxBeanWithStats, WithInitializer<IdGeneratorSql> {

	private final Eventloop eventloop;
	private final Executor executor;
	private final DataSource dataSource;

	private final SqlAtomicSequence sequence;

	private int stride = 1;

	private long next;
	private long limit;

	private final PromiseStats promiseCreateId = PromiseStats.create(Duration.ofMinutes(5));

	private final AsyncRunnable reserveId = reuse(this::doReserveId).transformWith(promiseCreateId::wrapper);

	private IdGeneratorSql(Eventloop eventloop, Executor executor, DataSource dataSource, SqlAtomicSequence sequence) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.dataSource = dataSource;
		this.sequence = sequence;
	}

	public static IdGeneratorSql create(Eventloop eventloop, Executor executor, DataSource dataSource,
			SqlAtomicSequence sequence) {
		return new IdGeneratorSql(eventloop, executor, dataSource, sequence);
	}

	public IdGeneratorSql withStride(int stride) {
		this.stride = stride;
		return this;
	}

	private Promise<Void> doReserveId() {
		int finalStride = stride;
		return Promise.ofBlocking(executor, () -> getAndAdd(finalStride))
				.whenResult(id -> {
					next = id;
					limit = id + finalStride;
				})
				.toVoid();
	}

	private long getAndAdd(int stride) throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(true);
			return sequence.getAndAdd(connection, stride);
		}
	}

	@Override
	public Promise<Long> createId() {
		checkState(next <= limit, "Cannot create id larger than the limit of " + limit);
		if (next < limit) {
			return Promise.of(next++);
		}
		return retry(
				isResultOrException(Objects::nonNull),
				() -> reserveId.run()
						.map($ -> next < limit ? next++ : null));
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute
	public PromiseStats getPromiseCreateId() {
		return promiseCreateId;
	}

	@JmxAttribute
	public int getStride() {
		return stride;
	}

	@JmxAttribute
	public void setStride(int stride) {
		this.stride = stride;
	}

	@JmxAttribute
	public long getNext() {
		return next;
	}

	@JmxAttribute
	public long getLimit() {
		return limit;
	}
}
