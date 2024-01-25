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

package io.activej.cube.ot.sql;

import io.activej.common.Checks;
import io.activej.cube.aggregation.ChunkIdGenerator;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.activej.promise.PromisePredicates.isResultOrException;
import static io.activej.promise.Promises.retry;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class SqlChunkIdGenerator extends AbstractReactive
	implements ChunkIdGenerator, ReactiveJmxBeanWithStats {

	private static final boolean CHECKS = Checks.isEnabled(SqlChunkIdGenerator.class);

	private final Executor executor;
	private final DataSource dataSource;

	private final SqlAtomicSequence sequence;

	private final PromiseStats promiseCreateId = PromiseStats.create(Duration.ofMinutes(5));

	private SqlChunkIdGenerator(Reactor reactor, Executor executor, DataSource dataSource, SqlAtomicSequence sequence) {
		super(reactor);
		this.executor = executor;
		this.dataSource = dataSource;
		this.sequence = sequence;
	}

	public static SqlChunkIdGenerator create(
		Reactor reactor, Executor executor, DataSource dataSource, SqlAtomicSequence sequence
	) {
		return new SqlChunkIdGenerator(reactor, executor, dataSource, sequence);
	}

	@Override
	public Promise<String> createProtoChunkId() {
		return Promise.of(UUID.randomUUID().toString());
	}

	@Override
	public Promise<Map<String, Long>> convertToActualChunkIds(Set<String> protoChunkIds) {
		if (CHECKS) checkInReactorThread(this);
		return retry(
			isResultOrException(Objects::nonNull),
			() -> doReserveId(protoChunkIds.size())
				.map(from -> {
					Iterator<String> protoChunkIdsIt = protoChunkIds.iterator();
					return LongStream.range(from, from + protoChunkIds.size())
						.boxed()
						.collect(Collectors.toMap($ -> protoChunkIdsIt.next(), Function.identity()));
				}));
	}

	private Promise<Long> doReserveId(int size) {
		return Promise.ofBlocking(executor, () -> getAndAdd(size));
	}

	private long getAndAdd(int size) throws SQLException {
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(true);
			return sequence.getAndAdd(connection, size);
		}
	}

	@JmxAttribute
	public PromiseStats getPromiseCreateId() {
		return promiseCreateId;
	}
}
