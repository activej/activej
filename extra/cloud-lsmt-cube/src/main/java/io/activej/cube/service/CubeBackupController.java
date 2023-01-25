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

package io.activej.cube.service;

import io.activej.aggregation.AggregationChunkStorage;
import io.activej.async.function.AsyncRunnable;
import io.activej.common.Utils;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.ot.OTCommit;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.activej.async.function.AsyncRunnables.reuse;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Utils.first;
import static io.activej.cube.Utils.chunksInDiffs;
import static io.activej.ot.OTAlgorithms.checkout;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class CubeBackupController<K, D, C> extends AbstractReactive
		implements ReactiveJmxBeanWithStats {
	private static final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final OTSystem<D> otSystem;
	private final AsyncOTRepository<K, D> repository;
	private final AggregationChunkStorage<C> storage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private final PromiseStats promiseBackup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupDb = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeBackupController(Reactor reactor,
			CubeDiffScheme<D> cubeDiffScheme, AsyncOTRepository<K, D> repository, OTSystem<D> otSystem, AggregationChunkStorage<C> storage) {
		super(reactor);
		this.cubeDiffScheme = cubeDiffScheme;
		this.otSystem = otSystem;
		this.repository = repository;
		this.storage = storage;
	}

	public static <K, D, C> CubeBackupController<K, D, C> create(Reactor reactor,
			CubeDiffScheme<D> cubeDiffScheme,
			AsyncOTRepository<K, D> otRepository,
			OTSystem<D> otSystem,
			AggregationChunkStorage<C> storage) {
		return new CubeBackupController<>(reactor, cubeDiffScheme, otRepository, otSystem, storage);
	}

	private final AsyncRunnable backup = reuse(this::backupHead);

	@SuppressWarnings("UnusedReturnValue")
	public Promise<Void> backup() {
		checkInReactorThread(this);
		return backup.run();
	}

	public Promise<Void> backupHead() {
		checkInReactorThread(this);
		return repository.getHeads()
				.mapException(e -> new CubeException("Failed to get heads", e))
				.whenResult(Set::isEmpty, $ -> {
					throw new CubeException("Heads are empty");
				})
				.then(heads -> backup(first(heads)))
				.whenComplete(promiseBackup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public Promise<Void> backup(K commitId) {
		checkInReactorThread(this);
		return Promises.toTuple(repository.loadCommit(commitId), checkout(repository, otSystem, commitId))
				.mapException(e -> new CubeException("Failed to check out commit '" + commitId + '\'', e))
				.then(tuple -> Promises.sequence(
						() -> backupChunks(commitId, chunksInDiffs(cubeDiffScheme, tuple.value2())),
						() -> backupDb(tuple.value1(), tuple.value2())))
				.whenComplete(toLogger(logger, thisMethod(), commitId));
	}

	private Promise<Void> backupChunks(K commitId, Set<C> chunkIds) {
		return storage.backup(String.valueOf(commitId), chunkIds)
				.mapException(e -> new CubeException("Failed to backup chunks on storage: " + storage, e))
				.whenComplete(promiseBackupChunks.recordStats())
				.whenComplete(logger.isTraceEnabled() ?
						toLogger(logger, TRACE, thisMethod(), chunkIds) :
						toLogger(logger, thisMethod(), Utils.toString(chunkIds)));
	}

	private Promise<Void> backupDb(OTCommit<K, D> commit, List<D> snapshot) {
		return repository.backup(commit, snapshot)
				.mapException(e -> new CubeException("Failed to backup chunks in repository: " + repository, e))
				.whenComplete(promiseBackupDb.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), commit, snapshot));
	}

	@JmxOperation
	public void backupNow() {
		backup();
	}

	@JmxAttribute
	public PromiseStats getPromiseBackup() {
		return promiseBackup;
	}

	@JmxAttribute
	public PromiseStats getPromiseBackupDb() {
		return promiseBackupDb;
	}

	@JmxAttribute
	public PromiseStats getPromiseBackupChunks() {
		return promiseBackupChunks;
	}

}

