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

import io.activej.aggregation.ActiveFsChunkStorage;
import io.activej.async.function.AsyncSupplier;
import io.activej.cube.exception.CubeException;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.ot.OTCommit;
import io.activej.ot.repository.OTRepositoryEx;
import io.activej.ot.system.OTSystem;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.activej.aggregation.util.Utils.wrapException;
import static io.activej.async.function.AsyncSuppliers.reuse;
import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.collection.CollectionUtils.first;
import static io.activej.common.collection.CollectionUtils.toLimitedString;
import static io.activej.cube.Utils.chunksInDiffs;
import static io.activej.ot.OTAlgorithms.checkout;

public final class CubeBackupController<K, D, C> implements EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeBackupController.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;
	private final OTSystem<D> otSystem;
	private final OTRepositoryEx<K, D> repository;
	private final ActiveFsChunkStorage<C> storage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private final PromiseStats promiseBackup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupDb = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseBackupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeBackupController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTRepositoryEx<K, D> repository,
			OTSystem<D> otSystem,
			ActiveFsChunkStorage<C> storage) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.otSystem = otSystem;
		this.repository = repository;
		this.storage = storage;
	}

	public static <K, D, C> CubeBackupController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTRepositoryEx<K, D> otRepository,
			OTSystem<D> otSystem,
			ActiveFsChunkStorage<C> storage) {
		return new CubeBackupController<>(eventloop, cubeDiffScheme, otRepository, otSystem, storage);
	}

	private final AsyncSupplier<Void> backup = reuse(this::backupHead);

	@SuppressWarnings("UnusedReturnValue")
	public Promise<Void> backup() {
		return backup.get();
	}

	public Promise<Void> backupHead() {
		return repository.getHeads()
				.thenEx(wrapException(e -> new CubeException("Failed to get heads", e)))
				.then(heads -> {
					if (heads.isEmpty()) {
						return Promise.ofException(new CubeException("Heads are empty"));
					}
					return backup(first(heads));
				})
				.whenComplete(promiseBackup.recordStats())
				.whenComplete(toLogger(logger, thisMethod()));
	}

	public Promise<Void> backup(K commitId) {
		return Promises.toTuple(repository.loadCommit(commitId), checkout(repository, otSystem, commitId))
				.thenEx(wrapException(e -> new CubeException("Failed to check out commit '" + commitId + '\'', e)))
				.then(tuple -> Promises.sequence(
						() -> backupChunks(commitId, chunksInDiffs(cubeDiffScheme, tuple.getValue2())),
						() -> backupDb(tuple.getValue1(), tuple.getValue2())))
				.whenComplete(toLogger(logger, thisMethod(), commitId));
	}

	private Promise<Void> backupChunks(K commitId, Set<C> chunkIds) {
		return storage.backup(String.valueOf(commitId), chunkIds)
				.thenEx(wrapException(e -> new CubeException("Failed to backup chunks on storage: " + storage, e)))
				.whenComplete(promiseBackupChunks.recordStats())
				.whenComplete(logger.isTraceEnabled() ?
						toLogger(logger, TRACE, thisMethod(), chunkIds) :
						toLogger(logger, thisMethod(), toLimitedString(chunkIds, 6)));
	}

	private Promise<Void> backupDb(OTCommit<K, D> commit, List<D> snapshot) {
		return repository.backup(commit, snapshot)
				.thenEx(wrapException(e -> new CubeException("Failed to backup chunks in repository: " + repository, e)))
				.whenComplete(promiseBackupDb.recordStats())
				.whenComplete(toLogger(logger, thisMethod(), commit, snapshot));
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
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

