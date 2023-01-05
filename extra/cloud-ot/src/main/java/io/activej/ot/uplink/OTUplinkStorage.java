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

package io.activej.ot.uplink;

import io.activej.ot.TransformResult;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.OTUplinkStorage.AsyncStorage.SyncData;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.activej.common.Utils.concat;
import static io.activej.promise.PromisePredicates.isResultOrException;
import static io.activej.promise.Promises.retry;

@SuppressWarnings("WeakerAccess")
public final class OTUplinkStorage<K, D> implements AsyncOTUplink<Long, D, OTUplinkStorage.ProtoCommit<D>> {

	public static final long FIRST_COMMIT_ID = 1L;
	public static final int NO_LEVEL = 0;

	public static final class ProtoCommit<D> {
		private final long id;
		private final List<D> diffs;

		private ProtoCommit(long id, List<D> diffs) {
			this.id = id;
			this.diffs = diffs;
		}

		public long getId() {
			return id;
		}

		public List<D> getDiffs() {
			return diffs;
		}
	}

	public interface AsyncStorage<K, D> {
		Promise<Boolean> init(long commitId, List<D> snapshot, K uplinkCommitId, long uplinkLevel);

		Promise<@Nullable FetchData<Long, D>> getSnapshot();

		Promise<Long> getHead();

		default Promise<FetchData<Long, D>> fetch(long commitId) {
			List<D> diffs = new ArrayList<>();
			return getHead()
					.then(headCommitId ->
							Promises.loop(commitId + 1L,
											i -> i <= headCommitId,
											i -> getCommit(i)
													.map(commit -> {
														diffs.addAll(commit.getDiffs());
														return i + 1;
													}))
									.map($ -> new FetchData<>(headCommitId, NO_LEVEL, diffs)));
		}

		default Promise<FetchData<Long, D>> poll(long currentCommitId) {
			return fetch(currentCommitId);
		}

		Promise<ProtoCommit<D>> getCommit(long commitId);

		Promise<Boolean> add(long commitId, List<D> diffs);

		final class SyncData<K, D> {
			private final long commitId;
			private final K uplinkCommitId;
			private final long uplinkLevel;
			private final List<D> uplinkDiffs;
			private final @Nullable Object protoCommit;

			public SyncData(long id, K uplinkCommitId, long uplinkLevel, List<D> uplinkDiffs, @Nullable Object protoCommit) {
				this.commitId = id;
				this.uplinkCommitId = uplinkCommitId;
				this.uplinkLevel = uplinkLevel;
				this.uplinkDiffs = uplinkDiffs;
				this.protoCommit = protoCommit;
			}

			public long getCommitId() {
				return commitId;
			}

			public K getUplinkCommitId() {
				return uplinkCommitId;
			}

			public long getUplinkLevel() {
				return uplinkLevel;
			}

			public List<D> getUplinkDiffs() {
				return uplinkDiffs;
			}

			public @Nullable Object getProtoCommit() {
				return protoCommit;
			}

			public boolean isSyncing() {
				return protoCommit != null;
			}
		}

		Promise<SyncData<K, D>> getSyncData();

		default Promise<Boolean> isSyncing() {
			return getSyncData().map(SyncData::isSyncing);
		}

		Promise<Void> startSync(long headCommitId, K uplinkCommitId, Object protoCommit);

		Promise<Boolean> completeSync(long commitId, List<D> diffs, K uplinkCommitId, long uplinkLevel, List<D> uplinkDiffs);
	}

	private final AsyncStorage<K, D> storage;

	private final OTSystem<D> otSystem;
	private final AsyncOTUplink<K, D, Object> uplink;

	private OTUplinkStorage(AsyncStorage<K, D> storage, OTSystem<D> otSystem, AsyncOTUplink<K, D, ?> uplink) {
		this.otSystem = otSystem;
		this.storage = storage;
		//noinspection unchecked
		this.uplink = (AsyncOTUplink<K, D, Object>) uplink;
	}

	public Promise<Void> sync() {
		return startSync()
				.then(syncData -> uplink.push(syncData.getProtoCommit())
						.then(uplinkFetchedData -> Promise.ofCallback(cb ->
								completeSync(syncData.getCommitId(), new ArrayList<>(), uplinkFetchedData.getCommitId(), uplinkFetchedData.getLevel(), uplinkFetchedData.getDiffs(), cb))));
	}

	Promise<SyncData<K, D>> startSync() {
		return storage.getSyncData()
				.thenIf(syncData -> syncData.getProtoCommit() == null,
						syncData -> storage.fetch(syncData.getCommitId())
								.then(fetchedData -> {
									long headCommitId = fetchedData.getCommitId();
									List<D> diffs = fetchedData.getDiffs();
									return uplink.createProtoCommit(syncData.getUplinkCommitId(), concat(syncData.getUplinkDiffs(), diffs), 0)
											.then(protoCommit ->
													storage.startSync(headCommitId, syncData.getUplinkCommitId(), protoCommit)
															.map($ -> new SyncData<>(headCommitId, syncData.getUplinkCommitId(), syncData.getUplinkLevel(), diffs, protoCommit)));
								}));
	}

	void completeSync(long commitId, List<D> accumulatedDiffs, K uplinkCommitId, long uplinkLevel, List<D> uplinkDiffs, SettablePromise<Void> cb) {
		storage.fetch(commitId)
				.whenResult(fetchData -> {
					TransformResult<D> transformResult = otSystem.transform(uplinkDiffs, fetchData.getDiffs());

					accumulatedDiffs.addAll(transformResult.left);
					storage.completeSync(fetchData.getCommitId(), accumulatedDiffs, uplinkCommitId, uplinkLevel, transformResult.right)
							.whenResult(ok -> {
								if (ok) {
									cb.set(null);
								} else {
									completeSync(commitId, accumulatedDiffs, uplinkCommitId, uplinkLevel, transformResult.right, cb);
								}
							})
							.whenException(cb::setException);
				})
				.whenException(cb::setException);
	}

	@Override
	public Promise<FetchData<Long, D>> checkout() {
		//noinspection ConstantConditions
		return retry(
				isResultOrException(Objects::nonNull),
				() -> storage.getSnapshot()
						.thenIfNull(() -> uplink.checkout()
								.then(uplinkSnapshotData -> storage.init(FIRST_COMMIT_ID, uplinkSnapshotData.getDiffs(), uplinkSnapshotData.getCommitId(), uplinkSnapshotData.getLevel())
										.mapIfElse(ok -> ok,
												$ -> new FetchData<>(FIRST_COMMIT_ID, NO_LEVEL, uplinkSnapshotData.getDiffs()),
												$ -> null))))
				.then(snapshotData -> storage.fetch(snapshotData.getCommitId())
						.map(fetchData ->
								new FetchData<>(fetchData.getCommitId(), NO_LEVEL, concat(snapshotData.getDiffs(), fetchData.getDiffs()))));
	}

	@Override
	public Promise<ProtoCommit<D>> createProtoCommit(Long parentCommitId, List<D> diffs, long parentLevel) {
		return Promise.of(new ProtoCommit<>(parentCommitId, diffs));
	}

	@Override
	public Promise<FetchData<Long, D>> push(ProtoCommit<D> protoCommit) {
		return Promise.ofCallback(cb -> doPush(protoCommit.getId(), protoCommit.getDiffs(), List.of(), cb));
	}

	void doPush(long commitId, List<D> diffs, List<D> fetchedDiffs, SettablePromise<FetchData<Long, D>> cb) {
		storage.add(commitId, diffs)
				.whenResult(ok -> {
					if (ok) {
						cb.set(new FetchData<>(commitId + 1, NO_LEVEL, fetchedDiffs));
					} else {
						storage.fetch(commitId)
								.whenResult(fetchData -> {
									TransformResult<D> transformResult = otSystem.transform(fetchData.getDiffs(), diffs);
									doPush(fetchData.getCommitId(), transformResult.left, concat(fetchedDiffs, transformResult.right), cb);
								})
								.whenException(cb::setException);
					}
				})
				.whenException(cb::setException);
	}

	@Override
	public Promise<FetchData<Long, D>> fetch(Long currentCommitId) {
		return storage.fetch(currentCommitId);
	}

	@Override
	public Promise<FetchData<Long, D>> poll(Long currentCommitId) {
		return storage.poll(currentCommitId);
	}

}
