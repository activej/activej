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

package io.activej.ot;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.process.AsyncExecutors;
import io.activej.async.service.EventloopService;
import io.activej.common.exception.UncheckedException;
import io.activej.eventloop.Eventloop;
import io.activej.ot.exceptions.OTTransformException;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.OTUplink;
import io.activej.ot.uplink.OTUplink.FetchData;
import io.activej.promise.Promise;
import io.activej.promise.RetryPolicy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.activej.async.function.AsyncSuppliers.reuse;
import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.nullToEmpty;
import static io.activej.common.collection.CollectionUtils.concat;
import static io.activej.promise.Promises.sequence;
import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> implements EventloopService {
	private static final Logger logger = LoggerFactory.getLogger(OTStateManager.class);

	private final Eventloop eventloop;
	private final OTSystem<D> otSystem;
	private final OTUplink<K, D, Object> uplink;

	private OTState<D> state;

	@Nullable
	private K commitId;
	private long level;

	private List<D> workingDiffs = new ArrayList<>();

	@Nullable
	private Object pendingProtoCommit;
	@Nullable
	private List<D> pendingProtoCommitDiffs;

	private final AsyncSupplier<Void> sync = reuse(this::doSync);
	private boolean isSyncing;

	@Nullable
	private AsyncSupplier<Void> poll;
	private boolean isPolling;

	@SuppressWarnings("unchecked")
	private OTStateManager(Eventloop eventloop, OTSystem<D> otSystem, OTUplink<K, D, ?> uplink, OTState<D> state) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.uplink = (OTUplink<K, D, Object>) uplink;
		this.state = state;
	}

	@NotNull
	public static <K, D> OTStateManager<K, D> create(@NotNull Eventloop eventloop, @NotNull OTSystem<D> otSystem, @NotNull OTUplink<K, D, ?> repository,
			@NotNull OTState<D> state) {
		return new OTStateManager<>(eventloop, otSystem, repository, state);
	}

	@NotNull
	public OTStateManager<K, D> withPoll() {
		return withPoll(Function.identity());
	}

	@NotNull
	public OTStateManager<K, D> withPoll(@NotNull RetryPolicy<?> pollRetryPolicy) {
		return withPoll(poll -> poll.withExecutor(AsyncExecutors.retry(pollRetryPolicy)));
	}

	@NotNull
	public OTStateManager<K, D> withPoll(@NotNull Function<AsyncSupplier<Void>, AsyncSupplier<Void>> pollPolicy) {
		this.poll = pollPolicy.apply(this::doPoll);
		return this;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return checkout()
				.whenResult(this::poll);
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		poll = null;
		return isValid() ?
				sync().whenComplete(this::invalidateInternalState) :
				Promise.complete();
	}

	@NotNull
	public Promise<Void> checkout() {
		checkState(commitId == null);
		return uplink.checkout()
				.whenResult(checkoutData -> {
					state.init();
					apply(checkoutData.getDiffs());

					workingDiffs.clear();

					commitId = checkoutData.getCommitId();
					level = checkoutData.getLevel();
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	private boolean isSyncing() {
		return isSyncing;
	}

	private boolean isPolling() {
		return isPolling;
	}

	@NotNull
	public Promise<Void> sync() {
		return sync.get();
	}

	@NotNull
	private Promise<Void> doSync() {
		checkState(isValid());
		isSyncing = true;
		return sequence(
				this::push,
				poll == null ? this::fetch : Promise::complete,
				this::commit,
				this::push)
				.whenComplete(() -> isSyncing = false)
				.whenComplete(this::poll)
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private void poll() {
		if (poll != null && !isPolling()) {
			isPolling = true;
			poll.get()
					.async()
					.whenComplete(() -> isPolling = false)
					.whenComplete(() -> {
						if (!isSyncing()) {
							poll();
						}
					});
		}
	}

	@NotNull
	private Promise<Void> fetch() {
		K fetchCommitId = this.commitId;
		return uplink.fetch(fetchCommitId)
				.whenResult(fetchData -> rebase(fetchCommitId, fetchData))
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	@NotNull
	private Promise<Void> doPoll() {
		if (!isValid()) return Promise.complete();
		K pollCommitId = this.commitId;
		return uplink.poll(pollCommitId)
				.whenResult(fetchData -> {
					if (!isSyncing()) {
						rebase(pollCommitId, fetchData);
					}
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	private void rebase(K originalCommitId, FetchData<K, D> fetchData) {
		logger.info("Rebasing - {} {}", originalCommitId, fetchData);
		if (commitId != originalCommitId) return;
		if (pendingProtoCommit != null) return;

		List<D> fetchedDiffs = fetchData.getDiffs();

		TransformResult<D> transformed;
		try {
			transformed = otSystem.transform(
					otSystem.squash(workingDiffs),
					otSystem.squash(fetchedDiffs));
		} catch (OTTransformException e) {
			invalidateInternalState();
			throw new UncheckedException(e);
		}

		apply(transformed.left);
		workingDiffs = new ArrayList<>(transformed.right);

		commitId = fetchData.getCommitId();
		level = fetchData.getLevel();
	}

	@NotNull
	private Promise<Void> commit() {
		assert pendingProtoCommit == null;
		if (workingDiffs.isEmpty()) return Promise.complete();
		int originalSize = workingDiffs.size();
		List<D> diffs = new ArrayList<>(otSystem.squash(workingDiffs));
		return uplink.createProtoCommit(this.commitId, diffs, level)
				.whenResult(protoCommit -> {
					assert pendingProtoCommit == null;
					pendingProtoCommit = protoCommit;
					pendingProtoCommitDiffs = diffs;
					workingDiffs = new ArrayList<>(workingDiffs.subList(originalSize, workingDiffs.size()));
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	@NotNull
	private Promise<Void> push() {
		if (pendingProtoCommit == null) return Promise.complete();
		K currentCommitId = this.commitId;
		return uplink.push(pendingProtoCommit)
				.whenResult(fetchData -> {
					pendingProtoCommit = null;
					pendingProtoCommitDiffs = null;
					rebase(currentCommitId, fetchData);
				})
				.toVoid()
				.whenComplete(toLogger(logger, thisMethod(), this));
	}

	public void reset() {
		checkState(!isSyncing());
		apply(otSystem.invert(
				concat(nullToEmpty(pendingProtoCommitDiffs), workingDiffs)));
		workingDiffs.clear();
		pendingProtoCommit = null;
		pendingProtoCommitDiffs = null;
	}

	public void add(@NotNull D diff) {
		checkState(isValid());
		addAll(singletonList(diff));
	}

	public void addAll(@NotNull List<? extends D> diffs) {
		checkState(isValid());
		try {
			for (D diff : diffs) {
				if (!otSystem.isEmpty(diff)) {
					workingDiffs.add(diff);
					state.apply(diff);
				}
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	private void apply(List<D> diffs) {
		try {
			for (D op : diffs) {
				state.apply(op);
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	@SuppressWarnings("AssignmentToNull") // state is invalid, no further calls should be made
	public void invalidateInternalState() {
		state = null;

		commitId = null;
		level = 0;
		workingDiffs = null;

		pendingProtoCommit = null;
		pendingProtoCommitDiffs = null;

		poll = null;
	}

	public K getCommitId() {
		return checkNotNull(commitId, "Internal state has been invalidated");
	}

	public OTState<D> getState() {
		return state;
	}

	public boolean isValid() {
		return commitId != null;
	}

	public boolean hasWorkingDiffs() {
		return !workingDiffs.isEmpty();
	}

	public boolean hasPendingCommits() {
		return pendingProtoCommit != null;
	}

	@Override
	public String toString() {
		return "{" +
				"revision=" + commitId +
				" workingDiffs:" + (workingDiffs != null ? workingDiffs.size() : null) +
				" pendingCommits:" + (pendingProtoCommit != null) +
				"}";
	}
}
