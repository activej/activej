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

import io.activej.async.function.AsyncPredicate;
import io.activej.common.function.FunctionEx;
import io.activej.common.ref.Ref;
import io.activej.ot.AsyncOTCommitFactory.DiffsWithLevel;
import io.activej.ot.OTCommit;
import io.activej.ot.PollSanitizer;
import io.activej.ot.reducers.DiffsReducer;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static io.activej.async.util.LogUtils.thisMethod;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Utils.*;
import static io.activej.common.exception.FatalErrorHandlers.handleError;
import static io.activej.ot.OTAlgorithms.*;
import static io.activej.ot.reducers.DiffsReducer.toSquashedList;
import static io.activej.promise.PromisePredicates.isResultOrException;
import static io.activej.promise.Promises.retry;
import static io.activej.reactor.Reactive.checkInReactorThread;

public final class OTUplink<K, D, PC> extends AbstractReactive
		implements AsyncOTUplink<K, D, PC> {
	private static final Logger logger = LoggerFactory.getLogger(OTUplink.class);

	private final OTSystem<D> otSystem;
	private final AsyncOTRepository<K, D> repository;
	private final FunctionEx<OTCommit<K, D>, PC> protoCommitEncoder;
	private final FunctionEx<PC, OTCommit<K, D>> protoCommitDecoder;

	private OTUplink(Reactor reactor, AsyncOTRepository<K, D> repository, OTSystem<D> otSystem, FunctionEx<OTCommit<K, D>, PC> protoCommitEncoder,
			FunctionEx<PC, OTCommit<K, D>> protoCommitDecoder) {
		super(reactor);
		this.otSystem = otSystem;
		this.repository = repository;
		this.protoCommitEncoder = protoCommitEncoder;
		this.protoCommitDecoder = protoCommitDecoder;
	}

	public static <K, D, C> OTUplink<K, D, C> create(Reactor reactor, AsyncOTRepository<K, D> repository, OTSystem<D> otSystem,
			FunctionEx<OTCommit<K, D>, C> commitToObject, FunctionEx<C, OTCommit<K, D>> objectToCommit) {
		return new OTUplink<>(reactor, repository, otSystem, commitToObject, objectToCommit);
	}

	public static <K, D> OTUplink<K, D, OTCommit<K, D>> create(Reactor reactor, AsyncOTRepository<K, D> repository, OTSystem<D> otSystem) {
		return new OTUplink<>(reactor, repository, otSystem, commit -> commit, object -> object);
	}

	public AsyncOTRepository<K, D> getRepository() {
		return repository;
	}

	@Override
	public Promise<PC> createProtoCommit(K parent, List<D> diffs, long parentLevel) {
		checkInReactorThread(this);
		return repository.createCommit(parent, new DiffsWithLevel<>(parentLevel, diffs))
				.map(protoCommitEncoder)
				.whenComplete(toLogger(logger, thisMethod(), parent, diffs, parentLevel));
	}

	@Override
	public Promise<FetchData<K, D>> push(PC protoCommit) {
		checkInReactorThread(this);
		OTCommit<K, D> commit;
		try {
			commit = protoCommitDecoder.apply(protoCommit);
		} catch (Exception ex) {
			handleError(ex, this);
			return Promise.ofException(ex);
		}
		return repository.push(commit)
				.then(repository::getHeads)
				.then(initialHeads -> excludeParents(repository, otSystem, union(initialHeads, Set.of(commit.getId())))
						.then(heads -> mergeAndPush(repository, otSystem, heads))
						.then(mergeHead -> {
							Set<K> mergeHeadSet = Set.of(mergeHead);
							return repository.updateHeads(mergeHeadSet, difference(initialHeads, mergeHeadSet))
									.then(() -> doFetch(mergeHeadSet, commit.getId()));
						}))
				.whenComplete(toLogger(logger, thisMethod(), protoCommit));
	}

	@Override
	public Promise<FetchData<K, D>> checkout() {
		checkInReactorThread(this);
		Ref<List<D>> cachedSnapshotRef = new Ref<>();
		return repository.getHeads()
				.then(heads -> findParent(
						repository,
						otSystem,
						heads,
						DiffsReducer.toList(),
						commit -> repository.loadSnapshot(commit.getId())
								.map(maybeSnapshot -> (cachedSnapshotRef.value = maybeSnapshot.orElse(null)) != null)))
				.then(findResult -> Promise.of(
						new FetchData<>(
								findResult.getChild(),
								findResult.getChildLevel(),
								concat(cachedSnapshotRef.value, findResult.getAccumulatedDiffs()))))
				.then(checkoutData -> fetch(checkoutData.commitId())
						.map(fetchData -> new FetchData<>(
								fetchData.commitId(),
								fetchData.level(),
								otSystem.squash(concat(checkoutData.diffs(), fetchData.diffs()))
						))
				)
				.whenComplete(toLogger(logger, thisMethod()));
	}

	@Override
	public Promise<FetchData<K, D>> fetch(K currentCommitId) {
		checkInReactorThread(this);
		return repository.getHeads()
				.then(heads -> doFetch(heads, currentCommitId))
				.whenComplete(toLogger(logger, thisMethod(), currentCommitId));
	}

	@Override
	public Promise<FetchData<K, D>> poll(K currentCommitId) {
		checkInReactorThread(this);
		return retry(
				isResultOrException((Set<K> polledHeads) -> !polledHeads.contains(currentCommitId)),
				PollSanitizer.create(repository.pollHeads()))
				.then(heads -> doFetch(heads, currentCommitId));
	}

	private Promise<FetchData<K, D>> doFetch(Set<K> heads, K currentCommitId) {
		return findParent(
				repository,
				otSystem,
				heads,
				toSquashedList(otSystem),
				AsyncPredicate.of(commit -> commit.getId().equals(currentCommitId)))
				.map(findResult -> new FetchData<>(
						findResult.getChild(),
						findResult.getChildLevel(),
						otSystem.squash(findResult.getAccumulatedDiffs())
				))
				.whenComplete(toLogger(logger, thisMethod(), currentCommitId));
	}
}
