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

package io.activej.ot.reducers;

import io.activej.ot.OTCommit;
import io.activej.promise.Promise;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.activej.ot.reducers.GraphReducer.Result.completePromise;
import static io.activej.ot.reducers.GraphReducer.Result.resumePromise;
import static java.util.Collections.singletonMap;

public abstract class AbstractGraphReducer<K, D, A, R> implements GraphReducer<K, D, R> {
	private final DiffsReducer<A, D> diffsReducer;
	private final Map<K, Map<K, A>> accumulators = new HashMap<>();
	private final Map<K, OTCommit<K, D>> headCommits = new HashMap<>();

	protected AbstractGraphReducer(DiffsReducer<A, D> diffsReducer) {
		this.diffsReducer = diffsReducer;
	}

	@Override
	public void onStart(Collection<OTCommit<K, D>> queue) {
		for (OTCommit<K, D> headCommit : queue) {
			this.headCommits.put(headCommit.getId(), headCommit);
			this.accumulators.put(headCommit.getId(), new HashMap<>(singletonMap(headCommit.getId(), diffsReducer.initialValue())));
		}
	}

	protected abstract Promise<Optional<R>> tryGetResult(OTCommit<K, D> commit, Map<K, Map<K, A>> accumulators,
			Map<K, OTCommit<K, D>> headCommits);

	@Override
	public final Promise<Result<R>> onCommit(OTCommit<K, D> commit) {
		return tryGetResult(commit, accumulators, headCommits)
				.then(result -> {
					if (result.isPresent()) return completePromise(result.get());
					Map<K, A> toHeads = accumulators.remove(commit.getId());
					for (K parent : commit.getParents().keySet()) {
						Map<K, A> parentToHeads = accumulators.computeIfAbsent(parent, $2 -> new HashMap<>());
						for (Map.Entry<K, A> entry : toHeads.entrySet()) {
							A newAccumulatedDiffs = diffsReducer.accumulate(entry.getValue(), commit.getParents().get(parent));
							A existingAccumulatedDiffs = parentToHeads.get(entry.getKey());
							A combinedAccumulatedDiffs = existingAccumulatedDiffs == null ?
									newAccumulatedDiffs :
									diffsReducer.combine(existingAccumulatedDiffs, newAccumulatedDiffs);
							parentToHeads.put(entry.getKey(), combinedAccumulatedDiffs);
						}
					}
					return resumePromise();
				});
	}
}
