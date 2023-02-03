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

import io.activej.promise.Promise;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static io.activej.common.Utils.keysToMap;

public interface AsyncOTCommitFactory<K, D> {
	record DiffsWithLevel<D>(long level, List<D> diffs) {
	}

	Promise<OTCommit<K, D>> createCommit(Map<K, DiffsWithLevel<D>> parentDiffs);

	default Promise<OTCommit<K, D>> createCommit(K parent, DiffsWithLevel<D> parentDiff) {
		return createCommit(Map.of(parent, parentDiff));
	}

	default Promise<OTCommit<K, D>> createCommit(Set<K> parents, Function<K, List<D>> diffs, ToLongFunction<K> level) {
		return createCommit(keysToMap(parents.stream(), parent -> new DiffsWithLevel<>(level.applyAsLong(parent), diffs.apply(parent))));
	}

	default Promise<OTCommit<K, D>> createCommit(K parent, List<D> diffs, long level) {
		return createCommit(parent, new DiffsWithLevel<>(level, diffs));
	}
}
