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

import io.activej.promise.Promise;

import java.util.List;

public interface AsyncOTUplink<K, D, PC> {

	record FetchData<K, D>(K commitId, long level, List<D> diffs) {
	}

	Promise<FetchData<K, D>> checkout();

	Promise<FetchData<K, D>> fetch(K currentCommitId);

	default Promise<FetchData<K, D>> poll(K currentCommitId) {
		return fetch(currentCommitId);
	}

	Promise<PC> createProtoCommit(K parent, List<D> diffs, long parentLevel);

	Promise<FetchData<K, D>> push(PC protoCommit);

}
