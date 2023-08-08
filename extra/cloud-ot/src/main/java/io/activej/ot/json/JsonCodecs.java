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

package io.activej.ot.json;

import io.activej.json.JsonCodec;
import io.activej.ot.uplink.AsyncOTUplink.FetchData;

import static io.activej.json.JsonCodecs.*;

public class JsonCodecs {

	public static <K, D> JsonCodec<FetchData<K, D>> ofFetchData(JsonCodec<K> idCodec, JsonCodec<D> diffCodec) {
		return ofObject(FetchData::new,
			"id", FetchData::commitId, idCodec,
			"level", FetchData::level, ofLong(),
			"diffs", FetchData::diffs, ofList(diffCodec));
	}

}
