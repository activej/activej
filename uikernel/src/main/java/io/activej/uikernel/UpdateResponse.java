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

package io.activej.uikernel;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class UpdateResponse<K, R extends AbstractRecord<K>> {
	private final List<R> changes;
	private final Map<K, Map<String, List<String>>> errors;

	private UpdateResponse(@NotNull List<R> changes, @NotNull Map<K, Map<String, List<String>>> errors) {
		this.changes = changes;
		this.errors = errors;
	}

	public static <K, R extends AbstractRecord<K>> UpdateResponse<K, R> of(List<R> changes) {
		return new UpdateResponse<>(changes, Collections.emptyMap());
	}

	public static <K, R extends AbstractRecord<K>> UpdateResponse<K, R> of(List<R> changes, Map<K, Map<String, List<String>>> errors) {
		return new UpdateResponse<>(changes, errors);
	}

	String toJson(Gson gson, Class<R> type, Class<K> idType) {
		JsonObject result = new JsonObject();

		JsonArray change = new JsonArray();
		for (R record : changes) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(record.getId(), idType));
			arr.add(gson.toJsonTree(record, type));
			change.add(arr);
		}
		result.add("changes", change);

		JsonArray errs = new JsonArray();
		for (Map.Entry<K, Map<String, List<String>>> entry : errors.entrySet()) {
			JsonArray arr = new JsonArray();
			arr.add(gson.toJsonTree(entry.getKey(), idType));
			arr.add(gson.toJsonTree(entry.getValue()));
			errs.add(arr);
		}
		result.add("errors", errs);

		return gson.toJson(result);
	}
}
