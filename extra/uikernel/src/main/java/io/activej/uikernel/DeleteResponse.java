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
import com.google.gson.JsonObject;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public final class DeleteResponse {
	private static final DeleteResponse OK = new DeleteResponse(List.of());

	private final List<String> errors;

	private DeleteResponse(@NotNull List<String> errors) {
		this.errors = errors;
	}

	public static DeleteResponse ok() {
		return OK;
	}

	public static DeleteResponse of(String... errors) {
		return new DeleteResponse(List.of(errors));
	}

	public static DeleteResponse of(List<String> errors) {
		return new DeleteResponse(errors);
	}

	boolean hasErrors() {
		return !errors.isEmpty();
	}

	List<String> getErrors() {
		return errors;
	}

	JsonObject toJson(Gson gson) {
		JsonObject result = new JsonObject();
		result.add("errors", gson.toJsonTree(errors));
		return result;
	}
}
