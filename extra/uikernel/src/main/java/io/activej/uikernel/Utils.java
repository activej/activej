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
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import io.activej.common.exception.MalformedDataException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

class Utils {
	static <K, R extends AbstractRecord<K>> List<R> deserializeUpdateRequest(Gson gson, String json, Class<R> type, Class<K> idType) throws MalformedDataException {
		List<R> result = new ArrayList<>();
		JsonArray root = fromJson(gson, json, JsonArray.class);
		for (JsonElement element : root) {
			JsonArray arr = fromJson(gson, element, JsonArray.class);
			K id = fromJson(gson, arr.get(0), idType);
			R obj = fromJson(gson, arr.get(1), type);
			obj.setId(id);
			result.add(obj);
		}
		return result;
	}

	static <T> T fromJson(Gson gson, String json, Type typeOfT) throws MalformedDataException {
		try {
			return gson.fromJson(json, typeOfT);
		} catch (JsonSyntaxException e) {
			throw new MalformedDataException("Failed to read from JSON", e);
		}
	}

	static <T> T fromJson(Gson gson, String json, Class<T> typeOfT) throws MalformedDataException {
		try {
			return gson.fromJson(json, typeOfT);
		} catch (JsonSyntaxException e) {
			throw new MalformedDataException("Failed to read from JSON", e);
		}
	}

	static <T> T fromJson(Gson gson, JsonElement json, Type typeOfT) throws MalformedDataException {
		try {
			return gson.fromJson(json, typeOfT);
		} catch (JsonSyntaxException e) {
			throw new MalformedDataException("Failed to read from JSON", e);
		}
	}

	static <T> T fromJson(Gson gson, JsonElement json, Class<T> typeOfT) throws MalformedDataException {
		try {
			return gson.fromJson(json, typeOfT);
		} catch (JsonSyntaxException e) {
			throw new MalformedDataException("Failed to read from JSON", e);
		}
	}

}
