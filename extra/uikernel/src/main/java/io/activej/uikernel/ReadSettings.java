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
import com.google.gson.reflect.TypeToken;
import io.activej.common.exception.MalformedDataException;
import io.activej.http.HttpRequest;
import io.activej.http.MalformedHttpException;

import java.lang.reflect.Type;
import java.util.*;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.http.HttpUtils.trimAndDecodePositiveInt;
import static io.activej.uikernel.Utils.fromJson;
import static java.util.Collections.unmodifiableMap;

@SuppressWarnings("unused")
public final class ReadSettings<K> {
	public enum SortOrder {
		ASCENDING, DESCENDING;

		static SortOrder of(String value) {
			if (value.equals("asc"))
				return ASCENDING;
			else
				return DESCENDING;
		}
	}

	private static final int DEFAULT_OFFSET = 0;
	private static final int DEFAULT_LIMIT = Integer.MAX_VALUE;
	private static final Type LIST_STRING_TYPE_TOKEN = new TypeToken<List<String>>() {}.getType();
	private static final Type MAP_STRING_STRING_TYPE_TOKEN = new TypeToken<LinkedHashMap<String, String>>() {}.getType();

	private final List<String> fields;
	private final int offset;
	private final int limit;
	private final Map<String, String> filters;
	private final Map<String, SortOrder> sort;
	private final Set<K> extra;

	private ReadSettings(List<String> fields,
			int offset,
			int limit,
			Map<String, String> filters,
			Map<String, SortOrder> sort,
			Set<K> extra) {
		this.fields = fields;
		this.offset = offset;
		this.limit = limit;
		this.filters = filters;
		this.sort = sort;
		this.extra = extra;
	}

	public static <K> ReadSettings<K> from(Gson gson, HttpRequest request) throws MalformedHttpException, MalformedDataException {
		String fieldsParameter = request.getQueryParameter("fields");
		List<String> fields;
		if (fieldsParameter != null && !fieldsParameter.isEmpty()) {
			fields = fromJson(gson, fieldsParameter, LIST_STRING_TYPE_TOKEN);
		} else {
			fields = List.of();
		}

		String offsetParameter = request.getQueryParameter("offset");
		int offset = DEFAULT_OFFSET;
		if (offsetParameter != null && !offsetParameter.isEmpty()) {
			offset = trimAndDecodePositiveInt(encodeAscii(offsetParameter), 0, offsetParameter.length());
		}

		String limitParameter = request.getQueryParameter("limit");
		int limit = DEFAULT_LIMIT;
		if (limitParameter != null && !limitParameter.isEmpty()) {
			limit = trimAndDecodePositiveInt(encodeAscii(limitParameter), 0, limitParameter.length());
		}

		String filtersParameter = request.getQueryParameter("filters");
		Map<String, String> filters;
		if (filtersParameter != null && !filtersParameter.isEmpty()) {
			filters = fromJson(gson, filtersParameter, MAP_STRING_STRING_TYPE_TOKEN);
			filters = unmodifiableMap(filters);
		} else {
			filters = Map.of();
		}

		String sortParameter = request.getQueryParameter("sort");
		Map<String, SortOrder> sort;
		if (sortParameter != null && !sortParameter.isEmpty()) {
			sort = new LinkedHashMap<>();
			JsonArray array = fromJson(gson, sortParameter, JsonArray.class);
			String key;
			SortOrder value;
			for (JsonElement element : array) {
				JsonArray arr = element.getAsJsonArray();
				key = arr.get(0).getAsString();

				value = SortOrder.of(arr.get(1).getAsString());
				sort.put(key, value);
			}
			sort = unmodifiableMap(sort);
		} else {
			sort = Map.of();
		}

		String extraParameter = request.getQueryParameter("extra");
		Set<K> extra;
		if (extraParameter != null && !extraParameter.isEmpty()) {
			extra = fromJson(gson, extraParameter, new TypeToken<LinkedHashSet<K>>() {}.getType());
		} else {
			extra = Set.of();
		}

		return new ReadSettings<>(fields, offset, limit, filters, sort, extra);
	}

	public static <K> ReadSettings<K> of(List<String> fields,
			int offset,
			int limit,
			Map<String, String> filters,
			Map<String, SortOrder> sort,
			Set<K> extra) {
		return new ReadSettings<>(fields, offset, limit, filters, sort, extra);
	}

	public List<String> getFields() {
		return fields;
	}

	public int getOffset() {
		return offset;
	}

	public int getLimit() {
		return limit;
	}

	public Map<String, String> getFilters() {
		return filters;
	}

	public Map<String, SortOrder> getSort() {
		return sort;
	}

	public Set<K> getExtra() {
		return extra;
	}

	@Override
	public String toString() {
		return "ReadSettings{" +
				"fields=" + fields +
				", offset=" + offset +
				", limit=" + limit +
				", filters=" + filters +
				", sort=" + sort +
				", extra=" + extra +
				'}';
	}
}
