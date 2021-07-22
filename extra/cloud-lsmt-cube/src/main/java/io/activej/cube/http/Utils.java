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

package io.activej.cube.http;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.JsonWriter.WriteObject;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.Settings;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.exception.MalformedDataException;
import io.activej.cube.CubeQuery.Ordering;
import io.activej.cube.ReportType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

class Utils {
	static final String MEASURES_PARAM = "measures";
	static final String ATTRIBUTES_PARAM = "attributes";
	static final String WHERE_PARAM = "where";
	static final String HAVING_PARAM = "having";
	static final String SORT_PARAM = "sort";
	static final String LIMIT_PARAM = "limit";
	static final String OFFSET_PARAM = "offset";
	static final String REPORT_TYPE_PARAM = "reportType";

	private static final Pattern splitter = Pattern.compile(",");

	static String formatOrderings(List<Ordering> orderings) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Ordering ordering : orderings) {
			sb.append(first ? "" : ",").append(ordering.getField()).append(":").append(ordering.isAsc() ? "ASC" : "DESC");
			first = false;
		}
		return sb.toString();
	}

	static List<Ordering> parseOrderings(String string) throws MalformedDataException {
		List<Ordering> result = new ArrayList<>();
		List<String> tokens = splitter.splitAsStream(string)
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.collect(toList());
		for (String s : tokens) {
			int i = s.indexOf(':');
			if (i == -1) {
				throw new MalformedDataException("Failed to parse orderings, missing semicolon");
			}
			String field = s.substring(0, i);
			String tail = s.substring(i + 1).toLowerCase();
			if ("asc".equals(tail))
				result.add(Ordering.asc(field));
			else if ("desc".equals(tail))
				result.add(Ordering.desc(field));
			else {
				throw new MalformedDataException("Tail is neither 'asc' nor 'desc'");
			}
		}
		return result;
	}

	static int parseNonNegativeInteger(String parameter) throws MalformedDataException {
		try {
			int value = Integer.parseInt(parameter);
			if (value < 0) throw new MalformedDataException("Must be non negative value: " + parameter);
			return value;
		} catch (NumberFormatException e) {
			throw new MalformedDataException("Could not parse: " + parameter, e);
		}
	}

	static ReportType parseReportType(String parameter) throws MalformedDataException {
		try {
			return ReportType.valueOf(parameter.toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new MalformedDataException("'" + parameter + "' neither of: " + Arrays.toString(ReportType.values()), e);
		}
	}

	public static final DslJson<?> CUBE_DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(CUBE_DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(CUBE_DSL_JSON::newReader);

	public static <T> String toJson(@NotNull WriteObject<T> writeObject, @Nullable T object) {
		return toJsonWriter(writeObject, object).toString();
	}

	public static <T> ByteBuf toJsonBuf(@NotNull WriteObject<T> writeObject, @Nullable T object) {
		return ByteBuf.wrapForReading(toJsonWriter(writeObject, object).toByteArray());
	}

	private static <T> JsonWriter toJsonWriter(@NotNull WriteObject<T> writeObject, @Nullable T object) {
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		writeObject.write(jsonWriter, object);
		return jsonWriter;
	}

	public static <T> T fromJson(@NotNull ReadObject<T> readObject, @NotNull ByteBuf jsonBuf) throws MalformedDataException {
		return fromJson(readObject, jsonBuf.getArray());
	}

	public static <T> T fromJson(@NotNull ReadObject<T> readObject, String json) throws MalformedDataException {
		return fromJson(readObject, json.getBytes(UTF_8));
	}

	private static <T> T fromJson(@NotNull ReadObject<T> readObject, byte[] bytes) throws MalformedDataException {
		JsonReader<?> jsonReader = READERS.get().process(bytes, bytes.length);
		try {
			jsonReader.getNextToken();
			T deserialized = readObject.read(jsonReader);
			if (jsonReader.length() != jsonReader.getCurrentIndex()) {
				String unexpectedData = jsonReader.toString().substring(jsonReader.getCurrentIndex());
				throw new MalformedDataException("Unexpected JSON data: " + unexpectedData);
			}
			return deserialized;
		} catch (ParsingException e) {
			throw new MalformedDataException(e);
		} catch (IOException e) {
			throw new AssertionError(e);
		}
	}
}
