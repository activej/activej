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

package io.activej.crdt;

import com.dslplatform.json.*;

import java.util.Arrays;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public final class CrdtMessaging {

	public interface CrdtMessage {}

	public interface CrdtResponse {}

	@CompiledJson
	public enum CrdtMessages implements CrdtMessage {
		UPLOAD,
		REMOVE,
		PING,
		TAKE,
		TAKE_FINISHED
	}

	public static final class Download implements CrdtMessage {
		private final long token;

		public Download(long token) {
			this.token = token;
		}

		public long getToken() {
			return token;
		}

		@Override
		public String toString() {
			return "Download{token=" + token + '}';
		}
	}

	@CompiledJson
	public enum CrdtResponses implements CrdtResponse {
		UPLOAD_FINISHED,
		REMOVE_FINISHED,
		PONG,
		DOWNLOAD_STARTED,
		TAKE_STARTED
	}

	public static final class ServerError implements CrdtResponse {
		private final String msg;

		public ServerError(String msg) {
			this.msg = msg;
		}

		public String getMsg() {
			return msg;
		}

		@Override
		public String toString() {
			return "ServerError{msg=" + msg + '}';
		}
	}

	@SuppressWarnings("unused")
	static class JsonConverters {
		@JsonConverter(target = CrdtMessage.class)
		public static class CrdtMessageConverter {
			public static final JsonReader.ReadObject<CrdtMessage> JSON_READER = typedReader(CrdtMessages.class, Download.class);
			public static final JsonWriter.WriteObject<CrdtMessage> JSON_WRITER = typedWriter();
		}

		@JsonConverter(target = CrdtResponse.class)
		public static class CrdtResponseConverter {
			public static final JsonReader.ReadObject<CrdtResponse> JSON_READER = typedReader(CrdtResponses.class, ServerError.class);
			public static final JsonWriter.WriteObject<CrdtResponse> JSON_WRITER = typedWriter();
		}

		@SafeVarargs
		static <T> JsonReader.ReadObject<T> typedReader(Class<? extends T>... types) {
			Map<String, Class<? extends T>> typeMap = Arrays.stream(types)
					.collect(toMap(Class::getSimpleName, identity()));

			return reader -> {
				if (reader.wasNull()) return null;
				if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
				reader.getNextToken();

				String type = reader.readString();
				reader.semicolon();

				T result;

				Class<? extends T> aClass = typeMap.get(type);
				if (aClass == null) {
					throw ParsingException.create("Unknown type: " + type, true);
				}
				//noinspection unchecked
				result = (T) reader.next(aClass);
				reader.endObject();

				return result;
			};
		}

		static <T> JsonWriter.WriteObject<T> typedWriter() {
			return (writer, value) -> {
				if (value == null) {
					writer.writeNull();
					return;
				}
				writer.writeByte(OBJECT_START);
				writer.writeString(value.getClass().getSimpleName());
				writer.writeByte(SEMI);
				writer.serializeObject(value);
				writer.writeByte(OBJECT_END);
			};
		}
	}
}
