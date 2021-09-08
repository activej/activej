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

package io.activej.fs.tcp;

import com.dslplatform.json.JsonConverter;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.ParsingException;

import java.util.Arrays;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.*;
import static io.activej.fs.tcp.RemoteFsCommands.*;
import static io.activej.fs.tcp.RemoteFsResponses.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

class RemoteFsCodec {
	@SuppressWarnings("unused")
	@JsonConverter(target = FsCommand.class)
	public static class FsCommandConverter {
		public static final ReadObject<FsCommand> JSON_READER = typedReader(
				Append.class, Copy.class, CopyAll.class, Delete.class, DeleteAll.class, Download.class,
				Info.class, InfoAll.class, List.class, Move.class, MoveAll.class, Ping.class, Upload.class
		);
		public static final WriteObject<FsCommand> JSON_WRITER = typedWriter();
	}

	@JsonConverter(target = FsResponse.class)
	public static class FsResponseConverter {
		public static final ReadObject<FsResponse> JSON_READER = typedReader(
				AppendAck.class, AppendFinished.class, CopyAllFinished.class, CopyFinished.class, DeleteAllFinished.class,
				DeleteFinished.class, DownloadSize.class, InfoAllFinished.class, InfoFinished.class, ListFinished.class,
				MoveAllFinished.class, MoveFinished.class, PingFinished.class, ServerError.class, UploadAck.class,
				UploadFinished.class
		);
		public static final WriteObject<FsResponse> JSON_WRITER = typedWriter();
	}

	@SafeVarargs
	static <T> ReadObject<T> typedReader(Class<? extends T>... types) {
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

	static <T> WriteObject<T> typedWriter() {
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
