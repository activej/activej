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

package io.activej.fs.exception;

import com.dslplatform.json.JsonConverter;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.*;

@SuppressWarnings("unused")
@JsonConverter(target = FileSystemException.class)
public abstract class FileSystemExceptionConverter {
	private static final String MESSAGE = "message";
	private static final String EXCEPTIONS = "exceptions";

	public static final JsonReader.ReadObject<FileSystemException> JSON_READER = FileSystemExceptionConverter::readFileSystemException;

	public static final JsonWriter.WriteObject<FileSystemException> JSON_WRITER = FileSystemExceptionConverter::writeFileSystemException;

	private static FileSystemException readFileSystemException(JsonReader<?> reader) throws IOException {
		if (reader.wasNull()) return null;
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();

		String type = reader.readKey();
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();

		FileSystemException exception;

		String key = reader.readKey();
		if (type.equals("FileSystemBatchException")) {
			if (!key.equals(EXCEPTIONS)) throw reader.newParseError("Expected key '" + EXCEPTIONS + '\'');
			exception = new FileSystemBatchException(readExceptions(reader), false);
		} else {
			if (!key.equals(MESSAGE)) throw reader.newParseError("Expected key '" + MESSAGE + '\'');
			String message = reader.readString();
			exception = switch (type) {
				case "FileSystemException" -> new FileSystemException(message, false);
				case "FileNotFoundException" -> new FileNotFoundException(message, false);
				case "ForbiddenPathException" -> new ForbiddenPathException(message, false);
				case "FileSystemIOException" -> new FileSystemIOException(message, false);
				case "FileSystemScalarException" -> new FileSystemScalarException(message, false);
				case "FileSystemStateException" -> new FileSystemStateException(message, false);
				case "IllegalOffsetException" -> new IllegalOffsetException(message, false);
				case "IsADirectoryException" -> new IsADirectoryException(message, false);
				case "MalformedGlobException" -> new MalformedGlobException(message, false);
				case "PathContainsFileException" -> new PathContainsFileException(message, false);
				default -> throw ParsingException.create("Unknown type: " + type, true);
			};
		}
		reader.endObject();
		reader.endObject();
		return exception;
	}

	private static void writeFileSystemException(JsonWriter writer, FileSystemException value) {
		if (value == null) {
			writer.writeNull();
			return;
		}
		writer.writeByte(OBJECT_START);
		writer.writeString(value.getClass().getSimpleName());
		writer.writeByte(SEMI);
		writer.writeByte(OBJECT_START);
		if (value instanceof FileSystemBatchException) {
			writer.writeString(EXCEPTIONS);
			writer.writeByte(SEMI);
			writeExceptions(writer, ((FileSystemBatchException) value).getExceptions());
		} else {
			writer.writeString(MESSAGE);
			writer.writeByte(SEMI);
			writer.writeString(value.getMessage());
		}
		writer.writeByte(OBJECT_END);
		writer.writeByte(OBJECT_END);
	}

	private static Map<String, FileSystemScalarException> readExceptions(JsonReader<?> reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		if (reader.getNextToken() == OBJECT_END) return Map.of();
		Map<String, FileSystemScalarException> res = new LinkedHashMap<>();
		String key = reader.readKey();
		res.put(key, readScalarException(reader));
		while (reader.getNextToken() == COMMA) {
			reader.getNextToken();
			key = reader.readKey();
			res.put(key, readScalarException(reader));
		}
		if (reader.last() != OBJECT_END) throw reader.newParseError("Expected '}'");
		return res;
	}

	private static void startObject2(JsonReader<?> reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();
	}

	private static void writeExceptions(JsonWriter writer, Map<String, FileSystemScalarException> exceptions) {
		writer.writeByte(OBJECT_START);
		if (!exceptions.isEmpty()) {
			boolean isFirst = true;

			for (Map.Entry<String, FileSystemScalarException> entry : exceptions.entrySet()) {
				if (!isFirst) writer.writeByte(COMMA);
				isFirst = false;
				writer.writeString(entry.getKey());
				writer.writeByte(SEMI);
				writeFileSystemException(writer, entry.getValue());
			}
		}
		writer.writeByte(OBJECT_END);
	}

	private static FileSystemScalarException readScalarException(JsonReader<?> reader) throws IOException {
		FileSystemException exception = readFileSystemException(reader);
		if (exception instanceof FileSystemScalarException) return (FileSystemScalarException) exception;
		throw ParsingException.create("Expected exception to be instance of FileSystemScalarException", true);
	}
}
