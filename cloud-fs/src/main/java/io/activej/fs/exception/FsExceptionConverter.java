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
import io.activej.fs.tcp.FsMessagingProto.FsResponse.ServerError;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.dslplatform.json.JsonWriter.*;

@SuppressWarnings("unused")
@JsonConverter(target = FsException.class)
public abstract class FsExceptionConverter {
	private static final String MESSAGE = "message";
	private static final String EXCEPTIONS = "exceptions";

	public static final JsonReader.ReadObject<FsException> JSON_READER = FsExceptionConverter::readFsException;

	public static final JsonWriter.WriteObject<FsException> JSON_WRITER = FsExceptionConverter::writeFsException;

	private static FsException readFsException(JsonReader<?> reader) throws IOException {
		if (reader.wasNull()) return null;
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();

		String type = reader.readKey();
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		reader.getNextToken();

		FsException exception;

		String key = reader.readKey();
		if (type.equals("FsBatchException")) {
			if (!key.equals(EXCEPTIONS)) throw reader.newParseError("Expected key '" + EXCEPTIONS + '\'');
			exception = new FsBatchException(readExceptions(reader), false);
		} else {
			if (!key.equals(MESSAGE)) throw reader.newParseError("Expected key '" + MESSAGE + '\'');
			String message = reader.readString();
			exception = switch (type) {
				case "FsException" -> new FsException(message, false);
				case "FileNotFoundException" -> new FileNotFoundException(message, false);
				case "ForbiddenPathException" -> new ForbiddenPathException(message, false);
				case "FsIOException" -> new FsIOException(message, false);
				case "FsScalarException" -> new FsScalarException(message, false);
				case "FsStateException" -> new FsStateException(message, false);
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

	private static void writeFsException(JsonWriter writer, FsException value) {
		if (value == null) {
			writer.writeNull();
			return;
		}
		writer.writeByte(OBJECT_START);
		writer.writeString(value.getClass().getSimpleName());
		writer.writeByte(SEMI);
		writer.writeByte(OBJECT_START);
		if (value instanceof FsBatchException) {
			writer.writeString(EXCEPTIONS);
			writer.writeByte(SEMI);
			writeExceptions(writer, ((FsBatchException) value).getExceptions());
		} else {
			writer.writeString(MESSAGE);
			writer.writeByte(SEMI);
			writer.writeString(value.getMessage());
		}
		writer.writeByte(OBJECT_END);
		writer.writeByte(OBJECT_END);
	}

	private static Map<String, FsScalarException> readExceptions(JsonReader<?> reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		if (reader.getNextToken() == OBJECT_END) return Map.of();
		Map<String, FsScalarException> res = new LinkedHashMap<>();
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

	private static void writeExceptions(JsonWriter writer, Map<String, FsScalarException> exceptions) {
		writer.writeByte(OBJECT_START);
		if (!exceptions.isEmpty()) {
			boolean isFirst = true;

			for (Map.Entry<String, FsScalarException> entry : exceptions.entrySet()) {
				if (!isFirst) writer.writeByte(COMMA);
				isFirst = false;
				writer.writeString(entry.getKey());
				writer.writeByte(SEMI);
				writeFsException(writer, entry.getValue());
			}
		}
		writer.writeByte(OBJECT_END);
	}

	private static @NotNull FsScalarException readScalarException(JsonReader<?> reader) throws IOException {
		FsException exception = readFsException(reader);
		if (exception instanceof FsScalarException) return (FsScalarException) exception;
		throw ParsingException.create("Expected exception to be instance of FsScalarException", true);
	}

	public static FsException unwindProtobufException(ServerError serverError) {
		switch (serverError.getErrorCase()) {
			case FS_BATCH_EXCEPTION:
				Map<String, FsScalarException> exceptions = new LinkedHashMap<>();
				serverError.getFsBatchException().getExceptionsMap()
						.forEach((fileName, oneOfFsScalarExceptions) -> {
							switch (oneOfFsScalarExceptions.getExceptionCase()) {
								case FS_SCALAR_EXCEPTION:
									exceptions.put(fileName, new FsScalarException(oneOfFsScalarExceptions.getFsScalarException().getMessage(), false));
									break;
								case PATH_CONTAINS_FILE_EXCEPTION:
									exceptions.put(fileName, new PathContainsFileException(oneOfFsScalarExceptions.getPathContainsFileException().getMessage(), false));
									break;
								case ILLEGAL_OFFSET_EXCEPTION:
									exceptions.put(fileName, new IllegalOffsetException(oneOfFsScalarExceptions.getIllegalOffsetException().getMessage(), false));
									break;
								case FILE_NOT_FOUND_EXCEPTION:
									exceptions.put(fileName, new FileNotFoundException(oneOfFsScalarExceptions.getFileNotFoundException().getMessage(), false));
									break;
								case FORBIDDEN_PATH_EXCEPTION:
									exceptions.put(fileName, new ForbiddenPathException(oneOfFsScalarExceptions.getForbiddenPathException().getMessage(), false));
									break;
								case MALFORMED_GLOB_EXCEPTION:
									exceptions.put(fileName, new MalformedGlobException(oneOfFsScalarExceptions.getMalformedGlobException().getMessage(), false));
									break;
								case IS_A_DIRECTORY_EXCEPTION:
									exceptions.put(fileName, new IsADirectoryException(oneOfFsScalarExceptions.getIsADirectoryException().getMessage(), false));
									break;
								default:
									exceptions.put(fileName, new FsScalarException("Unknown exception", false));
									break;
							}
						});
				return new FsBatchException(exceptions, false);
			case FS_EXCEPTION:
				return new FsException(serverError.getFsException().getMessage(), false);
			case FS_STATE_EXCEPTION:
				return new FsStateException(serverError.getFsStateException().getMessage(), false);
			case FS_SCALAR_EXCEPTION:
				return new FsScalarException(serverError.getFsScalarException().getMessage(), false);
			case PATH_CONTAINS_FILE_EXCEPTION:
				return new PathContainsFileException(serverError.getPathContainsFileException().getMessage(), false);
			case ILLEGAL_OFFSET_EXCEPTION:
				return new IllegalOffsetException(serverError.getIllegalOffsetException().getMessage(), false);
			case FILE_NOT_FOUND_EXCEPTION:
				return new FileNotFoundException(serverError.getFileNotFoundException().getMessage(), false);
			case FORBIDDEN_PATH_EXCEPTION:
				return new ForbiddenPathException(serverError.getForbiddenPathException().getMessage(), false);
			case MALFORMED_GLOB_EXCEPTION:
				return new MalformedGlobException(serverError.getMalformedGlobException().getMessage(), false);
			case IS_A_DIRECTORY_EXCEPTION:
				return new IsADirectoryException(serverError.getIsADirectoryException().getMessage(), false);
			case FS_IO_EXCEPTION:
				return new FsIOException(serverError.getFsIoException().getMessage(), false);
			default:
				return new FsException("Unknown exception", false);
		}
	}
}
