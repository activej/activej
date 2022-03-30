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

package io.activej.crdt.util;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.Settings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.promise.Promise;
import io.activej.types.TypeT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static io.activej.crdt.wal.FileWriteAheadLog.EXT_FINAL;
import static java.util.stream.Collectors.toList;

public final class Utils {

	public static Promise<List<Path>> getWalFiles(Executor executor, Path walDir) {
		return Promise.ofBlocking(executor,
				() -> {
					try (Stream<Path> list = Files.list(walDir)) {
						return list
								.filter(file -> Files.isRegularFile(file) && file.toString().endsWith(EXT_FINAL))
								.collect(toList());
					}
				});
	}

	public static Promise<Void> deleteWalFiles(Executor executor, Collection<Path> walFiles) {
		return Promise.ofBlocking(executor, () -> {
			for (Path walFile : walFiles) {
				Files.deleteIfExists(walFile);
			}
		});
	}

	// region JSON
	private static final DslJson<?> DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(DSL_JSON::newReader);

	public static <T> ByteBuf toJson(@NotNull Type manifest, @Nullable T object) {
		if (object == null) return ByteBuf.wrap(new byte[]{'n', 'u', 'l', 'l'}, 0, 4);
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		if (!DSL_JSON.serialize(jsonWriter, manifest, object)) {
			throw new IllegalArgumentException("Cannot serialize " + manifest);
		}
		return ByteBuf.wrapForReading(jsonWriter.toByteArray());
	}

	public static <T> T fromJson(@NotNull Type manifest, @NotNull ByteBuf buf) throws MalformedDataException {
		return fromJson(manifest, buf.getArray());
	}

	public static <T> T fromJson(@NotNull TypeT<T> typeT, byte[] bytes) throws MalformedDataException {
		return fromJson(typeT.getType(), bytes);
	}

	public static <T> T fromJson(@NotNull Type manifest, byte[] bytes) throws MalformedDataException {
		try {
			//noinspection unchecked
			JsonReader.ReadObject<T> readObject = (JsonReader.ReadObject<T>) DSL_JSON.tryFindReader(manifest);
			if (readObject == null) {
				throw new IllegalArgumentException("Unknown type: " + manifest);
			}
			JsonReader<?> jsonReader = READERS.get().process(bytes, bytes.length);
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
	// endregion

	public static <I extends Message, O extends Message> ByteBufsCodec<I, O> codec(Parser<I> inputParser) {
		return new ByteBufsCodec<I, O>() {
			@Override
			public ByteBuf encode(O item) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try {
					item.writeDelimitedTo(baos);
				} catch (IOException e) {
					throw new AssertionError(e);
				}
				return ByteBuf.wrapForReading(baos.toByteArray());
			}

			@Override
			public @Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException {
				try {
					return inputParser.parseDelimitedFrom(asInputStream(bufs));
				} catch (InvalidProtocolBufferException e) {
					IOException ioException = e.unwrapIOException();
					if (ioException != e) {
						assert ioException == NEED_MORE_DATA_EXCEPTION;
						return null;
					}
					throw new MalformedDataException(e);
				}
			}
		};
	}

	private static InputStream asInputStream(ByteBufs bufs) {
		return new InputStream() {
			@Override
			public int read() throws IOException {
				if (bufs.isEmpty()) throw NEED_MORE_DATA_EXCEPTION;
				return bufs.getByte();
			}

			@Override
			public int read(byte @NotNull [] b, int off, int len) throws IOException {
				if (bufs.isEmpty()) throw NEED_MORE_DATA_EXCEPTION;
				return bufs.drainTo(b, off, len);
			}
		};
	}

	private static final NeedMoreDataException NEED_MORE_DATA_EXCEPTION = new NeedMoreDataException();

	private static final class NeedMoreDataException extends IOException {
		@Override
		public synchronized Throwable fillInStackTrace() {
			return this;
		}
	}
}
