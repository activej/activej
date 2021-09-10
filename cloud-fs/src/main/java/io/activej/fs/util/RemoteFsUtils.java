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

package io.activej.fs.util;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.Settings;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.collection.Try;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.TruncatedDataException;
import io.activej.common.exception.UnexpectedDataException;
import io.activej.common.ref.RefLong;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.fs.exception.FsBatchException;
import io.activej.fs.exception.FsException;
import io.activej.fs.exception.FsIOException;
import io.activej.fs.exception.FsScalarException;
import io.activej.promise.Promise;
import io.activej.types.TypeT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.activej.common.Utils.mapOf;

public final class RemoteFsUtils {
	private static final Pattern ANY_GLOB_METACHARS = Pattern.compile("[*?{}\\[\\]\\\\]");
	private static final Pattern UNESCAPED_GLOB_METACHARS = Pattern.compile("(?<!\\\\)(?:\\\\\\\\)*[*?{}\\[\\]]");

	/**
	 * Escapes any glob metacharacters so that given path string can ever only match one file.
	 *
	 * @param path path that potentially can contain glob metachars
	 * @return escaped glob which matches only a file with that name
	 */
	public static String escapeGlob(String path) {
		return ANY_GLOB_METACHARS.matcher(path).replaceAll("\\\\$0");
	}

	/**
	 * Checks if given glob can match more than one file.
	 *
	 * @param glob the glob to check.
	 * @return <code>true</code> if given glob can match more than one file.
	 */
	public static boolean isWildcard(String glob) {
		return UNESCAPED_GLOB_METACHARS.matcher(glob).find();
	}

	/**
	 * Returns a {@link PathMatcher} for given glob
	 *
	 * @param glob a glob string
	 * @return a path matcher for the glob string
	 */
	public static PathMatcher getGlobPathMatcher(String glob) {
		return FileSystems.getDefault().getPathMatcher("glob:" + glob);
	}

	/**
	 * Same as {@link #getGlobPathMatcher(String)} but returns a string predicate.
	 *
	 * @param glob a glob string
	 * @return a predicate for the glob string
	 */
	public static Predicate<String> getGlobStringPredicate(String glob) {
		PathMatcher matcher = getGlobPathMatcher(glob);
		return str -> matcher.matches(Paths.get(str));
	}

	public static ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> ofFixedSize(long size) {
		return consumer -> {
			RefLong total = new RefLong(size);
			return consumer
					.<ByteBuf>mapAsync(byteBuf -> {
						long left = total.dec(byteBuf.readRemaining());
						if (left < 0) {
							byteBuf.recycle();
							return Promise.ofException(new UnexpectedDataException());
						}
						return Promise.of(byteBuf);
					})
					.withAcknowledgement(ack -> ack
							.whenResult(() -> {
								if (total.get() > 0) {
									throw new TruncatedDataException();
								}
							}));
		};
	}

	/*
		Casting received errors before sending to remote peer
		ActiveFs.class is used as a component to hide implementation details from peer
	 */
	public static FsException castError(Exception e) {
		return e instanceof FsException ? (FsException) e : new FsIOException("Unknown error");
	}

	public static FsBatchException fsBatchException(String name, FsScalarException exception) {
		return new FsBatchException(mapOf(name, exception));
	}

	public static void reduceErrors(List<Try<Void>> tries, Iterator<String> sources) throws Exception {
		Map<String, FsScalarException> scalarExceptions = new HashMap<>();
		for (Try<Void> aTry : tries) {
			String source = sources.next();
			if (aTry.isSuccess()) continue;
			Exception exception = aTry.getException();
			if (exception instanceof FsScalarException) {
				scalarExceptions.put(source, ((FsScalarException) exception));
			} else {
				//noinspection ConstantConditions - a Try is not successfull, hence exception is not 'null'
				throw exception;
			}
		}
		if (!scalarExceptions.isEmpty()) {
			throw new FsBatchException(scalarExceptions);
		}
	}

	// region JSON
	private static final DslJson<?> DSL_JSON = new DslJson<>(Settings.withRuntime().includeServiceLoader());
	private static final ThreadLocal<JsonWriter> WRITERS = ThreadLocal.withInitial(DSL_JSON::newWriter);
	private static final ThreadLocal<JsonReader<?>> READERS = ThreadLocal.withInitial(DSL_JSON::newReader);

	public static <T> ByteBuf toJson(@Nullable T object) {
		if (object == null) return ByteBuf.wrap(new byte[]{'n', 'u', 'l', 'l'}, 0, 4);
		JsonWriter writer = toJson(object.getClass(), object);
		return ByteBuf.wrapForReading(writer.toByteArray());
	}

	public static <T> ByteBuf toJson(@NotNull Class<? super T> manifest, @Nullable T object) {
		if (object == null) return ByteBuf.wrap(new byte[]{'n', 'u', 'l', 'l'}, 0, 4);
		JsonWriter writer = toJson((Type) manifest, object);
		return ByteBuf.wrapForReading(writer.toByteArray());
	}

	private static <T> JsonWriter toJson(@NotNull Type manifest, @Nullable T object) {
		JsonWriter jsonWriter = WRITERS.get();
		jsonWriter.reset();
		if (!DSL_JSON.serialize(jsonWriter, manifest, object)) {
			throw new IllegalArgumentException("Cannot serialize " + manifest);
		}
		return jsonWriter;
	}

	public static <T> T fromJson(@NotNull Class<T> type, @NotNull ByteBuf buf) throws MalformedDataException {
		return fromJson((Type) type, buf);
	}

	public static <T> T fromJson(@NotNull TypeT<T> type, @NotNull ByteBuf buf) throws MalformedDataException {
		return fromJson(type.getType(), buf);
	}

	public static <T> T fromJson(@NotNull Type manifest, @NotNull ByteBuf buf) throws MalformedDataException {
		byte[] bytes = buf.getArray();
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
}
