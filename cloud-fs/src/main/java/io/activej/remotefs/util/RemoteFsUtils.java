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

package io.activej.remotefs.util;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.codec.StructuredCodec;
import io.activej.codec.StructuredDecoder;
import io.activej.codec.json.JsonUtils;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.ref.RefLong;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.csp.dsl.ChannelConsumerTransformer;
import io.activej.promise.Promise;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static io.activej.codec.json.JsonUtils.fromJson;
import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_DATA_EXCEPTION;
import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_END_OF_STREAM_EXCEPTION;
import static io.activej.remotefs.FsClient.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

public final class RemoteFsUtils {
	private static final Pattern ANY_GLOB_METACHARS = Pattern.compile("[*?{}\\[\\]\\\\]");
	private static final Pattern UNESCAPED_GLOB_METACHARS = Pattern.compile("(?<!\\\\)(?:\\\\\\\\)*[*?{}\\[\\]]");

	public static final Map<Integer, Throwable> ID_TO_ERROR;
	public static final Map<Throwable, Integer> ERROR_TO_ID;

	static {
		Map<Integer, Throwable> tempMap = new HashMap<>();

		tempMap.put(1, FILE_NOT_FOUND);
		tempMap.put(2, FILE_EXISTS);
		tempMap.put(3, BAD_PATH);
		tempMap.put(6, BAD_RANGE);
		tempMap.put(7, IS_DIRECTORY);
		tempMap.put(8, MALFORMED_GLOB);
		tempMap.put(9, ILLEGAL_OFFSET);

		ID_TO_ERROR = unmodifiableMap(tempMap);
		ERROR_TO_ID = unmodifiableMap(tempMap.entrySet().stream().collect(toMap(Map.Entry::getValue, Map.Entry::getKey)));
	}

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

	public static <I, O> ByteBufsCodec<I, O> nullTerminatedJson(StructuredCodec<I> in, StructuredCodec<O> out) {
		return ByteBufsCodec
				.ofDelimiter(
						ByteBufsDecoder.ofNullTerminatedBytes(),
						buf -> {
							ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
							buf1.put((byte) 0);
							return buf1;
						})
				.andThen(
						buf -> JsonUtils.fromJson(in, buf.asString(UTF_8)),
						item -> JsonUtils.toJsonBuf(out, item));
	}

	public static <T> Function<ByteBuf, Promise<T>> parseBody(StructuredDecoder<T> decoder) {
		return body -> {
			try {
				return Promise.of(fromJson(decoder, body.getString(UTF_8)));
			} catch (ParseException e) {
				return Promise.ofException(e);
			}
		};
	}

	public static ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> ofFixedSize(long size) {
		return consumer -> {
			RefLong total = new RefLong(size);
			return consumer
					.<ByteBuf>mapAsync(byteBuf -> {
						long left = total.dec(byteBuf.readRemaining());
						if (left < 0) {
							byteBuf.recycle();
							return Promise.ofException(UNEXPECTED_DATA_EXCEPTION);
						}
						return Promise.of(byteBuf);
					})
					.withAcknowledgement(ack -> ack
							.then(() -> {
								if (total.get() > 0) {
									return Promise.ofException(UNEXPECTED_END_OF_STREAM_EXCEPTION);
								}
								return Promise.complete();
							}));
		};
	}

}
