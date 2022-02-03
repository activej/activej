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

package io.activej.http.decoder;

import io.activej.common.collection.Either;
import io.activej.http.HttpRequest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;

/**
 * This class contains some common primitive {@link Decoder HttpDecoders}, that
 * can be combined to form complex ones.
 */
@SuppressWarnings("RedundantCast")
public final class Decoders {
	public static final String REQUIRED_GET_PARAM = "Required GET param: %s";
	public static final String REQUIRED_POST_PARAM = "Required POST param: %s";
	public static final String REQUIRED_PATH_PARAM = "Required path param: %s";
	public static final String REQUIRED_COOKIE = "Required cookie: %s";

	private static <T> Decoder<T> create(String paramName,
			@NotNull Mapper<String, T> fn,
			@NotNull BiFunction<HttpRequest, String, String> paramSupplier,
			String message) {
		return new AbstractDecoder<>(paramName) {
			@Override
			public Either<T, DecodeErrors> decode(@NotNull HttpRequest request) {
				String str = paramSupplier.apply(request, paramName);
				return str != null ?
						fn.map(str)
								.mapRight(DecodeErrors::of) :
						Either.right(DecodeErrors.of(message, paramName));
			}
		};
	}

	private static <T> Decoder<T> create(String paramName,
			@NotNull Mapper<String, T> fn,
			@NotNull BiFunction<HttpRequest, String, String> paramSupplier,
			@Nullable T defaultValue) {
		return new AbstractDecoder<>(paramName) {
			@Override
			public Either<T, DecodeErrors> decode(@NotNull HttpRequest request) {
				String str = paramSupplier.apply(request, paramName);
				return str != null ?
						fn.map(str)
								.mapRight(DecodeErrors::of) :
						Either.left(defaultValue);
			}
		};
	}

	public static Decoder<String> ofGet(String paramName) {
		return ofGet(paramName, (Mapper<String, String>) Either::left);
	}

	public static Decoder<String> ofGet(String paramName, String defaultValue) {
		return ofGet(paramName, (Mapper<String, String>) Either::left, defaultValue);
	}

	public static <T> Decoder<T> ofGet(@NotNull String paramName, @NotNull Mapper<String, T> fn) {
		return create(paramName, fn, HttpRequest::getQueryParameter, REQUIRED_GET_PARAM);
	}

	public static <T> Decoder<T> ofGet(@NotNull String paramName, @NotNull Mapper<String, T> fn, @Nullable T defaultValue) {
		return create(paramName, fn, HttpRequest::getQueryParameter, defaultValue);
	}

	public static Decoder<String> ofPost(String paramName) {
		return ofPost(paramName, (Mapper<String, String>) Either::left);
	}

	public static Decoder<String> ofPost(String paramName, String defaultValue) {
		return ofPost(paramName, (Mapper<String, String>) Either::left, defaultValue);
	}

	public static <T> Decoder<T> ofPost(@NotNull String paramName, @NotNull Mapper<String, T> fn) {
		return create(paramName, fn, HttpRequest::getPostParameter, REQUIRED_POST_PARAM);
	}

	public static <T> Decoder<T> ofPost(@NotNull String paramName, @NotNull Mapper<String, T> fn, @Nullable T defaultValue) {
		return create(paramName, fn, HttpRequest::getPostParameter, defaultValue);
	}

	public static Decoder<String> ofPath(String paramName) {
		return ofPath(paramName, (Mapper<String, String>) Either::left);
	}

	public static Decoder<String> ofPath(String paramName, String defaultValue) {
		return ofPath(paramName, (Mapper<String, String>) Either::left, defaultValue);
	}

	public static <T> Decoder<T> ofPath(@NotNull String paramName, @NotNull Mapper<String, T> fn) {
		return create(paramName, fn, HttpRequest::getPathParameter, REQUIRED_PATH_PARAM);
	}

	public static <T> Decoder<T> ofPath(@NotNull String paramName, @NotNull Mapper<String, T> fn, @Nullable T defaultValue) {
		return create(paramName, fn, HttpRequest::getPathParameter, defaultValue);
	}

	public static Decoder<String> ofCookie(String paramName) {
		return ofCookie(paramName, (Mapper<String, String>) Either::left);
	}

	public static Decoder<String> ofCookie(String paramName, String defaultValue) {
		return ofCookie(paramName, (Mapper<String, String>) Either::left, defaultValue);
	}

	public static <T> Decoder<T> ofCookie(@NotNull String paramName, @NotNull Mapper<String, T> fn) {
		return create(paramName, fn, HttpRequest::getCookie, REQUIRED_COOKIE);
	}

	public static <T> Decoder<T> ofCookie(@NotNull String paramName, @NotNull Mapper<String, T> fn, @Nullable T defaultValue) {
		return create(paramName, fn, HttpRequest::getCookie, defaultValue);
	}
}
