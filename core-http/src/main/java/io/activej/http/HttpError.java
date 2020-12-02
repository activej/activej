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

package io.activej.http;

import io.activej.promise.Promisable;
import io.activej.promise.Promise;

/**
 * This is a special exception, that is formatted as HTTP response with code and text from it by default.
 * It is a stackless exception.
 * Please be aware that when a cause is given, its stacktrace is printed too
 */
public class HttpError extends HttpException implements Promisable<HttpResponse> {
	private final int code;

	protected HttpError(int code) {
		this.code = code;
	}

	protected HttpError(int code, String message) {
		super(message);
		this.code = code;
	}

	protected HttpError(int code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}

	protected HttpError(int code, Throwable cause) {
		super(cause);
		this.code = code;
	}

	public static HttpError ofCode(int code) {
		return new HttpError(code);
	}

	public static HttpError ofCode(int code, String message) {
		return new HttpError(code, message);
	}

	public static HttpError ofCode(int code, String message, Throwable cause) {
		return new HttpError(code, message, cause);
	}

	public static HttpError ofCode(int code, Throwable cause) {
		return new HttpError(code, cause);
	}

	public static HttpError badRequest400(String message) {
		return new HttpError(400, message);
	}

	public static HttpError notFound404() {
		return new HttpError(404, "Not found");
	}

	public static HttpError internalServerError500() {
		return new HttpError(500, "Internal server error");
	}

	public static HttpError notAllowed405() {
		return new HttpError(405, "Not allowed");
	}

	public final int getCode() {
		return code;
	}

	@Override
	public Promise<HttpResponse> promise() {
		return Promise.ofException(this);
	}

	@Override
	public final Throwable fillInStackTrace() {
		return this;
	}

	@Override
	public String toString() {
		String msg = getLocalizedMessage();
		if (msg != null) {
			return "HTTP code " + code + ": " + msg;
		}
		return "HTTP code " + code;
	}
}
