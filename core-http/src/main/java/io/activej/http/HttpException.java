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
public class HttpException extends Exception implements Promisable<HttpResponse> {
	private final int code;

	protected HttpException(int code) {
		this.code = code;
	}

	protected HttpException(int code, String message) {
		super(message);
		this.code = code;
	}

	protected HttpException(int code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}

	protected HttpException(int code, Throwable cause) {
		super(cause);
		this.code = code;
	}

	public static HttpException ofCode(int code) {
		return new HttpException(code);
	}

	public static HttpException ofCode(int code, String message) {
		return new HttpException(code, message);
	}

	public static HttpException ofCode(int code, String message, Throwable cause) {
		return new HttpException(code, message, cause);
	}

	public static HttpException ofCode(int code, Throwable cause) {
		return new HttpException(code, cause);
	}

	public static HttpException badRequest400(String message) {
		return new HttpException(400, message);
	}

	public static HttpException notFound404() {
		return new HttpException(404, "Not found");
	}

	public static HttpException internalServerError500() {
		return new HttpException(500, "Internal server error");
	}

	public static HttpException notAllowed405() {
		return new HttpException(405, "Not allowed");
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
