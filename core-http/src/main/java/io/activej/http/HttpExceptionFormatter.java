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

import io.activej.promise.Promise;
import org.intellij.lang.annotations.Language;

import static io.activej.http.HttpHeaders.CACHE_CONTROL;

/**
 * This is an interface for the formatter function for HTTP.
 * It converts unhandled checked exceptions that could be returned
 * from servers root servlet and transforms them into HTTP error responses.
 */
@FunctionalInterface
public interface HttpExceptionFormatter {
	String ACTIVEJ_VERSION = "6.0-beta2";

	@Language("HTML")
	String HTTP_ERROR_HTML = """
		<!doctype html>
		<html lang="en">
		<head>
		<meta charset="UTF-8">
		<title>{title}</title>
		<style>h1, p { font-family: sans-serif; }</style>
		</head>
		<body>
		<h1 style="text-align: center;">{title}</h1>
		<hr>
		<p style="text-align: center">{message}</p>
		<hr>
		<p style="text-align: center;">{activej_version}</p>
		</body>
		</html>
		"""
		.replace("{activej_version}", ACTIVEJ_VERSION);

	String INTERNAL_SERVER_ERROR_HTML = HTTP_ERROR_HTML
		.replace("{title}", "Internal Server Error")
		.replace("""
			<p style="text-align: center">{message}</p>
			<hr>""", "");

	Promise<HttpResponse> formatException(Exception e);

	/**
	 * Standard formatter maps all exceptions except HttpException to an empty response with 500 status code.
	 * HttpExceptions are mapped to a response with their status code, message and stacktrace of the cause if it was specified.
	 */
	HttpExceptionFormatter DEFAULT_FORMATTER = e -> {
		HttpResponse.Builder responseBuilder;
		if (e instanceof HttpError httpError) {
			int code = httpError.getCode();
			responseBuilder = HttpResponse.ofCode(code)
				.withHtml(HTTP_ERROR_HTML
					.replace("{title}", HttpUtils.getHttpErrorTitle(code))
					.replace("{message}", e.toString()));
		} else if (e instanceof MalformedHttpException) {
			responseBuilder = HttpResponse.ofCode(400)
				.withHtml(HTTP_ERROR_HTML
					.replace("{title}", "400. Bad Request")
					.replace("{message}", e.toString()));

		} else {
			responseBuilder = HttpResponse.ofCode(500)
				.withHtml(INTERNAL_SERVER_ERROR_HTML);
		}
		// default formatter leaks no information about unknown exceptions
		return responseBuilder
			.withHeader(CACHE_CONTROL, "no-store")
			.toPromise();
	};

	/**
	 * This formatter prints the stacktrace of the exception into the HTTP response.
	 */
	HttpExceptionFormatter DEBUG_FORMATTER = e -> {
		int code = e instanceof HttpError httpError ?
			httpError.getCode() :
			e instanceof MalformedHttpException ?
				400 :
				500;
		return DebugStacktraceRenderer.render(e, code)
			.withHeader(CACHE_CONTROL, "no-store")
			.toPromise();
	};

	/**
	 * This formatter if either one of {@link #DEFAULT_FORMATTER} or {@link #DEBUG_FORMATTER},
	 * depending on whether you start the application from the IntelliJ IDE or not.
	 */
	HttpExceptionFormatter COMMON_FORMATTER =
		System.getProperty("java.class.path", "").contains("idea_rt.jar") ?
			DEBUG_FORMATTER :
			DEFAULT_FORMATTER;
}
