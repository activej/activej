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

import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

import static io.activej.http.HttpHeaders.*;

/**
 * This is an interface for the formatter function for HTTP.
 * It converts unhandled checked exceptions that could be returned
 * from servers root servlet and transforms them into HTTP error responses.
 */
@FunctionalInterface
public interface HttpExceptionFormatter {
	String ACTIVEJ_VERSION = "3.0-SNAPSHOT";

	@Language("HTML")
	String HTTP_ERROR_HTML = "<!doctype html>" +
			"<html lang=\"en\">" +
			"<head>" +
			"<meta charset=\"UTF-8\">" +
			"<title>{title}</title>" +
			"<style>h1, p { font-family: sans-serif; }</style>" +
			"</head>" +
			"<body>" +
			"<h1 style=\"text-align: center;\">{title}</h1>" +
			"<hr>" +
			"<p style=\"text-align: center\">{message}</p>" +
			"<hr>" +
			"<p style=\"text-align: center;\">ActiveJ " + ACTIVEJ_VERSION + "</p>" +
			"</body>" +
			"</html>";

	String INTERNAL_SERVER_ERROR_HTML = HTTP_ERROR_HTML
			.replace("{title}", "Internal Server Error")
			.replace("<p style=\"text-align: center\">{message}</p><hr>", "");

	@NotNull
	HttpResponse formatException(@NotNull Throwable e);

	/**
	 * Standard formatter maps all exceptions except HttpException to an empty response with 500 status code.
	 * HttpExceptions are mapped to a response with their status code, message and stacktrace of the cause if it was specified.
	 */
	HttpExceptionFormatter DEFAULT_FORMATTER = e -> {
		HttpResponse response;
		if (e instanceof HttpException) {
			int code = ((HttpException) e).getCode();
			response = HttpResponse.ofCode(code).withHtml(HTTP_ERROR_HTML.replace("{title}", HttpUtils.getHttpErrorTitle(code)).replace("{message}", e.toString()));
		} else {
			response = HttpResponse.ofCode(500).withHtml(INTERNAL_SERVER_ERROR_HTML);
		}
		// default formatter leaks no information about unknown exceptions
		return response
				.withHeader(CACHE_CONTROL, "no-store")
				.withHeader(PRAGMA, "no-cache")
				.withHeader(AGE, "0");
	};

	/**
	 * This formatter prints the stacktrace of the exception into the HTTP response.
	 */
	HttpExceptionFormatter DEBUG_FORMATTER = e ->
			DebugStacktraceRenderer.render(e, e instanceof HttpException ? ((HttpException) e).getCode() : 500)
					.withHeader(CACHE_CONTROL, "no-store")
					.withHeader(PRAGMA, "no-cache")
					.withHeader(AGE, "0");

	/**
	 * This formatter if either one of {@link #DEFAULT_FORMATTER} or {@link #DEBUG_FORMATTER},
	 * depending on whether you start the application from the IntelliJ IDE or not.
	 */
	HttpExceptionFormatter COMMON_FORMATTER =
			System.getProperty("java.class.path", "").contains("idea_rt.jar") ?
					DEBUG_FORMATTER :
					DEFAULT_FORMATTER;
}
