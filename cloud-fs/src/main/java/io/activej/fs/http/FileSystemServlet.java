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

package io.activej.fs.http;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.function.FunctionEx;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.IFileSystem;
import io.activej.fs.exception.FileNotFoundException;
import io.activej.fs.exception.FileSystemException;
import io.activej.http.*;
import io.activej.http.MultipartByteBufsDecoder.AsyncMultipartDataHandler;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;

import java.util.Objects;

import static io.activej.common.function.FunctionEx.identity;
import static io.activej.fs.http.FileSystemCommand.*;
import static io.activej.fs.util.JsonUtils.fromJson;
import static io.activej.fs.util.JsonUtils.toJson;
import static io.activej.fs.util.MessageTypes.STRING_SET_TYPE;
import static io.activej.fs.util.MessageTypes.STRING_STRING_MAP_TYPE;
import static io.activej.fs.util.RemoteFileSystemUtils.castError;
import static io.activej.http.ContentTypes.JSON_UTF_8;
import static io.activej.http.ContentTypes.PLAIN_TEXT_UTF_8;
import static io.activej.http.HttpHeaderValue.ofContentType;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpMethod.GET;
import static io.activej.http.HttpMethod.POST;

/**
 * An HTTP servlet that exposes some given {@link IFileSystem}.
 * <p>
 * Servlet is fully compatible with {@link FileSystem_HttpClient} client.
 * <p>
 * It also defines additional endpoints that can be useful for accessing via web browser,
 * such as uploading multiple files using <i>multipart/form-data</i> content type
 * and downloading a file using range requests.
 * <p>
 * This server may  be launched as a publicly available server.
 */
public final class FileSystemServlet {
	private FileSystemServlet() {
	}

	public static RoutingServlet create(Reactor reactor, IFileSystem fs) {
		return create(reactor, fs, true);
	}

	public static RoutingServlet create(Reactor reactor, IFileSystem fs, boolean inline) {
		return RoutingServlet.create(reactor)
				.map(POST, "/" + UPLOAD + "/*", request -> {
					String contentLength = request.getHeader(CONTENT_LENGTH);
					Long size = contentLength == null ? null : Long.valueOf(contentLength);
					return (size == null ?
							fs.upload(decodePath(request)) :
							fs.upload(decodePath(request), size))
							.map(uploadAcknowledgeFn(request), errorResponseFn());
				})
				.map(POST, "/" + UPLOAD, request -> request.handleMultipart(AsyncMultipartDataHandler.file(fs::upload))
						.map(voidResponseFn(), errorResponseFn()))
				.map(POST, "/" + APPEND + "/*", request -> {
					long offset = getNumberParameterOr(request, "offset", 0);
					return fs.append(decodePath(request), offset)
							.map(uploadAcknowledgeFn(request), errorResponseFn());
				})
				.map(GET, "/" + DOWNLOAD + "/*", request -> {
					String name = decodePath(request);
					String rangeHeader = request.getHeader(HttpHeaders.RANGE);
					if (rangeHeader != null) {
						return rangeDownload(fs, inline, name, rangeHeader);
					}
					long offset = getNumberParameterOr(request, "offset", 0);
					long limit = getNumberParameterOr(request, "limit", Long.MAX_VALUE);
					return fs.download(name, offset, limit)
							.map(res -> HttpResponse.ok200()
											.withHeader(ACCEPT_RANGES, "bytes")
											.withBodyStream(res),
									errorResponseFn());
				})
				.map(GET, "/" + LIST, request -> {
					String glob = request.getQueryParameter("glob");
					glob = glob != null ? glob : "**";
					return fs.list(glob)
							.map(list -> HttpResponse.ok200()
											.withBody(toJson(list))
											.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8)),
									errorResponseFn());
				})
				.map(GET, "/" + INFO + "/*", request ->
						fs.info(decodePath(request))
								.map(meta -> HttpResponse.ok200()
												.withBody(toJson(meta))
												.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8)),
										errorResponseFn()))
				.map(GET, "/" + INFO_ALL, request -> request.loadBody()
						.map(body -> fromJson(STRING_SET_TYPE, body))
						.then(fs::infoAll)
						.map(map -> HttpResponse.ok200()
										.withBody(toJson(map))
										.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8)),
								errorResponseFn()))
				.map(GET, "/" + PING, request -> fs.ping()
						.map(voidResponseFn(), errorResponseFn()))
				.map(POST, "/" + MOVE, request -> {
					String name = getQueryParameter(request, "name");
					String target = getQueryParameter(request, "target");
					return fs.move(name, target)
							.map(voidResponseFn(), errorResponseFn());
				})
				.map(POST, "/" + MOVE_ALL, request -> request.loadBody()
						.map(body -> fromJson(STRING_STRING_MAP_TYPE, body))
						.then(fs::moveAll)
						.map(voidResponseFn(), errorResponseFn()))
				.map(POST, "/" + COPY, request -> {
					String name = getQueryParameter(request, "name");
					String target = getQueryParameter(request, "target");
					return fs.copy(name, target)
							.map(voidResponseFn(), errorResponseFn());
				})
				.map(POST, "/" + COPY_ALL, request -> request.loadBody()
						.map(body -> fromJson(STRING_STRING_MAP_TYPE, body))
						.then(fs::copyAll)
						.map(voidResponseFn(), errorResponseFn()))
				.map(HttpMethod.DELETE, "/" + DELETE + "/*", request ->
						fs.delete(decodePath(request))
								.map(voidResponseFn(), errorResponseFn()))
				.map(POST, "/" + DELETE_ALL, request -> request.loadBody()
						.map(body -> fromJson(STRING_SET_TYPE, body))
						.then(fs::deleteAll)
						.map(voidResponseFn(), errorResponseFn()));
	}

	private static Promise<HttpResponse> rangeDownload(IFileSystem fs, boolean inline, String name, String rangeHeader) {
		//noinspection ConstantConditions
		return fs.info(name)
				.whenResult(Objects::isNull, $ -> {
					throw new FileNotFoundException();
				})
				.then(meta -> HttpResponse.file(
						(offset, limit) -> fs.download(name, offset, limit),
						name,
						meta.getSize(),
						rangeHeader,
						inline))
				.map(identity(), errorResponseFn());
	}

	private static String decodePath(HttpRequest request) throws HttpError {
		String value = UrlParser.urlParse(request.getRelativePath());
		if (value == null) {
			throw HttpError.ofCode(400, "Path contains invalid UTF");
		}
		return value;
	}

	private static String getQueryParameter(HttpRequest request, String parameterName) throws HttpError {
		String value = request.getQueryParameter(parameterName);
		if (value == null) {
			throw HttpError.ofCode(400, "No '" + parameterName + "' query parameter");
		}
		return value;
	}

	private static long getNumberParameterOr(HttpRequest request, String parameterName, long defaultValue) throws HttpError {
		String value = request.getQueryParameter(parameterName);
		if (value == null) {
			return defaultValue;
		}
		try {
			long val = Long.parseLong(value);
			if (val < 0) {
				throw new NumberFormatException();
			}
			return val;
		} catch (NumberFormatException ignored) {
			throw HttpError.ofCode(400, "Invalid '" + parameterName + "' value");
		}
	}

	private static FunctionEx<Exception, HttpResponse> errorResponseFn() {
		return e -> HttpResponse.ofCode(500)
				.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
				.withBody(toJson(FileSystemException.class, castError(e)));
	}

	private static <T> FunctionEx<T, HttpResponse> voidResponseFn() {
		return $ -> HttpResponse.ok200().withHeader(CONTENT_TYPE, ofContentType(PLAIN_TEXT_UTF_8));
	}

	private static FunctionEx<ChannelConsumer<ByteBuf>, HttpResponse> uploadAcknowledgeFn(HttpRequest request) {
		return consumer -> HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
				.withBodyStream(ChannelSupplier.ofPromise(request.takeBodyStream()
						.streamTo(consumer)
						.map($ -> UploadAcknowledgement.ok(), e -> UploadAcknowledgement.ofError(castError(e)))
						.map(ack -> ChannelSupplier.of(toJson(ack)))));
	}

}
