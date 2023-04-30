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

import io.activej.async.function.AsyncFunction;
import io.activej.async.function.AsyncFunctionEx;
import io.activej.bytebuf.ByteBuf;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.fs.IFileSystem;
import io.activej.fs.exception.FileNotFoundException;
import io.activej.fs.exception.FileSystemException;
import io.activej.http.*;
import io.activej.http.MultipartByteBufsDecoder.AsyncMultipartDataHandler;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

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
import static io.activej.http.MediaTypes.OCTET_STREAM;

/**
 * An HTTP servlet that exposes some given {@link IFileSystem}.
 * <p>
 * Servlet is fully compatible with {@link HttpClientFileSystem} client.
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
							.then(uploadAcknowledgeFn(request), errorResponseFn());
				})
				.map(POST, "/" + UPLOAD, request -> request.handleMultipart(AsyncMultipartDataHandler.file(fs::upload))
						.then(voidResponseFn(), errorResponseFn()))
				.map(POST, "/" + APPEND + "/*", request -> {
					long offset = getNumberParameterOr(request, "offset", 0);
					return fs.append(decodePath(request), offset)
							.then(uploadAcknowledgeFn(request), errorResponseFn());
				})
				.map(GET, "/" + DOWNLOAD + "/*", request -> {
					String name = decodePath(request);
					String rangeHeader = request.getHeader(HttpHeaders.RANGE);
					if (rangeHeader != null) {
						//noinspection ConstantConditions
						return fs.info(name)
								.whenResult(meta -> {if (meta == null) throw new FileNotFoundException();})
								.then(meta -> rangeDownload(fs, name, meta.getSize(), inline, rangeHeader))
								.then(Promise::of, errorResponseFn());
					}
					long offset = getNumberParameterOr(request, "offset", 0);
					long limit = getNumberParameterOr(request, "limit", Long.MAX_VALUE);
					return fs.download(name, offset, limit)
							.then(res -> HttpResponse.ok200()
											.withHeader(ACCEPT_RANGES, "bytes")
											.withBodyStream(res)
											.toPromise(),
									errorResponseFn());
				})
				.map(GET, "/" + LIST, request -> {
					String glob = request.getQueryParameter("glob");
					glob = glob != null ? glob : "**";
					return fs.list(glob)
							.then(list -> HttpResponse.ok200()
											.withBody(toJson(list))
											.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
											.toPromise(),
									errorResponseFn());
				})
				.map(GET, "/" + INFO + "/*", request ->
						fs.info(decodePath(request))
								.then(meta -> HttpResponse.ok200()
												.withBody(toJson(meta))
												.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
												.toPromise(),
										errorResponseFn()))
				.map(GET, "/" + INFO_ALL, request -> request.loadBody()
						.map(body -> fromJson(STRING_SET_TYPE, body))
						.then(fs::infoAll)
						.then(map -> HttpResponse.ok200()
										.withBody(toJson(map))
										.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
										.toPromise(),
								errorResponseFn()))
				.map(GET, "/" + PING, request -> fs.ping()
						.then(voidResponseFn(), errorResponseFn()))
				.map(POST, "/" + MOVE, request -> {
					String name = getQueryParameter(request, "name");
					String target = getQueryParameter(request, "target");
					return fs.move(name, target)
							.then(voidResponseFn(), errorResponseFn());
				})
				.map(POST, "/" + MOVE_ALL, request -> request.loadBody()
						.map(body -> fromJson(STRING_STRING_MAP_TYPE, body))
						.then(fs::moveAll)
						.then(voidResponseFn(), errorResponseFn()))
				.map(POST, "/" + COPY, request -> {
					String name = getQueryParameter(request, "name");
					String target = getQueryParameter(request, "target");
					return fs.copy(name, target)
							.then(voidResponseFn(), errorResponseFn());
				})
				.map(POST, "/" + COPY_ALL, request -> request.loadBody()
						.map(body -> fromJson(STRING_STRING_MAP_TYPE, body))
						.then(fs::copyAll)
						.then(voidResponseFn(), errorResponseFn()))
				.map(HttpMethod.DELETE, "/" + DELETE + "/*", request ->
						fs.delete(decodePath(request))
								.then(voidResponseFn(), errorResponseFn()))
				.map(POST, "/" + DELETE_ALL, request -> request.loadBody()
						.map(body -> fromJson(STRING_SET_TYPE, body))
						.then(fs::deleteAll)
						.then(voidResponseFn(), errorResponseFn()));
	}

	private static Promise<HttpResponse> rangeDownload(IFileSystem fs, String name, long size, boolean inline, @Nullable String rangeHeader) {
		HttpResponse.Builder builder = HttpResponse.ofCode(rangeHeader == null ? 200 : 206);

		String localName = name.substring(name.lastIndexOf('/') + 1);
		MediaType mediaType = MediaTypes.getByExtension(localName.substring(localName.lastIndexOf('.') + 1));
		if (mediaType == null) {
			mediaType = OCTET_STREAM;
		}

		builder.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(ContentType.of(mediaType)));
		builder.withHeader(ACCEPT_RANGES, "bytes");
		builder.withHeader(CONTENT_DISPOSITION, inline ? "inline" : "attachment; filename=\"" + localName + "\"");

		long contentLength, offset;
		if (rangeHeader != null) {
			if (!rangeHeader.startsWith("bytes=")) {
				return Promise.ofException(HttpError.ofCode(416, "Invalid range header (not in bytes)"));
			}
			rangeHeader = rangeHeader.substring(6);
			if (!rangeHeader.matches("(?:\\d+)?-(?:\\d+)?")) {
				return Promise.ofException(HttpError.ofCode(416, "Only single part ranges are allowed"));
			}
			String[] parts = rangeHeader.split("-", 2);
			long endOffset;
			if (parts[0].isEmpty()) {
				if (parts[1].isEmpty()) {
					return Promise.ofException(HttpError.ofCode(416, "Invalid range"));
				}
				offset = size - Long.parseLong(parts[1]);
				endOffset = size;
			} else {
				if (parts[1].isEmpty()) {
					offset = Long.parseLong(parts[0]);
					endOffset = size - 1;
				} else {
					offset = Long.parseLong(parts[0]);
					endOffset = Long.parseLong(parts[1]);
				}
			}
			if (endOffset != -1 && offset > endOffset) {
				return Promise.ofException(HttpError.ofCode(416, "Invalid range"));
			}
			contentLength = endOffset - offset + 1;
			builder.withHeader(CONTENT_RANGE, "bytes " + offset + "-" + endOffset + "/" + size);
		} else {
			contentLength = size;
			offset = 0;
		}
		builder.withHeader(CONTENT_LENGTH, Long.toString(contentLength));
		builder.withBodyStream(ChannelSuppliers.ofPromise(fs.download(name, offset, contentLength)));
		return builder.toPromise();
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

	private static AsyncFunction<Exception, HttpResponse> errorResponseFn() {
		return e -> HttpResponse.ofCode(500)
				.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
				.withBody(toJson(FileSystemException.class, castError(e)))
				.toPromise();
	}

	private static <T> AsyncFunction<T, HttpResponse> voidResponseFn() {
		return $ -> HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(PLAIN_TEXT_UTF_8))
				.toPromise();
	}

	private static AsyncFunctionEx<ChannelConsumer<ByteBuf>, HttpResponse> uploadAcknowledgeFn(HttpRequest request) {
		return consumer -> HttpResponse.ok200()
				.withHeader(CONTENT_TYPE, ofContentType(JSON_UTF_8))
				.withBodyStream(ChannelSuppliers.ofPromise(request.takeBodyStream()
						.streamTo(consumer)
						.map($ -> UploadAcknowledgement.ok(), e -> UploadAcknowledgement.ofError(castError(e)))
						.map(ack -> ChannelSuppliers.ofValue(toJson(ack)))))
				.toPromise();
	}

}
