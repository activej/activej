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

import io.activej.async.function.AsyncSupplier;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.initializer.WithInitializer;
import io.activej.http.loader.ResourceIsADirectoryException;
import io.activej.http.loader.ResourceNotFoundException;
import io.activej.http.loader.StaticLoader;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.http.HttpHeaderValue.ofContentType;
import static io.activej.http.HttpHeaders.CONTENT_TYPE;

/**
 * This servlet allows return HTTP responses by HTTP paths from some predefined storage, mainly the filesystem.
 */
public final class StaticServlet implements AsyncServlet, WithInitializer<StaticServlet> {
	public static final Charset DEFAULT_TXT_ENCODING = StandardCharsets.UTF_8;

	private final StaticLoader resourceLoader;
	private Function<String, ContentType> contentTypeResolver = StaticServlet::getContentType;
	private Function<HttpRequest, @Nullable String> pathMapper = HttpRequest::getRelativePath;
	private Supplier<HttpResponse> responseSupplier = HttpResponse::ok200;
	private final Set<String> indexResources = new LinkedHashSet<>();

	private @Nullable String defaultResource;

	private StaticServlet(StaticLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	public static StaticServlet create(StaticLoader resourceLoader) {
		return new StaticServlet(resourceLoader);
	}

	public static StaticServlet create(StaticLoader resourceLoader, String page) {
		return create(resourceLoader).withMappingTo(page);
	}

	public static StaticServlet ofClassPath(Executor executor, String path) {
		return new StaticServlet(StaticLoader.ofClassPath(executor, path));
	}

	public static StaticServlet ofPath(Executor executor, Path path) {
		return new StaticServlet(StaticLoader.ofPath(executor, path));
	}

	@SuppressWarnings("UnusedReturnValue")
	public StaticServlet withContentType(ContentType contentType) {
		return withContentTypeResolver($ -> contentType);
	}

	public StaticServlet withContentTypeResolver(Function<String, ContentType> contentTypeResolver) {
		this.contentTypeResolver = contentTypeResolver;
		return this;
	}

	public StaticServlet withMapping(Function<HttpRequest, String> fn) {
		pathMapper = fn;
		return this;
	}

	public StaticServlet withMappingTo(String path) {
		//noinspection RedundantCast - it does not compile without the cast
		if (this.contentTypeResolver == (Function<String, ContentType>) StaticServlet::getContentType) {
			withContentType(getContentType(path));
		}
		return withMapping($ -> path);
	}

	public StaticServlet withMappingNotFoundTo(String defaultResource) {
		this.defaultResource = defaultResource;
		return this;
	}

	public StaticServlet withIndexResources(String... indexResources) {
		this.indexResources.addAll(List.of(indexResources));
		return this;
	}

	public StaticServlet withIndexHtml() {
		this.indexResources.add("index.html");
		return this;
	}

	public StaticServlet withResponse(Supplier<HttpResponse> responseSupplier) {
		this.responseSupplier = responseSupplier;
		return this;
	}

	public static ContentType getContentType(String path) {
		int pos = path.lastIndexOf('.');
		if (pos == -1) {
			return ContentType.of(MediaTypes.OCTET_STREAM);
		}

		String ext = path.substring(pos + 1);

		MediaType mime = MediaTypes.getByExtension(ext);
		if (mime == null) {
			mime = MediaTypes.OCTET_STREAM;
		}

		ContentType type;
		if (mime.isTextType()) {
			type = ContentType.of(mime, DEFAULT_TXT_ENCODING);
		} else {
			type = ContentType.of(mime);
		}

		return type;
	}

	private HttpResponse createHttpResponse(ByteBuf buf, ContentType contentType) {
		return responseSupplier.get()
				.withBody(buf)
				.withHeader(CONTENT_TYPE, ofContentType(contentType));
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) {
		String mappedPath = pathMapper.apply(request);
		if (mappedPath == null) return Promise.ofException(HttpError.notFound404());
		ContentType contentType = contentTypeResolver.apply(mappedPath);
		return Promise.complete()
				.then(() -> (mappedPath.endsWith("/") || mappedPath.isEmpty()) ?
						tryLoadIndexResource(mappedPath) :
						resourceLoader.load(mappedPath)
								.map(byteBuf -> createHttpResponse(byteBuf, contentType))
								.then((value, e) -> {
									if (e instanceof ResourceIsADirectoryException) {
										return tryLoadIndexResource(mappedPath);
									} else {
										return Promise.of(value, e);
									}
								}))
				.then(Promise::of,
						e -> e instanceof ResourceNotFoundException ?
								tryLoadDefaultResource() :
								Promise.ofException(HttpError.ofCode(400, e)));
	}

	private Promise<HttpResponse> tryLoadIndexResource(String mappedPath) {
		String dirPath = mappedPath.endsWith("/") || mappedPath.isEmpty() ? mappedPath : (mappedPath + '/');
		return Promises.first(
						indexResources.stream()
								.map(indexResource -> (AsyncSupplier<HttpResponse>) () ->
										resourceLoader.load(dirPath + indexResource)
												.map(byteBuf -> createHttpResponse(byteBuf, contentTypeResolver.apply(indexResource)))))
				.mapException(e -> new ResourceNotFoundException("Could not find '" + mappedPath + '\'', e));
	}

	private Promise<? extends HttpResponse> tryLoadDefaultResource() {
		return defaultResource != null ?
				resourceLoader.load(defaultResource)
						.map(buf -> createHttpResponse(buf, contentTypeResolver.apply(defaultResource))) :
				Promise.ofException(HttpError.notFound404());
	}
}
