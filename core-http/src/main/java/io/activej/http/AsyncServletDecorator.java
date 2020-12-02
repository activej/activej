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

import io.activej.common.MemSize;
import io.activej.common.exception.UncheckedException;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.*;

import static io.activej.http.AsyncServlet.firstSuccessful;
import static java.util.Arrays.asList;

/**
 * A higher order function that allows transformations of {@link AsyncServlet} functions.
 */
public interface AsyncServletDecorator {
	@NotNull AsyncServlet serve(@NotNull AsyncServlet servlet);

	default @NotNull AsyncServlet serveFirstSuccessful(@NotNull AsyncServlet... servlets) {
		return serve(firstSuccessful(servlets));
	}

	static AsyncServletDecorator create() {
		return servlet -> servlet;
	}

	default AsyncServletDecorator then(AsyncServletDecorator next) {
		return servlet -> this.serve(next.serve(servlet));
	}

	@NotNull
	static AsyncServletDecorator combineDecorators(AsyncServletDecorator... decorators) {
		return combineDecorators(asList(decorators));
	}

	@NotNull
	static AsyncServletDecorator combineDecorators(List<AsyncServletDecorator> decorators) {
		return decorators.stream()
				.reduce(create(), AsyncServletDecorator::then);
	}

	static AsyncServletDecorator onRequest(Consumer<HttpRequest> consumer) {
		return servlet ->
				request -> {
					consumer.accept(request);
					return servlet.serve(request);
				};
	}

	static AsyncServletDecorator onResponse(Consumer<HttpResponse> consumer) {
		return servlet ->
				request -> servlet.serveAsync(request).whenResult(consumer);
	}

	static AsyncServletDecorator onResponse(BiConsumer<HttpRequest, HttpResponse> consumer) {
		return servlet ->
				request -> servlet.serveAsync(request)
						.whenResult(response -> consumer.accept(request, response));
	}

	static AsyncServletDecorator mapResponse(Function<HttpResponse, HttpResponse> fn) {
		return servlet ->
				request -> servlet.serveAsync(request)
						.map(response -> {
							HttpResponse newResponse = fn.apply(response);
							if (response != newResponse) {
								response.recycle();
							}
							return newResponse;
						});
	}

	static AsyncServletDecorator mapResponse(BiFunction<HttpRequest, HttpResponse, HttpResponse> fn) {
		return servlet ->
				request -> servlet.serveAsync(request)
						.map(response -> {
							HttpResponse newResponse = fn.apply(request, response);
							if (response != newResponse) {
								response.recycle();
							}
							return newResponse;
						});
	}

	static AsyncServletDecorator onException(BiConsumer<HttpRequest, Throwable> consumer) {
		return servlet ->
				request -> servlet.serveAsync(request).whenException((e -> consumer.accept(request, e)));
	}

	static AsyncServletDecorator mapException(Function<Throwable, HttpResponse> fn) {
		return servlet ->
				request -> servlet.serveAsync(request)
						.mapEx(((response, e) -> {
							if (e == null) {
								return response;
							} else {
								return fn.apply(e);
							}
						}));
	}

	static AsyncServletDecorator mapException(BiFunction<HttpRequest, Throwable, HttpResponse> fn) {
		return servlet ->
				request -> servlet.serveAsync(request)
						.mapEx(((response, e) -> {
							if (e == null) {
								return response;
							} else {
								return fn.apply(request, e);
							}
						}));
	}

	static AsyncServletDecorator mapException(Predicate<Throwable> predicate, AsyncServlet fallbackServlet) {
		return servlet ->
				request -> servlet.serveAsync(request)
						.thenEx((response, e) -> predicate.test(e) ?
								fallbackServlet.serveAsync(request) :
								Promise.of(response, e));
	}

	static AsyncServletDecorator mapHttpException(AsyncServlet fallbackServlet) {
		return mapException(throwable -> throwable instanceof HttpError, fallbackServlet);
	}

	static AsyncServletDecorator mapHttpException404(AsyncServlet fallbackServlet) {
		return mapException(throwable -> throwable instanceof HttpError && ((HttpError) throwable).getCode() == 404, fallbackServlet);
	}

	static AsyncServletDecorator mapHttpException500(AsyncServlet fallbackServlet) {
		return mapException(throwable -> throwable instanceof HttpError && ((HttpError) throwable).getCode() == 500, fallbackServlet);
	}

	static AsyncServletDecorator mapHttpClientException(AsyncServlet fallbackServlet) {
		return mapException(throwable -> {
			if (throwable instanceof HttpError) {
				int code = ((HttpError) throwable).getCode();
				return code >= 400 && code < 500;
			}
			return false;
		}, fallbackServlet);
	}

	static AsyncServletDecorator mapHttpServerException(AsyncServlet fallbackServlet) {
		return mapException(throwable -> {
			if (throwable instanceof HttpError) {
				int code = ((HttpError) throwable).getCode();
				return code >= 500 && code < 600;
			}
			return false;
		}, fallbackServlet);
	}

	static AsyncServletDecorator mapToHttp500Exception() {
		return mapToHttpException(e -> HttpError.internalServerError500());
	}

	static AsyncServletDecorator mapToHttpException(Function<Throwable, HttpError> fn) {
		return servlet ->
				request -> servlet.serveAsync(request)
						.thenEx(((response, e) -> {
							if (e == null) {
								return Promise.of(response);
							} else {
								if (e instanceof HttpError) return Promise.ofException(e);
								return Promise.ofException(fn.apply(e));
							}
						}));
	}

	static AsyncServletDecorator mapToHttpException(BiFunction<HttpRequest, Throwable, HttpError> fn) {
		return servlet ->
				request -> servlet.serveAsync(request)
						.thenEx(((response, e) -> {
							if (e == null) {
								return Promise.of(response);
							} else {
								if (e instanceof HttpError) return Promise.ofException(e);
								return Promise.ofException(fn.apply(request, e));
							}
						}));
	}

	static AsyncServletDecorator catchUncheckedExceptions() {
		return servlet ->
				request -> {
					try {
						return servlet.serve(request);
					} catch (UncheckedException u) {
						return Promise.ofException(u.getCause());
					}
				};
	}

	static AsyncServletDecorator catchRuntimeExceptions() {
		return servlet ->
				request -> {
					try {
						return servlet.serve(request);
					} catch (UncheckedException u) {
						return Promise.ofException(u.getCause());
					} catch (RuntimeException e) {
						return Promise.ofException(e);
					}
				};
	}

	static AsyncServletDecorator setMaxBodySize(MemSize maxBodySize) {
		return setMaxBodySize(maxBodySize.toInt());
	}

	static AsyncServletDecorator setMaxBodySize(int maxBodySize) {
		return servlet ->
				request -> {
					request.setMaxBodySize(maxBodySize);
					return servlet.serve(request);
				};
	}

	static AsyncServletDecorator loadBody() {
		return servlet ->
				request -> request.loadBody()
						.then(() -> servlet.serveAsync(request));
	}

	static AsyncServletDecorator loadBody(MemSize maxBodySize) {
		return servlet ->
				request -> request.loadBody(maxBodySize)
						.then(() -> servlet.serveAsync(request));
	}

	static AsyncServletDecorator loadBody(int maxBodySize) {
		return servlet ->
				request -> request.loadBody(maxBodySize)
						.then(() -> servlet.serveAsync(request));
	}

	static AsyncServletDecorator logged() {
		return LoggableServlet::create;
	}

	static AsyncServletDecorator logged(BiFunction<HttpRequest, @Nullable HttpResponse, String> loggerFunction) {
		return servlet -> LoggableServlet.create(servlet, loggerFunction);
	}
}
