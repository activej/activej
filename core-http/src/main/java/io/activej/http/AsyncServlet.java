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

import java.util.concurrent.Executor;

/**
 * An interface for a servlet function that asynchronously receives a {@link HttpRequest},
 * processes it and then returns a {@link HttpResponse}.
 */
@FunctionalInterface
public interface AsyncServlet {
	Promisable<HttpResponse> serve(HttpRequest request) throws Exception;

	default Promise<HttpResponse> serveAsync(HttpRequest request) throws Exception {
		return serve(request).promise();
	}

	/**
	 * Wraps given {@link BlockingServlet} into async one using given {@link Executor}.
	 */
	static AsyncServlet ofBlocking(Executor executor, BlockingServlet blockingServlet) {
		return request -> request.loadBody()
				.then(() -> Promise.ofBlocking(executor,
						() -> blockingServlet.serve(request)));
	}
}
