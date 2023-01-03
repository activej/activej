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

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;

/**
 * A stub client which forwards requests straight to the underlying servlet without any real I/O operations.
 * Used for testing.
 */
public final class StubHttpClient implements HttpClient {
	private final AsyncServlet servlet;

	private StubHttpClient(AsyncServlet servlet) {
		this.servlet = servlet;
	}

	public static StubHttpClient of(AsyncServlet servlet) {
		return new StubHttpClient(servlet);
	}

	@Override
	public Promise<HttpResponse> request(HttpRequest request) {
		Promise<HttpResponse> servletResult;
		try {
			servletResult = servlet.serveAsync(request);
		} catch (Exception e) {
			servletResult = Promise.ofException(e);
		}
		return servletResult
				.whenComplete(request::recycleBody)
				.then(res -> {
					ChannelSupplier<ByteBuf> bodyStream = res.bodyStream;
					Reactor reactor = Reactor.getCurrentReactor();
					if (bodyStream != null) {
						res.setBodyStream(bodyStream
								.withEndOfStream(eos -> eos
										.whenComplete(() -> reactor.post(res::recycle))));
					} else {
						reactor.post(res::recycle);
					}
					return Promise.of(res);
				});
	}

}
