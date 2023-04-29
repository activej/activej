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

import io.activej.async.function.AsyncBiPredicate;
import io.activej.common.builder.AbstractBuilder;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;

import java.util.Base64;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.activej.http.ContentTypes.PLAIN_TEXT_UTF_8;
import static io.activej.http.HttpHeaders.AUTHORIZATION;
import static io.activej.http.HttpHeaders.CONTENT_TYPE;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This is a simple reference implementation of the HTTP Basic Auth protocol.
 * <p>
 * It operates over some servlet that it restricts access to and the async predicate for the credentials.
 * <p>
 * Also the credentials are {@link HttpRequest#attach attached} to the request so that the private servlet
 * could then receive and use it.
 */
public final class BasicAuthServlet extends AbstractReactive
		implements AsyncServlet {

	public static final BiPredicate<String, String> SILLY = (login, pass) -> true;

	private static final String PREFIX = "Basic ";
	private static final Base64.Decoder DECODER = Base64.getDecoder();

	private final AsyncServlet next;
	private final String challenge;
	private final AsyncBiPredicate<String, String> credentialsLookup;

	private UnaryOperator<HttpResponse.Builder> failureResponse =
			response -> response
					.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(PLAIN_TEXT_UTF_8))
					.withBody("Authentication is required".getBytes(UTF_8));

	private BasicAuthServlet(Reactor reactor, AsyncServlet next, String realm, AsyncBiPredicate<String, String> credentialsLookup) {
		super(reactor);
		this.next = next;
		this.credentialsLookup = credentialsLookup;

		challenge = PREFIX + "realm=\"" + realm + "\", charset=\"UTF-8\"";
	}

	public static BasicAuthServlet create(Reactor reactor, AsyncServlet next, String realm, AsyncBiPredicate<String, String> credentialsLookup) {
		return builder(reactor, next, realm, credentialsLookup).build();
	}

	public static Builder builder(Reactor reactor, AsyncServlet next, String realm, AsyncBiPredicate<String, String> credentialsLookup) {
		return new BasicAuthServlet(reactor, next, realm, credentialsLookup).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, BasicAuthServlet> {
		private Builder() {}

		public Builder withFailureResponse(UnaryOperator<HttpResponse.Builder> failureResponse) {
			checkNotBuilt(this);
			BasicAuthServlet.this.failureResponse = failureResponse;
			return this;
		}

		@Override
		protected BasicAuthServlet doBuild() {
			return BasicAuthServlet.this;
		}
	}

	public static Function<AsyncServlet, AsyncServlet> decorator(Reactor reactor, String realm, AsyncBiPredicate<String, String> credentialsLookup) {
		return next -> create(reactor, next, realm, credentialsLookup);
	}

	public static Function<AsyncServlet, AsyncServlet> decorator(Reactor reactor, String realm,
			AsyncBiPredicate<String, String> credentialsLookup,
			UnaryOperator<HttpResponse.Builder> failureResponse) {
		return next -> builder(reactor, next, realm, credentialsLookup)
				.withFailureResponse(failureResponse)
				.build();
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws HttpError {
		checkInReactorThread(this);
		String header = request.getHeader(AUTHORIZATION);
		if (header == null || !header.startsWith(PREFIX)) {
			return Promise.of(failureResponse.apply(HttpResponse.Builder.unauthorized401(challenge)).build());
		}
		byte[] raw;
		try {
			raw = DECODER.decode(header.substring(PREFIX.length()));
		} catch (IllegalArgumentException e) {
			// all the messages in decode method's illegal argument exception are informative enough
			throw HttpError.ofCode(400, "Base64: " + e.getMessage());
		}
		String[] authData = new String(raw, UTF_8).split(":", 2);
		if (authData.length != 2) {
			throw HttpError.ofCode(400, "No ':' separator");
		}
		return credentialsLookup.test(authData[0], authData[1])
				.then(ok -> {
					if (!ok) return Promise.of(failureResponse.apply(HttpResponse.Builder.unauthorized401(challenge)).build());
					request.attach(new BasicAuthCredentials(authData[0], authData[1]));
					return next.serve(request);
				});
	}

	public record BasicAuthCredentials(String username, String password) {
	}

	public static BiPredicate<String, String> lookupFrom(Map<String, String> credentials) {
		return (login, pass) -> pass.equals(credentials.get(login));
	}
}
