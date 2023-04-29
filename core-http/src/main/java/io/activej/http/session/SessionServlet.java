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

package io.activej.http.session;

import io.activej.http.AsyncServlet;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;

import java.util.function.Function;

import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * This is a simple abstract reference implementation of a concept known as HTTP sessions.
 * It operates over some session storage, session ids that are somehow (usually through cookies)
 * encoded in the requests and two other servlets one for when the session object is present
 * and one when it's not - the latter one usually redirects to the main or login pages or something.
 * <p>
 * The session object is {@link HttpRequest#attach attached} to the request so that the first servlet
 * could then receive and use it.
 */
public final class SessionServlet<T> extends AbstractReactive
		implements AsyncServlet {
	private final ISessionStore<T> store;
	private final Function<HttpRequest, String> sessionIdExtractor;
	private final AsyncServlet publicServlet;
	private final AsyncServlet privateServlet;

	private SessionServlet(Reactor reactor, ISessionStore<T> store, Function<HttpRequest, String> sessionIdExtractor, AsyncServlet publicServlet, AsyncServlet privateServlet) {
		super(reactor);
		this.store = store;
		this.sessionIdExtractor = sessionIdExtractor;
		this.publicServlet = publicServlet;
		this.privateServlet = privateServlet;
	}

	public static <T> SessionServlet<T> create(Reactor reactor, ISessionStore<T> store, String sessionIdCookie,
			AsyncServlet publicServlet,
			AsyncServlet privateServlet) {
		return new SessionServlet<>(reactor, store, request -> request.getCookie(sessionIdCookie), publicServlet, privateServlet);
	}

	public static <T> SessionServlet<T> create(Reactor reactor, ISessionStore<T> store, Function<HttpRequest, String> sessionIdExtractor,
			AsyncServlet publicServlet,
			AsyncServlet privateServlet) {
		return new SessionServlet<>(reactor, store, sessionIdExtractor, publicServlet, privateServlet);
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws Exception {
		checkInReactorThread(this);
		String id = sessionIdExtractor.apply(request);

		if (id == null) {
			return publicServlet.serve(request);
		}

		return store.get(id)
				.then(sessionObject -> {
					if (sessionObject != null) {
						request.attach(sessionObject);
						return privateServlet.serve(request);
					} else {
						return publicServlet.serve(request);
					}
				});
	}
}
