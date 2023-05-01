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

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.initializer.WithInitializer;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.http.Protocol.WS;
import static io.activej.http.Protocol.WSS;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This servlet allows building complex servlet trees, routing requests between them by the HTTP paths.
 */
public final class RoutingServlet extends AbstractReactive
		implements AsyncServlet, WithInitializer<RoutingServlet> {
	private static final String ROOT = "/";
	private static final String STAR = "*";
	private static final String WILDCARD = "/" + STAR;

	private static final int WS_ORDINAL = HttpMethod.values().length;
	private static final int ANY_HTTP_ORDINAL = WS_ORDINAL + 1;

	private final AsyncServlet[] servlets = new AsyncServlet[ANY_HTTP_ORDINAL + 1];
	private final AsyncServlet[] fallbackServlets = new AsyncServlet[ANY_HTTP_ORDINAL + 1];

	private final Map<String, RoutingServlet> routes = new HashMap<>();
	private final Map<String, RoutingServlet> parameters = new HashMap<>();

	private RoutingServlet(Reactor reactor) {
		super(reactor);
	}

	public static RoutingServlet.Builder builder(Reactor reactor) {
		return new RoutingServlet(reactor).new Builder();
	}

	public class Builder extends AbstractBuilder<Builder, RoutingServlet> {
		/**
		 * Maps given servlet on some path. Fails when such path already has a servlet mapped to it.
		 */
		public Builder with(String path, AsyncServlet servlet) {
			return with(null, path, servlet);
		}

		/**
		 * @see #with(HttpMethod, String, AsyncServlet)
		 */
		@Contract("_, _, _ -> this")
		public Builder with(@Nullable HttpMethod method, String path, AsyncServlet servlet) {
			checkNotNull(path);
			checkNotNull(servlet);
			return doMap(method == null ? ANY_HTTP_ORDINAL : method.ordinal(), path, servlet);
		}

		/**
		 * Maps given consumer of a web socket as a web socket servlet on some path.
		 * Fails if there is already a web socket servlet mapped on this path.
		 */
		@Contract("_, _ -> this")
		public Builder withWebSocket(String path, Consumer<IWebSocket> webSocketConsumer) {
			return withWebSocket(path, new WebSocketServlet(reactor) {
				@Override
				protected void onWebSocket(IWebSocket webSocket) {
					webSocketConsumer.accept(webSocket);
				}
			});
		}

		@Contract("_, _ -> this")
		public Builder withWebSocket(String path, WebSocketServlet servlet) {
			return doMap(WS_ORDINAL, path, servlet);
		}

		@Contract("_, _, _ -> this")
		private Builder doMap(int ordinal, String path, AsyncServlet servlet) {
			checkArgument(path.startsWith(ROOT) && (path.endsWith(WILDCARD) || !path.contains(STAR)), "Invalid path: " + path);
			if (path.endsWith(WILDCARD)) {
				RoutingServlet routingServlet = ensureChild(path.substring(0, path.length() - 2));
				RoutingServlet.set(routingServlet.fallbackServlets, ordinal, servlet);
			} else {
				RoutingServlet.set(ensureChild(path).servlets, ordinal, servlet);
			}
			return this;
		}

		@Contract("_ -> new")
		public Builder merge(RoutingServlet servlet) {
			return merge(ROOT, servlet);
		}

		@Contract("_, _ -> new")
		public Builder merge(String path, RoutingServlet servlet) {
			mergeInto(ensureChild(path), servlet);
			return this;
		}

		@Override
		protected RoutingServlet doBuild() {
			return RoutingServlet.this;
		}
	}

	public static RoutingServlet merge(RoutingServlet... servlets) {
		return merge(Arrays.asList(servlets));
	}

	public static RoutingServlet merge(List<RoutingServlet> servlets) {
		checkArgument(servlets.size() > 1, "Nothing to merge");
		Reactor reactor = servlets.get(0).reactor;
		for (RoutingServlet routingServlet : servlets) {
			checkArgument(reactor == routingServlet.reactor, "Different reactors");
		}

		RoutingServlet merged = new RoutingServlet(reactor);
		for (RoutingServlet servlet : servlets) {
			mergeInto(merged, servlet);
		}
		return merged;
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws Exception {
		checkInReactorThread(this);
		Promise<HttpResponse> processed = tryServe(request);
		return processed != null ?
				processed :
				Promise.ofException(HttpError.notFound404());
	}

	private @Nullable Promise<HttpResponse> tryServe(HttpRequest request) throws Exception {
		int introPosition = request.getPos();
		String urlPart = request.pollUrlPart();
		if (urlPart == null) {
			throw HttpError.badRequest400("Path contains bad percent encoding");
		}
		Protocol protocol = request.getProtocol();
		int ordinal = protocol == WS || protocol == WSS ? WS_ORDINAL : request.getMethod().ordinal();

		if (urlPart.isEmpty()) {
			AsyncServlet servlet = getOrDefault(servlets, ordinal);
			if (servlet != null) {
				return servlet.serve(request);
			}
		} else {
			int position = request.getPos();
			RoutingServlet transit = routes.get(urlPart);
			if (transit != null) {
				Promise<HttpResponse> result = transit.tryServe(request);
				if (result != null) {
					return result;
				}
				request.setPos(position);
			}
			for (Entry<String, RoutingServlet> entry : parameters.entrySet()) {
				String key = entry.getKey();
				request.putPathParameter(key, urlPart);
				Promise<HttpResponse> result = entry.getValue().tryServe(request);
				if (result != null) {
					return result;
				}
				request.removePathParameter(key);
				request.setPos(position);
			}
		}

		AsyncServlet servlet = getOrDefault(fallbackServlets, ordinal);
		if (servlet != null) {
			request.setPos(introPosition);
			return servlet.serve(request);
		}
		return null;
	}

	public @Nullable RoutingServlet getChild(String path) {
		return getChildImpl(path, (servlet, name) ->
				name.startsWith(":") ?
						servlet.parameters.get(name.substring(1)) :
						servlet.routes.get(name));
	}

	private RoutingServlet ensureChild(String path) {
		return getChildImpl(path, (servlet, name) ->
				name.startsWith(":") ?
						servlet.parameters.computeIfAbsent(name.substring(1), $ -> new RoutingServlet(reactor)) :
						servlet.routes.computeIfAbsent(name, $ -> new RoutingServlet(reactor)));
	}

	private RoutingServlet getChildImpl(String path, BiFunction<RoutingServlet, String, @Nullable RoutingServlet> childGetter) {
		if (path.isEmpty() || path.equals(ROOT)) {
			return this;
		}
		RoutingServlet sub = this;
		int slash = path.indexOf('/', 1);
		while (true) {
			String urlPart = path.substring(1, slash == -1 ? path.length() : slash);

			if (!urlPart.startsWith(":")) {
				urlPart = decodePattern(urlPart);
			}

			if (urlPart.isEmpty()) {
				return sub;
			}
			sub = childGetter.apply(sub, urlPart);

			if (slash == -1 || sub == null) {
				return sub;
			}
			path = path.substring(slash);
			slash = path.indexOf('/', 1);
		}
	}

	private static void mergeInto(RoutingServlet into, RoutingServlet from) {
		for (int i = 0; i < from.servlets.length; i++) {
			AsyncServlet rootServlet = from.servlets[i];
			if (rootServlet != null) {
				set(into.servlets, i, rootServlet);
			}
		}
		for (int i = 0; i < from.fallbackServlets.length; i++) {
			AsyncServlet fallbackServlet = from.fallbackServlets[i];
			if (fallbackServlet != null) {
				set(into.fallbackServlets, i, fallbackServlet);
			}
		}
		from.routes.forEach((key, value) ->
				into.routes.merge(key, value, (s1, s2) -> {
					mergeInto(s1, s2);
					return s1;
				}));
		from.parameters.forEach((key, value) ->
				into.parameters.merge(key, value, (s1, s2) -> {
					mergeInto(s1, s2);
					return s1;
				}));
	}

	private static void set(AsyncServlet[] servlets, int ordinal, AsyncServlet servlet) {
		checkArgument(servlets[ordinal] == null, "Already mapped");
		servlets[ordinal] = servlet;
	}

	private static @Nullable AsyncServlet getOrDefault(AsyncServlet[] servlets, int ordinal) {
		AsyncServlet maybeResult = servlets[ordinal];
		if (maybeResult != null || ordinal == WS_ORDINAL) {
			return maybeResult;
		}
		return servlets[ANY_HTTP_ORDINAL];
	}

	private static String decodePattern(String pattern) {
		try {
			return URLDecoder.decode(pattern, UTF_8);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Pattern contains bad percent encoding", e);
		}
	}
}
