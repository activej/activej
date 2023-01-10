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

import io.activej.common.initializer.WithInitializer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static io.activej.common.Checks.checkArgument;
import static io.activej.http.Protocol.WS;
import static io.activej.http.Protocol.WSS;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This servlet allows building complex servlet trees, routing requests between them by the HTTP paths.
 */
public final class RoutingServlet implements AsyncServlet, WithInitializer<RoutingServlet> {
	private static final String ROOT = "/";
	private static final String STAR = "*";
	private static final String WILDCARD = "/" + STAR;

	private static final int WS_ORDINAL = HttpMethod.values().length;
	private static final int ANY_HTTP_ORDINAL = WS_ORDINAL + 1;

	private final AsyncServlet[] rootServlets = new AsyncServlet[ANY_HTTP_ORDINAL + 1];
	private final AsyncServlet[] fallbackServlets = new AsyncServlet[ANY_HTTP_ORDINAL + 1];

	private final Map<String, RoutingServlet> routes = new HashMap<>();
	private final Map<String, RoutingServlet> parameters = new HashMap<>();

	private RoutingServlet() {
	}

	public static RoutingServlet create() {
		return new RoutingServlet();
	}

	public static RoutingServlet wrap(AsyncServlet servlet) {
		RoutingServlet wrapper = new RoutingServlet();
		wrapper.fallbackServlets[ANY_HTTP_ORDINAL] = servlet;
		return wrapper;
	}

	/**
	 * Maps given servlet on some path. Fails when such path already has a servlet mapped to it.
	 */
	public RoutingServlet map(String path, AsyncServlet servlet) {
		return map(null, path, servlet);
	}

	/**
	 * @see #map(HttpMethod, String, AsyncServlet)
	 */
	@Contract("_, _, _ -> this")
	public RoutingServlet map(@Nullable HttpMethod method, String path, AsyncServlet servlet) {
		return doMap(method == null ? ANY_HTTP_ORDINAL : method.ordinal(), path, servlet);
	}

	/**
	 * Maps given consumer of a web socket as a web socket servlet on some path.
	 * Fails if there is already a web socket servlet mapped on this path.
	 */
	@Contract("_, _ -> this")
	public RoutingServlet mapWebSocket(String path, Consumer<AsyncWebSocket> webSocketConsumer) {
		return mapWebSocket(path, new WebSocketServlet() {
			@Override
			protected void onWebSocket(AsyncWebSocket webSocket) {
				webSocketConsumer.accept(webSocket);
			}
		});
	}

	@Contract("_, _ -> this")
	public RoutingServlet mapWebSocket(String path, WebSocketServlet servlet) {
		return doMap(WS_ORDINAL, path, servlet);
	}

	@Contract("_, _, _ -> this")
	private RoutingServlet doMap(int ordinal, String path, AsyncServlet servlet) {
		checkArgument(path.startsWith(ROOT) && (path.endsWith(WILDCARD) || !path.contains(STAR)), "Invalid path: " + path);
		if (path.endsWith(WILDCARD)) {
			makeSubtree(path.substring(0, path.length() - 2)).mapFallback(ordinal, servlet);
		} else {
			makeSubtree(path).map(ordinal, servlet);
		}
		return this;
	}

	public void visit(Visitor visitor) {
		visit(ROOT, visitor);
	}

	private void visit(String prefix, Visitor visitor) {
		visitServlets(rootServlets, visitor, prefix);
		visitServlets(fallbackServlets, visitor, prefix);
		routes.forEach((route, subtree) -> subtree.visit(prefix + route + "/", visitor));
		parameters.forEach((route, subtree) -> subtree.visit(prefix + ":" + route + "/", visitor));
	}

	private void visitServlets(AsyncServlet[] servlets, Visitor visitor, String prefix) {
		for (int i = 0; i < servlets.length; i++) {
			AsyncServlet servlet = servlets[i];
			if (servlet != null) {
				HttpMethod method = i == WS_ORDINAL || i == ANY_HTTP_ORDINAL ? null : HttpMethod.values()[i];
				visitor.accept(method, prefix, servlet);
			}
		}
	}

	public @Nullable RoutingServlet getSubtree(String path) {
		return getOrCreateSubtree(path, (servlet, name) ->
				name.startsWith(":") ?
						servlet.parameters.get(name.substring(1)) :
						servlet.routes.get(name));
	}

	@Contract("_ -> new")
	public RoutingServlet merge(RoutingServlet servlet) {
		return merge(ROOT, servlet);
	}

	@Contract("_, _ -> new")
	public RoutingServlet merge(String path, RoutingServlet servlet) {
		RoutingServlet merged = new RoutingServlet();
		mergeInto(merged, this);
		mergeInto(merged.makeSubtree(path), servlet);
		return merged;
	}

	@Override
	public Promise<HttpResponse> serve(HttpRequest request) throws Exception {
		Promise<HttpResponse> processed = tryServe(request);
		return processed != null ?
				processed :
				Promise.ofException(HttpError.notFound404());
	}

	private void map(int ordinal, AsyncServlet servlet) {
		doMerge(rootServlets, ordinal, servlet);
	}

	private void mapFallback(int ordinal, AsyncServlet servlet) {
		doMerge(fallbackServlets, ordinal, servlet);
	}

	private void doMerge(AsyncServlet[] servlets, int ordinal, AsyncServlet servlet) {
		if (servlets[ordinal] != null) {
			throw new IllegalArgumentException("Already mapped");
		}
		servlets[ordinal] = servlet;
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
			AsyncServlet servlet = getOrDefault(rootServlets, ordinal);
			if (servlet != null) {
				return servlet.serveAsync(request);
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
			return servlet.serveAsync(request);
		}
		return null;
	}

	private RoutingServlet makeSubtree(String path) {
		return getOrCreateSubtree(path, (servlet, name) ->
				name.startsWith(":") ?
						servlet.parameters.computeIfAbsent(name.substring(1), $ -> new RoutingServlet()) :
						servlet.routes.computeIfAbsent(name, $ -> new RoutingServlet()));
	}

	private RoutingServlet getOrCreateSubtree(String path, BiFunction<RoutingServlet, String, @Nullable RoutingServlet> childGetter) {
		if (path.isEmpty() || path.equals(ROOT)) {
			return this;
		}
		RoutingServlet sub = this;
		int slash = path.indexOf('/', 1);
		String remainingPath = path;
		while (true) {
			String urlPart = remainingPath.substring(1, slash == -1 ? remainingPath.length() : slash);

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
			remainingPath = remainingPath.substring(slash);
			slash = remainingPath.indexOf('/', 1);
		}
	}

	public static RoutingServlet merge(RoutingServlet first, RoutingServlet second) {
		RoutingServlet merged = new RoutingServlet();
		mergeInto(merged, first);
		mergeInto(merged, second);
		return merged;
	}

	public static RoutingServlet merge(RoutingServlet... servlets) {
		RoutingServlet merged = new RoutingServlet();
		for (RoutingServlet servlet : servlets) {
			mergeInto(merged, servlet);
		}
		return merged;
	}

	private static void mergeInto(RoutingServlet into, RoutingServlet from) {
		for (int i = 0; i < from.rootServlets.length; i++) {
			AsyncServlet rootServlet = from.rootServlets[i];
			if (rootServlet != null) {
				into.map(i, rootServlet);
			}
		}
		for (int i = 0; i < from.fallbackServlets.length; i++) {
			AsyncServlet fallbackServlet = from.fallbackServlets[i];
			if (fallbackServlet != null) {
				into.mapFallback(i, fallbackServlet);
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

	private static @Nullable AsyncServlet getOrDefault(AsyncServlet[] servlets, int ordinal) {
		AsyncServlet maybeResult = servlets[ordinal];
		if (maybeResult != null || ordinal == WS_ORDINAL) {
			return maybeResult;
		}
		return servlets[ANY_HTTP_ORDINAL];
	}

	private static String decodePattern(String pattern) {
		try {
			return URLDecoder.decode(pattern, UTF_8.name());
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError();
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Pattern contains bad percent encoding", e);
		}
	}

	@FunctionalInterface
	public interface Visitor {
		void accept(@Nullable HttpMethod method, String path, AsyncServlet servlet);
	}
}
