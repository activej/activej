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

import io.activej.common.annotation.Beta;
import io.activej.common.api.WithInitializer;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;
import static io.activej.http.Protocol.WS;
import static io.activej.http.Protocol.WSS;
import static io.activej.http.WebSocketAdapter.webSocket;

/**
 * This servlet allows to build complex servlet trees, routing requests between them by the HTTP paths.
 */
public final class RoutingServlet implements AsyncServlet, WithInitializer<RoutingServlet> {
	private static final String ROOT = "/";
	private static final String STAR = "*";
	private static final String WILDCARD = "/" + STAR;

	private static final BinaryOperator<AsyncServlet> DEFAULT_MERGER = ($, $2) -> {
		throw new IllegalArgumentException("Already mapped");
	};
	protected final Map<MappingKey, AsyncServlet> rootServlets = new HashMap<>();

	protected final Map<String, RoutingServlet> routes = new HashMap<>();
	protected final Map<String, RoutingServlet> parameters = new HashMap<>();

	protected final Map<MappingKey, AsyncServlet> fallbackServlets = new HashMap<>();

	private RoutingServlet() {
	}

	public static RoutingServlet create() {
		return new RoutingServlet();
	}

	public static RoutingServlet wrap(AsyncServlet servlet) {
		RoutingServlet wrapper = new RoutingServlet();
		wrapper.fallbackServlets.put(new MappingKey(null), servlet);
		return wrapper;
	}

	@Beta
	public static RoutingServlet wrapWebSocket(AsyncWebSocketServlet servlet) {
		RoutingServlet wrapper = new RoutingServlet();
		wrapper.fallbackServlets.put(new MappingKey(), webSocket(servlet));
		return wrapper;
	}

	/**
	 * Maps given servlet on some path. Fails when such path already has a servlet mapped to it.
	 */
	public RoutingServlet map(@NotNull String path, @NotNull AsyncServlet servlet) {
		return map(null, path, servlet, DEFAULT_MERGER);
	}

	/**
	 * Maps given servlet on some path and calls the merger if there is already a servlet there.
	 */
	public RoutingServlet map(@NotNull String path, @NotNull AsyncServlet servlet, @NotNull BinaryOperator<AsyncServlet> merger) {
		return map(null, path, servlet, merger);
	}

	/**
	 * @see #map(HttpMethod, String, AsyncServlet)
	 */
	@Contract("_, _, _ -> this")
	public RoutingServlet map(@Nullable HttpMethod method, @NotNull String path, @NotNull AsyncServlet servlet) {
		return map(method, path, servlet, DEFAULT_MERGER);
	}

	/**
	 * @see #map(HttpMethod, String, AsyncServlet, BinaryOperator)
	 */
	@Contract("_, _, _, _ -> this")
	public RoutingServlet map(@Nullable HttpMethod method, @NotNull String path, @NotNull AsyncServlet servlet, @NotNull BinaryOperator<AsyncServlet> merger) {
		return doMap(new MappingKey(method), path, servlet, merger);
	}

	/**
	 * Maps given consumer of a web socket as a web socket servlet on some path.
	 * Fails if there is already a web socket servlet mapped on this path.
	 */
	@Beta
	@Contract("_, _ -> this")
	public RoutingServlet mapWebSocket(@NotNull String path, Consumer<WebSocket> webSocketConsumer) {
		return mapWebSocket(path, webSocketConsumer, DEFAULT_MERGER);
	}

	/**
	 * Maps given consumer of a web socket as a web socket servlet on some path.
	 * Calls the merger if there is already a web socket servlet mapped on this path.
	 */
	@Beta
	@Contract("_, _, _ -> this")
	public RoutingServlet mapWebSocket(@NotNull String path, Consumer<WebSocket> webSocketConsumer, @NotNull BinaryOperator<AsyncServlet> merger) {
		return mapWebSocket(path, ($, fn) -> fn.apply(HttpResponse.ofCode(101)).whenResult(webSocketConsumer), merger);
	}

	/**
	 * Maps given web socket servlet on some path.
	 * Fails if there is already a web socket servlet mapped on this path.
	 */
	@Beta
	@Contract("_, _ -> this")
	public RoutingServlet mapWebSocket(@NotNull String path, @NotNull AsyncWebSocketServlet servlet) {
		return mapWebSocket(path, servlet, DEFAULT_MERGER);
	}

	/**
	 * Maps given web socket servlet on some path.
	 * Calls the merger if there is already a web socket servlet mapped on this path.
	 */
	@Beta
	@Contract("_, _, _ -> this")
	public RoutingServlet mapWebSocket(@NotNull String path, @NotNull AsyncWebSocketServlet servlet, @NotNull BinaryOperator<AsyncServlet> merger) {
		return doMap(new MappingKey(), path, webSocket(servlet), merger);
	}

	@Contract("_, _, _, _ -> this")
	private RoutingServlet doMap(MappingKey key, @NotNull String path, @NotNull AsyncServlet servlet, @NotNull BinaryOperator<AsyncServlet> merger) {
		checkArgument(path.startsWith(ROOT) && (path.endsWith(WILDCARD) || !path.contains(STAR)), "Invalid path: " + path);
		if (path.endsWith(WILDCARD)) {
			makeSubtree(path.substring(0, path.length() - 2)).mapFallback(key, servlet, merger);
		} else {
			makeSubtree(path).map(key, servlet, merger);
		}
		return this;
	}

	public void visit(Visitor visitor) {
		visit(ROOT, visitor);
	}

	private void visit(String prefix, Visitor visitor) {
		rootServlets.forEach((key, servlet) -> visitor.accept(key.method, prefix, servlet));
		fallbackServlets.forEach((key, servlet) -> visitor.accept(key.method, prefix + STAR, servlet));
		routes.forEach((route, subtree) -> subtree.visit(prefix + route + "/", visitor));
		parameters.forEach((route, subtree) -> subtree.visit(prefix + ":" + route + "/", visitor));
	}

	@Nullable
	public RoutingServlet getSubtree(String path) {
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
		return merge(path, servlet, DEFAULT_MERGER);
	}

	@Contract("_, _, _ -> new")
	public RoutingServlet merge(String path, RoutingServlet servlet, BinaryOperator<AsyncServlet> merger) {
		RoutingServlet merged = new RoutingServlet();
		mergeInto(merged, this, merger);
		mergeInto(merged.makeSubtree(path), servlet, merger);
		return merged;
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) {
		Promise<HttpResponse> processed = tryServe(request);
		return processed != null ?
				processed :
				Promise.ofException(HttpException.notFound404());
	}

	private void map(MappingKey key, @NotNull AsyncServlet servlet, @NotNull BinaryOperator<AsyncServlet> merger) {
		rootServlets.merge(key, servlet, merger);
	}

	private void mapFallback(MappingKey key, @NotNull AsyncServlet servlet, @NotNull BinaryOperator<AsyncServlet> merger) {
		fallbackServlets.merge(key, servlet, merger);
	}

	@Nullable
	private Promise<HttpResponse> tryServe(HttpRequest request) {
		int introPosition = request.getPos();
		String urlPart = request.pollUrlPart();
		Protocol protocol = request.getProtocol();
		MappingKey mappingKey = protocol == WS || protocol == WSS ? new MappingKey() : new MappingKey(request.getMethod());

		if (urlPart.isEmpty()) {
			AsyncServlet servlet = getOrDefault(rootServlets, mappingKey);
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

		AsyncServlet servlet = getOrDefault(fallbackServlets, mappingKey);
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

	private RoutingServlet getOrCreateSubtree(@NotNull String path, BiFunction<RoutingServlet, String, @Nullable RoutingServlet> childGetter) {
		if (path.isEmpty() || path.equals(ROOT)) {
			return this;
		}
		RoutingServlet sub = this;
		int slash = path.indexOf('/', 1);
		String remainingPath = path;
		while (true) {
			String urlPart = remainingPath.substring(1, slash == -1 ? remainingPath.length() : slash);

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
		return merge(first, second, DEFAULT_MERGER);
	}

	public static RoutingServlet merge(RoutingServlet... servlets) {
		return merge(DEFAULT_MERGER, servlets);
	}

	public static RoutingServlet merge(RoutingServlet first, RoutingServlet second, BinaryOperator<AsyncServlet> merger) {
		RoutingServlet merged = new RoutingServlet();
		mergeInto(merged, first, merger);
		mergeInto(merged, second, merger);
		return merged;
	}

	public static RoutingServlet merge(BinaryOperator<AsyncServlet> merger, RoutingServlet... servlets) {
		RoutingServlet merged = new RoutingServlet();
		for (RoutingServlet servlet : servlets) {
			mergeInto(merged, servlet, merger);
		}
		return merged;
	}

	private static void mergeInto(RoutingServlet into, RoutingServlet from, BinaryOperator<AsyncServlet> merger) {
		from.rootServlets.forEach((key, servlet) -> into.map(key, servlet, merger));
		from.fallbackServlets.forEach((key, servlet) -> into.mapFallback(key, servlet, merger));
		from.routes.forEach((key, value) ->
				into.routes.merge(key, value, (s1, s2) -> {
					mergeInto(s1, s2, merger);
					return s1;
				}));
		from.parameters.forEach((key, value) ->
				into.parameters.merge(key, value, (s1, s2) -> {
					mergeInto(s1, s2, merger);
					return s1;
				}));
	}

	@Nullable
	private static AsyncServlet getOrDefault(Map<MappingKey, AsyncServlet> map, MappingKey key) {
		AsyncServlet maybeResult = map.get(key);
		if (maybeResult != null || key.isWebSocket) {
			return maybeResult;
		}
		return map.get(new MappingKey(null));
	}

	private static final class MappingKey {
		@Nullable
		private final HttpMethod method;
		private final boolean isWebSocket;

		private MappingKey(@Nullable HttpMethod method) {
			this.method = method;
			this.isWebSocket = false;
		}

		private MappingKey() {
			this.method = null;
			this.isWebSocket = true;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			MappingKey that = (MappingKey) o;

			if (isWebSocket != that.isWebSocket) return false;
			return method == that.method;
		}

		@Override
		public int hashCode() {
			int result = method != null ? method.hashCode() : 0;
			result = 31 * result + (isWebSocket ? 1 : 0);
			return result;
		}
	}

	/**
	 * A servlet for handling web socket upgrade requests.
	 * An implementation may inspect incoming HTTP request, based on which an implementation MUST call a provided function
	 * with a corresponding HTTP response.
	 * <p>
	 * If a response has a code {@code 101} it is considered successful and the resulted promise of a web socket will be
	 * completed with a {@link WebSocket}. A successful response must have no body or body stream.
	 * <p>
	 * If a response has code different than {@code 101}, it will be sent as is and the resulted promise will be completed
	 * exceptionally.
	 */
	@Beta
	public interface AsyncWebSocketServlet {
		void serve(HttpRequest request, Function<HttpResponse, Promise<WebSocket>> fn);
	}

	@FunctionalInterface
	public interface Visitor {
		void accept(@Nullable HttpMethod method, String path, AsyncServlet servlet);
	}
}
