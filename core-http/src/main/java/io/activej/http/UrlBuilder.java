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

import org.jetbrains.annotations.Nullable;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class UrlBuilder {

	private final @Nullable String scheme;

	private final List<String> path = new LinkedList<>();
	private final Map<String, String> query = new LinkedHashMap<>();

	private @Nullable String userInfo;
	private @Nullable String host;
	private @Nullable String port;
	private @Nullable String fragment;

	private UrlBuilder(@Nullable String scheme) {
		this.scheme = scheme;
	}

	public static UrlBuilder of(String scheme) {
		return new UrlBuilder(scheme);
	}

	public static UrlBuilder http() {
		return new UrlBuilder("http");
	}

	public static UrlBuilder https() {
		return new UrlBuilder("https");
	}

	public UrlBuilder withAuthority(String userInfo, InetSocketAddress address) {
		String host;
		if (address.isUnresolved()) {
			host = address.getHostName();
		} else {
			InetAddress inetAddress = address.getAddress();
			host = inetAddress.getHostAddress();
			if (inetAddress instanceof Inet6Address) {
				host = '[' + host.replace("%", "%25") + ']'; // yay, IPv6 syntax?
			}
		}
		return withAuthority(userInfo, host, address.getPort());
	}

	public UrlBuilder withAuthority(String host) {
		this.host = host;
		return this;
	}

	public UrlBuilder withAuthority(String host, int port) {
		checkArgument(port >= 0 && port <= 49151, "Port should in range [0, 49151]"); // exclude ephemeral ports (https://tools.ietf.org/html/rfc6335#section-6)
		this.port = Integer.toString(port);
		return withAuthority(host);
	}

	public UrlBuilder withAuthority(String userInfo, String host) {
		this.userInfo = userInfo;
		return withAuthority(host);
	}

	public UrlBuilder withAuthority(String userInfo, String host, int port) {
		this.userInfo = userInfo;
		return withAuthority(host, port);
	}

	public UrlBuilder withAuthority(InetSocketAddress address) {
		return withAuthority(address.isUnresolved() ? address.getHostName() : address.getAddress().getHostAddress(), address.getPort());
	}

	public static UrlBuilder relative() {
		return new UrlBuilder(null);
	}

	public static String mapToQuery(Map<String, ?> query) {
		StringBuilder sb = new StringBuilder();
		query.forEach((k, v) -> sb.append(urlEncode(k)).append('=').append(urlEncode(v.toString())).append('&'));
		sb.setLength(sb.length() - 1); // drop last '&'
		return sb.toString();
	}

	public UrlBuilder appendPathPart(String part) {
		path.add(part);
		return this;
	}

	public UrlBuilder appendPathPart(HttpPathPart pathPart) {
		path.add(pathPart.toString());
		return this;
	}

	public UrlBuilder appendPath(String pathTail) {
		path.addAll(List.of(pathTail.split("/")));
		return this;
	}

	public UrlBuilder appendQuery(String key, Object value) {
		query.put(key, value.toString());
		return this;
	}

	public UrlBuilder withFragment(String fragment) {
		this.fragment = fragment;
		return this;
	}

	public static String urlEncode(String str) {
		return URLEncoder.encode(str, UTF_8);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (scheme != null) {
			sb.append(scheme).append(':');
		}
		if (host != null) {
			sb.append("//");
			if (userInfo != null) {
				sb.append(userInfo).append('@');
			}
			sb.append(host);
			if (port != null) {
				sb.append(':').append(port);
			}
			sb.append('/');
		}
		if (!path.isEmpty()) {
			path.forEach(p -> sb.append(urlEncode(p)).append('/'));
			sb.setLength(sb.length() - 1); // drop last '/'
		}
		if (!query.isEmpty()) {
			sb.append('?');
			query.forEach((k, v) -> sb.append(urlEncode(k)).append('=').append(urlEncode(v)).append('&'));
			sb.setLength(sb.length() - 1); // drop last '&'
		}
		if (fragment != null) {
			sb.append('#').append(fragment);
		}
		return sb.toString();
	}
}
