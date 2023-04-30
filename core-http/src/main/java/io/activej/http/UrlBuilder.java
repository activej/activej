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

import io.activej.common.builder.Builder;
import io.activej.common.initializer.WithInitializer;
import org.jetbrains.annotations.Nullable;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class UrlBuilder implements Builder<String>, WithInitializer<UrlBuilder> {
	private @Nullable String scheme;
	private @Nullable String userinfo;
	private @Nullable String host;
	private int port = 0;

	private final List<String> path = new ArrayList<>();

	private record QueryKV(String k, Object v) {}

	private final List<QueryKV> query = new ArrayList<>();

	private @Nullable String fragment;

	private UrlBuilder() {
	}

	public static UrlBuilder create() {
		return new UrlBuilder();
	}

	public static UrlBuilder http(String host) {
		return create().withScheme("http").withHost(host);
	}

	public static UrlBuilder http(String host, int port) {
		return create().withScheme("http").withHost(host, port);
	}

	public static UrlBuilder https(String host) {
		return create().withScheme("https").withHost(host);
	}

	public static UrlBuilder https(String host, int port) {
		return create().withScheme("https").withHost(host, port);
	}

	public UrlBuilder withScheme(String scheme) {
		this.scheme = scheme;
		return this;
	}

	public UrlBuilder withAuth(String userinfo) {
		this.userinfo = userinfo;
		return this;
	}

	public UrlBuilder withHost(String host) {
		this.host = host;
		return this;
	}

	public UrlBuilder withHost(String host, int port) {
		this.host = host;
		this.port = port;
		return this;
	}

	public UrlBuilder withPort(int port) {
		this.port = port;
		return this;
	}

	public UrlBuilder withPath(String part) {
		path.add(part);
		return this;
	}

	public UrlBuilder withPath(String... parts) {
		return withPath(Arrays.asList(parts));
	}

	public UrlBuilder withPath(List<String> parts) {
		path.addAll(parts);
		return this;
	}

	public UrlBuilder withQuery(String key, Object value) {
		query.add(new QueryKV(key, value));
		return this;
	}

	public UrlBuilder withFragment(String fragment) {
		this.fragment = fragment;
		return this;
	}

	@Override
	public String build() {
		return toString();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (scheme != null) {
			sb.append(scheme).append(':');
		}
		if (host != null) {
			sb.append("//");
			if (userinfo != null) {
				sb.append(userinfo).append('@');
			}
			sb.append(host);
			if (port > 0) {
				sb.append(':').append(port);
			}
			sb.append('/');
		}
		if (!path.isEmpty()) {
			for (String p : path) {
				sb.append(urlEncode(p)).append('/');
			}
			sb.setLength(sb.length() - 1); // drop last '/'
		}
		if (!query.isEmpty()) {
			sb.append('?');
			for (QueryKV queryKV : query) {
				sb.append(urlEncode(queryKV.k)).append('=').append(urlEncode(queryKV.v.toString())).append('&');
			}
			sb.setLength(sb.length() - 1); // drop last '&'
		}
		if (fragment != null) {
			sb.append('#').append(fragment);
		}
		return sb.toString();
	}

	private static String urlEncode(String str) {
		return URLEncoder.encode(str, UTF_8);
	}
}
