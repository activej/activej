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

package io.activej.dns;

import io.activej.dns.protocol.*;
import io.activej.http.HttpUtils;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;

/**
 * Components implementing this interface can resolve given domain names into
 * their respective IP addresses.
 * If host is not recognized or connection to DNS server timed out it will
 * fail with a respective {@link DnsQueryException}.
 */
public interface AsyncDnsClient {
	/**
	 * Searches for an IPv4 for the given domain
	 *
	 * @param domainName domain name for to get IP for
	 */
	default Promise<DnsResponse> resolve4(String domainName) {
		return resolve(DnsQuery.ipv4(domainName));
	}

	/**
	 * Searches for an IPv6 for the given domain
	 *
	 * @param domainName domain name for to get IP for
	 */
	default Promise<DnsResponse> resolve6(String domainName) {
		return resolve(DnsQuery.ipv6(domainName));
	}

	/**
	 * Searches for an IP for the given query
	 *
	 * @param query domain and IP version
	 */
	Promise<DnsResponse> resolve(DnsQuery query);

	/**
	 * Closes the underlying UDP socket if it's open.
	 * Note that on next {@link #resolve} call it will be created again.
	 * Any pending requests will be completed with timeout exception.
	 */
	void close();

	/**
	 * Checks if query already contains an IP and returns fake {@link DnsResponse} for it and <code>null</code> otherwise.
	 *
	 * @param query query which might contain an IP
	 * @return fake query response if is does and <code>null</code> if it does not
	 */
	static @Nullable DnsResponse resolveFromQuery(DnsQuery query) {
		if ("localhost".equals(query.getDomainName())) {
			InetAddress[] ips = {InetAddress.getLoopbackAddress()};
			return DnsResponse.of(DnsTransaction.of((short) 0, query), DnsResourceRecord.of(ips, 0));
		}
		if (HttpUtils.isInetAddress(query.getDomainName())) {
			InetAddress[] ips = {HttpUtils.inetAddress(query.getDomainName())};
			return DnsResponse.of(DnsTransaction.of((short) 0, query), DnsResourceRecord.of(ips, 0));
		}
		return null;
	}
}
