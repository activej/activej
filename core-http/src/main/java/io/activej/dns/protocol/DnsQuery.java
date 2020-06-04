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

package io.activej.dns.protocol;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/***
 * A simple DNS query, which specifies a domain name and record type (one of A or AAAA).
 */
public final class DnsQuery {
	private final String domainName;
	private final DnsProtocol.RecordType recordType;

	private DnsQuery(@NotNull String domainName, @NotNull DnsProtocol.RecordType recordType) {
		this.domainName = domainName;
		this.recordType = recordType;
	}

	public static DnsQuery of(String domainName, DnsProtocol.RecordType recordType) {
		return new DnsQuery(domainName, recordType);
	}

	/**
	 * Shortcut to create {@link DnsQuery} with A (IPv4) record type.
	 */
	public static DnsQuery ipv4(String domainName) {
		return new DnsQuery(domainName, DnsProtocol.RecordType.A);
	}

	/**
	 * Shortcut to create {@link DnsQuery} with AAAA (IPv6) record type.
	 */
	public static DnsQuery ipv6(String domainName) {
		return new DnsQuery(domainName, DnsProtocol.RecordType.AAAA);
	}

	public String getDomainName() {
		return domainName;
	}

	public DnsProtocol.RecordType getRecordType() {
		return recordType;
	}

	@Override
	public String toString() {
		return "DnsQuery{domainName='" + domainName + "', recordType=" + recordType + '}';
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DnsQuery dnsQuery = (DnsQuery) o;

		return domainName.equals(dnsQuery.domainName) && recordType == dnsQuery.recordType;
	}

	@Override
	public int hashCode() {
		return 31 * domainName.hashCode() + recordType.hashCode();
	}
}
