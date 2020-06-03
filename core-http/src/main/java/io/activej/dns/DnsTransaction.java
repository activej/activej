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

/**
 * Represents a DNS transaction, instances are used to tie queries to responses
 */
public final class DnsTransaction {
	private final short id;
	private final DnsQuery query;

	private DnsTransaction(short id, DnsQuery query) {
		this.id = id;
		this.query = query;
	}

	public static DnsTransaction of(short transactionId, DnsQuery query) {
		return new DnsTransaction(transactionId, query);
	}

	public short getId() {
		return id;
	}

	public DnsQuery getQuery() {
		return query;
	}

	@Override
	public String toString() {
		return "DnsTransaction{id=" + Integer.toHexString(id & 0xffff) + ", query='" + query + "'}";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DnsTransaction that = (DnsTransaction) o;

		return id == that.id && query.equals(that.query);
	}

	@Override
	public int hashCode() {
		return 31 * id + query.hashCode();
	}
}
