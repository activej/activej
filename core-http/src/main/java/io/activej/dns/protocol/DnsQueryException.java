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

/**
 * Represents a failed DNS query response as a exception.
 */
public final class DnsQueryException extends Exception {
	private final DnsQuery query;
	private final DnsResponse result;

	/**
	 * Creates a new instance of DnsQueryException
	 */
	public DnsQueryException(DnsResponse response) {
		super(response.getTransaction().getQuery() + " failed with error code: " + response.getErrorCode().name());
		this.query = response.getTransaction().getQuery();
		this.result = response;
	}

	public DnsQuery getQuery() {
		return query;
	}

	public DnsResponse getResult() {
		return result;
	}
}
