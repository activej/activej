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

import org.jetbrains.annotations.Nullable;

import java.util.Objects;

import static io.activej.common.Checks.checkArgument;
import static io.activej.dns.protocol.DnsProtocol.ResponseErrorCode.NO_ERROR;

/**
 * Represents a response from DNS server.
 */
public final class DnsResponse {
	private final DnsTransaction transaction;
	private final @Nullable DnsResourceRecord record;
	private final DnsProtocol.ResponseErrorCode errorCode;

	private DnsResponse(DnsTransaction transaction, @Nullable DnsResourceRecord record, DnsProtocol.ResponseErrorCode errorCode) {
		this.transaction = transaction;
		this.errorCode = errorCode;
		this.record = record;
	}

	public static DnsResponse of(DnsTransaction transactionId, DnsResourceRecord record) {
		return new DnsResponse(transactionId, record, NO_ERROR);
	}

	public static DnsResponse ofFailure(DnsTransaction transactionId, DnsProtocol.ResponseErrorCode errorCode) {
		checkArgument(errorCode != NO_ERROR, "Creating failure DNS query response with NO_ERROR error code");
		return new DnsResponse(transactionId, null, errorCode);
	}

	public boolean isSuccessful() {
		return record != null;
	}

	public DnsTransaction getTransaction() {
		return transaction;
	}

	public DnsProtocol.ResponseErrorCode getErrorCode() {
		return errorCode;
	}

	public @Nullable DnsResourceRecord getRecord() {
		return record;
	}

	@Override
	public String toString() {
		return "DnsResponse{transaction=" + transaction + ", record=" + record + ", errorCode=" + errorCode + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		DnsResponse that = (DnsResponse) o;
		return transaction.equals(that.transaction) &&
				Objects.equals(record, that.record) &&
				errorCode == that.errorCode;
	}

	@Override
	public int hashCode() {
		return 31 * (31 * transaction.hashCode() + (record != null ? record.hashCode() : 0)) + errorCode.hashCode();
	}
}
