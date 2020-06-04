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

import java.net.InetAddress;
import java.util.Arrays;

import static io.activej.common.Preconditions.checkArgument;

/**
 * Represents a resolved domain (of A or AAAA type)
 */
public final class DnsResourceRecord {
	private final InetAddress[] ips;
	private final int minTtl;

	private DnsResourceRecord(InetAddress[] ips, int minTtl) {
		this.ips = ips;
		this.minTtl = minTtl;
	}

	public static DnsResourceRecord of(InetAddress[] ips, int minTtl) {
		checkArgument(ips.length > 0, "Cannot create DNS record with no data");
		return new DnsResourceRecord(ips, minTtl);
	}

	public InetAddress[] getIps() {
		return ips;
	}

	public int getMinTtl() {
		return minTtl;
	}

	@Override
	public String toString() {
		return "DnsResourceRecord{ips=" + Arrays.toString(ips) + ", minTtl=" + minTtl + '}';
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DnsResourceRecord that = (DnsResourceRecord) o;

		return minTtl == that.minTtl && Arrays.equals(ips, that.ips);
	}

	@Override
	public int hashCode() {
		return 31 * Arrays.hashCode(ips) + minTtl;
	}
}
