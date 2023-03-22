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

package io.activej.crdt.storage.cluster;

import com.dslplatform.json.CompiledJson;
import io.activej.common.exception.MalformedDataException;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Objects;

import static io.activej.common.StringFormatUtils.parseInetSocketAddressResolving;

public final class PartitionId {
	private final String id;
	private final @Nullable InetSocketAddress crdtAddress;
	private final @Nullable InetSocketAddress rpcAddress;

	private PartitionId(String id, @Nullable InetSocketAddress crdtAddress, @Nullable InetSocketAddress rpcAddress) {
		this.id = id;
		this.crdtAddress = crdtAddress;
		this.rpcAddress = rpcAddress;
	}

	public static PartitionId of(String id) {
		return new PartitionId(id, null, null);
	}

	public static PartitionId of(String id, @Nullable InetSocketAddress crdt, @Nullable InetSocketAddress rpc) {
		return new PartitionId(id, crdt, rpc);
	}

	@CompiledJson
	static PartitionId jsonFactory(String id, InetSocketAddress crdtAddress, InetSocketAddress rpcAddress) {
		return new PartitionId(id, crdtAddress, rpcAddress);
	}

	public static PartitionId ofCrdtAddress(String id, InetSocketAddress crdtAddress) {
		return new PartitionId(id, crdtAddress, null);
	}

	public static PartitionId ofRpcAddress(String id, InetSocketAddress rpcAddress) {
		return new PartitionId(id, null, rpcAddress);
	}

	public static PartitionId parseString(String string) throws MalformedDataException {
		String[] split = string.split("\\|");
		if (split.length > 3) {
			throw new MalformedDataException("");
		}
		String id = split[0];
		InetSocketAddress crdtAddress = split.length > 1 && !split[1].trim().isEmpty() ? parseInetSocketAddressResolving(split[1]) : null;
		InetSocketAddress rpcAddress = split.length > 2 && !split[2].trim().isEmpty() ? parseInetSocketAddressResolving(split[2]) : null;

		return new PartitionId(id, crdtAddress, rpcAddress);
	}

	public String getId() {
		return id;
	}

	@SuppressWarnings("NullableProblems")
	public InetSocketAddress getCrdtAddress() {
		return crdtAddress;
	}

	@SuppressWarnings("NullableProblems")
	public InetSocketAddress getRpcAddress() {
		return rpcAddress;
	}

	private static String addressToString(@Nullable InetSocketAddress address) {
		return address == null ?
				"" :
				address.getAddress().getHostAddress() + ":" + address.getPort();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PartitionId pid = (PartitionId) o;
		return Objects.equals(id, pid.id) && Objects.equals(crdtAddress, pid.crdtAddress) && Objects.equals(rpcAddress, pid.rpcAddress);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, crdtAddress, rpcAddress);
	}

	@Override
	public String toString() {
		return id + '|' + addressToString(crdtAddress) + '|' + addressToString(rpcAddress);
	}
}
