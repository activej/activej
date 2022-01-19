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

import io.activej.common.exception.MalformedDataException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public final class SimplePartitionId {
	private final String id;
	private final InetAddress host;
	private final int crdtPort;
	private final int rpcPort;

	public SimplePartitionId(String id, InetAddress host, int crdtPort, int rpcPort) {
		this.id = id;
		this.host = host;
		this.crdtPort = crdtPort;
		this.rpcPort = rpcPort;
	}

	public static SimplePartitionId parseString(String string) throws MalformedDataException {
		String[] split = string.split(":");
		int length = split.length;
		if (length < 4) {
			throw new MalformedDataException("Partitions ID is expected to contain 4 components");
		}

		int rpcPort = parsePort(split[length - 1]);
		int crdtPort = parsePort(split[length - 2]);

		InetAddress host;
		try {
			host = InetAddress.getByName(split[length - 3]);
		} catch (UnknownHostException e) {
			throw new MalformedDataException("Could not parse host value", e);
		}

		String id;
		if (length == 4) {
			id = split[0];
		} else {
			id = "";
			for (int i = 0; i < length - 3; i++) {
				//noinspection StringConcatenationInLoop - rare case
				id += split[i];
			}
		}

		return new SimplePartitionId(id, host, crdtPort, rpcPort);
	}

	public String asString() {
		return id + ':' + host.getHostAddress() + ':' + crdtPort + ':' + rpcPort;
	}

	public String getId() {
		return id;
	}

	public InetAddress getHost() {
		return host;
	}

	public int getCrdtPort() {
		return crdtPort;
	}

	public int getRpcPort() {
		return rpcPort;
	}

	public InetSocketAddress getCrdtAddress() {
		return new InetSocketAddress(host, crdtPort);
	}

	public InetSocketAddress getRpcAddress() {
		return new InetSocketAddress(host, rpcPort);
	}

	private static int parsePort(String portString) throws MalformedDataException {
		try {
			int port = Integer.parseInt(portString);
			if (port <= 0 || port >= 65536) {
				throw new MalformedDataException("Invalid port value. Port is not in range (0, 65536) " + port);
			}
			return port;
		} catch (NumberFormatException e) {
			throw new MalformedDataException("Could not parse port value", e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		SimplePartitionId that = (SimplePartitionId) o;
		return id.equals(that.id);
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String toString() {
		return '[' + id + ':' + host + ':' + crdtPort + ':' + rpcPort + ']';
	}
}
