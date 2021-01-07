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

package io.activej.dataflow.graph;

import java.net.InetSocketAddress;

/**
 * Defines a remote partition, which is represented by a server address.
 */
public final class Partition {
	private final InetSocketAddress address;

	/**
	 * Constructs a new remote partition with the given server address.
	 *
	 * @param address server address
	 */
	public Partition(InetSocketAddress address) {
		this.address = address;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public String toString() {
		return "RemotePartition{address=" + address + '}';
	}
}
