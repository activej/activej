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

package io.activej.rpc.client;

import io.activej.rpc.client.sender.RpcSender;

import java.net.InetSocketAddress;

public interface RpcClientConnectionPool {

	/**
	 * Returns connection for specified address or null if there is no such connection
	 *
	 * @param address address
	 * @return connection for specified address or null if there is no such connection
	 */
	RpcSender get(InetSocketAddress address);
}
