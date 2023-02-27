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

package io.activej.net;

import io.activej.reactor.NioReactive;
import io.activej.reactor.net.SocketSettings;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * This is an interface for a server that can be used in a {@link PrimaryServer}.
 * <p>
 * It should be reactive and should be able to accept and manage client connections.
 */
public interface WorkerServer extends NioReactive {
	void doAccept(SocketChannel socketChannel, InetSocketAddress localAddress, InetSocketAddress remoteAddress,
			boolean ssl, SocketSettings socketSettings);
}
