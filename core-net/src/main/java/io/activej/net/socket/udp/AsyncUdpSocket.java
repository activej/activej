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

package io.activej.net.socket.udp;

import io.activej.promise.Promise;

/**
 * Common interface for datagram-oriented transport protocols.
 * <p>
 * This interface describes asynchronous receive and send operations for data transmission through the network.
 * <p>
 * Implementations of this interface should follow rules described below:
 * <ul>
 * <li>Each request to the socket after it was closed should complete exceptionally.
 * </ul>
 */
public interface AsyncUdpSocket {
	Promise<UdpPacket> receive();

	Promise<Void> send(UdpPacket packet);

	void close();
}
