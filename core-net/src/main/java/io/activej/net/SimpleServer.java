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

import io.activej.net.socket.tcp.ReactiveTcpSocket;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;

import java.net.InetAddress;
import java.util.function.Consumer;

/**
 * This is a basic implementation of the {@link AbstractReactiveServer} which just dispatches
 * each {@link ReactiveTcpSocket acync client connection} to a given consumer.
 */
public final class SimpleServer extends AbstractReactiveServer<SimpleServer> {
	private final Consumer<ReactiveTcpSocket> socketConsumer;

	private SimpleServer(NioReactor reactor, Consumer<ReactiveTcpSocket> socketConsumer) {
		super(reactor);
		this.socketConsumer = socketConsumer;
	}

	public static SimpleServer create(NioReactor reactor, Consumer<ReactiveTcpSocket> socketConsumer) {
		return new SimpleServer(reactor, socketConsumer);
	}

	public static SimpleServer create(Consumer<ReactiveTcpSocket> socketConsumer) {
		return new SimpleServer(Reactor.getCurrentReactor(), socketConsumer);
	}

	@Override
	protected void serve(ReactiveTcpSocket socket, InetAddress remoteAddress) {
		socketConsumer.accept(socket);
	}
}
