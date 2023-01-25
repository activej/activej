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

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.reactor.nio.NioReactor;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * It is a simple balancer server, which dispatches its connections to its {@link WorkerServer WorkerServers}.
 * <p>
 * When an incoming connection takes place, it forwards the request to one of them with a round-robin algorithm.
 */
public final class PrimaryServer extends AbstractReactiveServer {

	private final WorkerServer[] workerServers;

	private int currentAcceptor = -1; // first server index is currentAcceptor + 1

	private PrimaryServer(NioReactor primaryReactor, WorkerServer[] workerServers) {
		super(primaryReactor);
		this.workerServers = workerServers;
		for (WorkerServer workerServer : workerServers) {
			if (workerServer instanceof AbstractReactiveServer) {
				((AbstractReactiveServer) workerServer).acceptServer = this;
			}
		}
	}

	public static Builder builder(NioReactor primaryReactor, List<? extends WorkerServer> workerServers) {
		return builder(primaryReactor, workerServers.toArray(new WorkerServer[0]));
	}

	public static Builder builder(NioReactor primaryReactor, Iterable<? extends WorkerServer> workerServers) {
		List<WorkerServer> list = new ArrayList<>();
		workerServers.forEach(list::add);
		return builder(primaryReactor, list);
	}

	public static Builder builder(NioReactor primaryReactor, WorkerServer... workerServer) {
		return new PrimaryServer(primaryReactor, workerServer).new Builder();
	}

	public final class Builder extends AbstractReactiveServer.Builder<Builder, PrimaryServer> {
		private Builder() {}
	}

	@Override
	protected void serve(ITcpSocket socket, InetAddress remoteAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected WorkerServer getWorkerServer() {
		currentAcceptor = (currentAcceptor + 1) % workerServers.length;
		return workerServers[currentAcceptor];
	}

	@Override
	public String toString() {
		return "PrimaryServer{" +
				"numOfWorkerServers=" + workerServers.length +
				(listenAddresses.isEmpty() ? "" : ", listenAddresses=" + listenAddresses) +
				(sslListenAddresses.isEmpty() ? "" : ", sslListenAddresses=" + sslListenAddresses) +
				(acceptOnce ? ", acceptOnce" : "") +
				", workerServers=" + Arrays.toString(workerServers) +
				'}';
	}
}
