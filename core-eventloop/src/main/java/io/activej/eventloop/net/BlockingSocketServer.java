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

package io.activej.eventloop.net;

import io.activej.common.initializer.WithInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

public final class BlockingSocketServer implements WithInitializer<BlockingSocketServer> {
	public interface AcceptHandler {
		void onAccept(Socket socket) throws IOException;
	}

	private static final Logger logger = LoggerFactory.getLogger(BlockingSocketServer.class);
	private ThreadFactory acceptThreadFactory;
	private final Executor executor;
	private final AcceptHandler acceptHandler;
	private ServerSocketSettings serverSocketSettings;
	private SocketSettings socketSettings;
	private final List<InetSocketAddress> listenAddresses = new ArrayList<>();

	private final List<ServerSocket> serverSockets = new ArrayList<>();
	private final Map<ServerSocket, Thread> acceptThreads = new HashMap<>();

	private BlockingSocketServer(Executor executor, AcceptHandler acceptHandler) {
		this.executor = executor;
		this.acceptHandler = acceptHandler;
	}

	public static BlockingSocketServer create(Executor executor, AcceptHandler acceptHandler) {
		return new BlockingSocketServer(executor, acceptHandler);
	}

	public BlockingSocketServer withAcceptThreadFactory(ThreadFactory acceptThreadFactory) {
		this.acceptThreadFactory = acceptThreadFactory;
		return this;
	}

	public BlockingSocketServer withListenAddresses(List<InetSocketAddress> listenAddresses) {
		this.listenAddresses.addAll(listenAddresses);
		return this;
	}

	public BlockingSocketServer withListenAddresses(InetSocketAddress... listenAddresses) {
		return withListenAddresses(List.of(listenAddresses));
	}

	public BlockingSocketServer withListenAddress(InetSocketAddress listenAddress) {
		this.listenAddresses.add(listenAddress);
		return this;
	}

	public BlockingSocketServer withListenPort(int port) {
		return withListenAddress(new InetSocketAddress(port));
	}

	public BlockingSocketServer withServerSocketSettings(ServerSocketSettings socketSettings) {
		this.serverSocketSettings = socketSettings;
		return this;
	}

	public BlockingSocketServer withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}

	private void serveClient(Socket socket) throws IOException {
		socketSettings.applySettings(socket.getChannel());
		executor.execute(() -> {
			try {
				acceptHandler.onAccept(socket);
			} catch (Exception e) {
				logger.error("Failed to serve socket " + socket, e);
			}
		});
	}

	public void start() throws Exception {
		for (InetSocketAddress address : listenAddresses) {
			ServerSocket serverSocket = new ServerSocket(address.getPort(), serverSocketSettings.getBacklog(), address.getAddress());
			serverSocketSettings.applySettings(serverSocket.getChannel());
			serverSockets.add(serverSocket);
			Runnable runnable = () -> {
				while (!Thread.interrupted()) {
					try {
						serveClient(serverSocket.accept());
					} catch (Exception e) {
						if (Thread.currentThread().isInterrupted())
							break;
						logger.error("Socket error for " + serverSocket, e);
					}
				}
			};
			Thread acceptThread = acceptThreadFactory == null ?
					new Thread(runnable) :
					acceptThreadFactory.newThread(runnable);
			acceptThread.setDaemon(true);
			acceptThreads.put(serverSocket, acceptThread);
			acceptThread.start();
		}
	}

	public void stop() throws Exception {
		for (ServerSocket serverSocket : serverSockets) {
			Thread acceptThread = acceptThreads.get(serverSocket);
			acceptThread.interrupt();
			serverSocket.close();
		}
		for (Thread acceptThread : acceptThreads.values()) {
			acceptThread.join();
		}
		serverSockets.clear();
	}
}
