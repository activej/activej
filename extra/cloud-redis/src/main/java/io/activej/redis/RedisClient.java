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

package io.activej.redis;

import io.activej.common.ApplicationSettings;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.SocketSettings;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executor;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.net.socket.tcp.AsyncTcpSocketSsl.wrapClientSocket;

public final class RedisClient {
	private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

	public static final InetSocketAddress DEFAULT_ADDRESS = ApplicationSettings.getInetSocketAddress(
			RedisClient.class, "address", new InetSocketAddress("localhost", 6379)
	);
	public static final Duration DEFAULT_CONNECT_TIMEOUT = ApplicationSettings.getDuration(RedisClient.class, "connectTimeout", Duration.ZERO);
	public static final Duration DEFAULT_POOL_TTL = ApplicationSettings.getDuration(RedisClient.class, "poolTTL", Duration.ZERO);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();

	private final Eventloop eventloop;
	private final InetSocketAddress address;

	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();

	@Nullable
	private SSLContext sslContext;
	@Nullable
	private Executor sslExecutor;

	// region creators
	private RedisClient(Eventloop eventloop, InetSocketAddress address) {
		this.eventloop = eventloop;
		this.address = address;
	}

	public static RedisClient create(Eventloop eventloop) {
		return new RedisClient(eventloop, DEFAULT_ADDRESS);
	}

	public static RedisClient create(Eventloop eventloop, InetSocketAddress address) {
		return new RedisClient(eventloop, address);
	}

	public RedisClient withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}

	public RedisClient withConnectTimeout(Duration connectTimeout) {
		this.connectTimeoutMillis = connectTimeout.toMillis();
		return this;
	}

	public RedisClient withSslEnabled(@NotNull SSLContext sslContext, @NotNull Executor sslExecutor) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		return this;
	}
	// endregion

	// region getters
	public Eventloop getEventloop() {
		return eventloop;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public Duration getConnectTimeout() {
		return Duration.ofMillis(connectTimeoutMillis);
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}
	// endregion

	public Promise<RedisConnection> connect() {
		return AsyncTcpSocketNio.connect(address, connectTimeoutMillis, socketSettings)
				.map((AsyncTcpSocket socket) -> {
					socket = sslContext != null ?
							wrapClientSocket(socket,
									address.getHostName(), address.getPort(),
									sslContext, sslExecutor) :
							socket;
					RedisConnection connection = new RedisConnection(eventloop, this, socket);
					connection.start();
					return connection;
				})
//				.thenEx(wrapException(() -> "Failed to connect to Redis server"))
				.whenComplete(toLogger(logger, TRACE, "connect", this));
	}

	@Override
	public String toString() {
		return "RedisClient{" +
				"address=" + address +
				", secure=" + (sslContext != null) +
				'}';
	}

}
