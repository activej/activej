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
import io.activej.common.initializer.AbstractBuilder;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractNioReactive;
import io.activej.reactor.net.SocketSettings;
import io.activej.reactor.nio.NioReactor;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executor;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.net.socket.tcp.TcpSocket_Ssl.wrapClientSocket;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * A client for Redis.
 * Allows connecting to Redis server, supports SSL.
 */
public final class RedisClient extends AbstractNioReactive {
	private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

	public static final InetSocketAddress DEFAULT_ADDRESS = ApplicationSettings.getInetSocketAddress(
			RedisClient.class, "address", new InetSocketAddress("localhost", 6379)
	);
	public static final Duration DEFAULT_CONNECT_TIMEOUT = ApplicationSettings.getDuration(RedisClient.class, "connectTimeout", Duration.ZERO);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();

	private final InetSocketAddress address;

	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private Duration autoFlushInterval = Duration.ZERO;

	private @Nullable SSLContext sslContext;
	private @Nullable Executor sslExecutor;

	// region creators
	private RedisClient(NioReactor reactor, InetSocketAddress address) {
		super(reactor);
		this.address = address;
	}

	public static RedisClient create(NioReactor reactor) {
		return builder(reactor).build();
	}

	public static RedisClient create(NioReactor reactor, InetSocketAddress address) {
		return builder(reactor, address).build();
	}

	public static Builder builder(NioReactor reactor) {
		return builder(reactor, DEFAULT_ADDRESS);
	}

	public static Builder builder(NioReactor reactor, InetSocketAddress address) {
		return new RedisClient(reactor, address).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RedisClient>{
		private Builder() {}

		public Builder withSocketSettings(SocketSettings socketSettings) {
			checkNotBuilt(this);
			RedisClient.this.socketSettings = socketSettings;
			return this;
		}

		public Builder withConnectTimeout(Duration connectTimeout) {
			checkNotBuilt(this);
			RedisClient.this.connectTimeoutMillis = connectTimeout.toMillis();
			return this;
		}

		public Builder withSslEnabled(SSLContext sslContext, Executor sslExecutor) {
			checkNotBuilt(this);
			RedisClient.this.sslContext = sslContext;
			RedisClient.this.sslExecutor = sslExecutor;
			return this;
		}

		public Builder withAutoFlushInterval(Duration autoFlushInterval) {
			checkNotBuilt(this);
			RedisClient.this.autoFlushInterval = autoFlushInterval;
			return this;
		}

		@Override
		protected RedisClient doBuild() {
			return RedisClient.this;
		}
	}
	// endregion

	// region getters
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

	/**
	 * Creates a connection to Redis server.
	 *
	 * @return promise of {@link RedisConnection}
	 */
	public Promise<RedisConnection> connect() {
		checkInReactorThread(this);
		return TcpSocket.connect(reactor, address, connectTimeoutMillis, socketSettings)
				.map(
						socket -> {
							RedisConnection connection = new RedisConnection(this,
									sslContext != null ?
											wrapClientSocket(reactor, socket,
													address.getHostName(), address.getPort(),
													sslContext, sslExecutor) :
											socket,
									autoFlushInterval);
							connection.start();
							return connection;
						},
						e -> {
							throw new RedisException("Failed to connect to Redis server: " + address, e);
						})
				.whenComplete(toLogger(logger, TRACE, "connect", this));
	}

	/**
	 * Creates a connection to Redis server and performs authentication using password.
	 * <p>
	 * If ACL is used on the server, the username is {@code default}
	 *
	 * @return promise of {@link RedisConnection}
	 * @see <a href="https://redis.io/commands/auth">AUTH</a>
	 * @see <a href="https://redis.io/topics/acl">ACL</a>
	 */
	public Promise<RedisConnection> connect(byte[] password) {
		checkInReactorThread(this);
		return connectAndAuth("AUTH", password);
	}

	/**
	 * @see #connect(byte[])
	 */
	public Promise<RedisConnection> connect(String password) {
		checkInReactorThread(this);
		return connectAndAuth("AUTH", password);
	}

	/**
	 * Creates a connection to Redis server and performs ACL authentication using username and password.
	 *
	 * @return promise of {@link RedisConnection}
	 * @see <a href="https://redis.io/commands/auth">AUTH</a>
	 * @see <a href="https://redis.io/topics/acl">ACL</a>
	 */
	public Promise<RedisConnection> connect(byte[] username, byte[] password) {
		checkInReactorThread(this);
		return connectAndAuth("AUTH", username, password);
	}

	/**
	 * @see #connect(byte[], byte[])
	 */
	public Promise<RedisConnection> connect(String username, String password) {
		checkInReactorThread(this);
		return connectAndAuth("AUTH", username, password);
	}

	private Promise<RedisConnection> connectAndAuth(Object... args) {
		checkInReactorThread(this);
		return connect()
				.then(connection ->
						connection.cmd(RedisRequest.of(args), RedisResponse.OK)
								.map($ -> connection)
								.whenException(connection::close));
	}

	@Override
	public String toString() {
		return "RedisClient{" +
				"address=" + address +
				", secure=" + (sslContext != null) +
				'}';
	}

}
