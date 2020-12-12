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

import io.activej.async.process.AsyncCloseable;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.CloseException;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.SocketSettings;
import io.activej.eventloop.schedule.ScheduledRunnable;
import io.activej.net.connection.ConnectionPool;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.net.socket.tcp.AsyncTcpSocketSsl.wrapClientSocket;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RedisClient implements EventloopService, ConnectionPool {
	private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

	public static final CloseException CLOSE_EXCEPTION = new CloseException(RedisClient.class, "Closed");
	public static final AsyncTimeoutException CONNECTION_TIMED_OUT = new AsyncTimeoutException(RedisClient.class, "Idle connection has timed out");

	public static final InetSocketAddress DEFAULT_ADDRESS = ApplicationSettings.getInetSocketAddress(
			RedisClient.class, "address", new InetSocketAddress("localhost", 6379)
	);
	public static final Charset DEFAULT_CHARSET = ApplicationSettings.getCharset(RedisClient.class, "charset", UTF_8);
	public static final Duration DEFAULT_CONNECT_TIMEOUT = ApplicationSettings.getDuration(RedisClient.class, "connectTimeout", Duration.ZERO);
	public static final Duration DEFAULT_POOL_TTL = ApplicationSettings.getDuration(RedisClient.class, "poolTTL", Duration.ZERO);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();

	private final Eventloop eventloop;
	private final InetSocketAddress address;

	private final NavigableMap<IdleKey, RedisConnection> idlePool = new TreeMap<>();
	private final Set<RedisConnection> active = new HashSet<>();

	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;
	private Charset charset = DEFAULT_CHARSET;
	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private long poolTTLMillis = DEFAULT_POOL_TTL.toMillis();

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

	public RedisClient withCharset(Charset charset) {
		this.charset = charset;
		return this;
	}

	public RedisClient withConnectTimeout(Duration connectTimeout) {
		this.connectTimeoutMillis = connectTimeout.toMillis();
		return this;
	}

	public RedisClient withPoolTTL(Duration poolTTL) {
		this.poolTTLMillis = poolTTL.toMillis();
		return this;
	}

	public RedisClient withSslEnabled(@NotNull SSLContext sslContext, @NotNull Executor sslExecutor) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		return this;
	}
	// endregion

	// region getters
	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public Charset getCharset() {
		return charset;
	}

	public Duration getConnectTimeout() {
		return Duration.ofMillis(connectTimeoutMillis);
	}

	public Duration getPoolTTL() {
		return Duration.ofSeconds(poolTTLMillis);
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	public int getActiveConnections() {
		return active.size();
	}

	public int getIdleConnections() {
		return idlePool.size();
	}

	public boolean isUsingPool() {
		return poolTTLMillis > 0;
	}
	// endregion

	@Override
	public @NotNull Promise<?> start() {
		return connect().whenResult(AsyncCloseable::close);
	}

	@Override
	public @NotNull Promise<?> stop() {
		Set<RedisConnection> connections = new HashSet<>(active);
		connections.addAll(idlePool.values());
		active.clear();
		idlePool.clear();
		return Promises.all(connections.stream().map(connection -> connection.closeEx(CLOSE_EXCEPTION).toTry()))
				.whenComplete(toLogger(logger, "stop", this));
	}

	public Promise<RedisConnection> getConnection() {
		logger.trace("Connection has been requested");
		return Promise.complete()
				.then(() -> {
					while (true) {
						Map.Entry<IdleKey, RedisConnection> idleEntry = idlePool.pollFirstEntry();
						if (idleEntry != null) {
							idleEntry.getKey().evictRunnable.cancel();
							RedisConnection idleConnection = idleEntry.getValue();
							if (idleConnection.isClosed()) continue;
							logger.trace("Returning connection from pool");
							idleConnection.inPool = false;
							return Promise.of(idleConnection);
						}
						break;
					}
					return connect()
							.map(messaging -> new RedisConnection(this, messaging, charset));
				})
				.whenResult(active::add);
	}

	void returnConnection(RedisConnection connection) {
		if (isUsingPool()) {
			connection.inPool = true;
			if (connection.isClosed()) return;
			logger.trace("Connection is returned to pool");
			idlePool.put(new IdleKey(), connection);
		} else {
			connection.close();
		}
	}

	void onConnectionClose(RedisConnection connection) {
		active.remove(connection);
	}

	private Promise<RedisMessaging> connect() {
		return AsyncTcpSocketNio.connect(address, connectTimeoutMillis, null)
				.map((AsyncTcpSocket socket) -> {
					socket = sslContext != null ?
							wrapClientSocket(socket,
									address.getHostName(), address.getPort(),
									sslContext, sslExecutor) :
							socket;

					ByteBufQueue tempQueue = new ByteBufQueue();
					RedisMessaging messaging =
							RedisMessaging.create(socket, new RESPv2(tempQueue, charset));
					messaging.setCloseable($ -> tempQueue.recycle());
					return messaging;
				})
				.whenComplete(toLogger(logger, TRACE, "connect", this));
	}

	@Override
	public String toString() {
		return "RedisClient{" +
				"address=" + address +
				", idlePoolSize=" + idlePool.size() +
				", activeConnections=" + active.size() +
				", secure=" + (sslContext != null) +
				'}';
	}

	private class IdleKey implements Comparable<IdleKey> {
		private final ScheduledRunnable evictRunnable;
		private final long evictTimestamp;

		private IdleKey() {
			evictTimestamp = eventloop.currentTimeMillis() + poolTTLMillis;
			evictRunnable = eventloop.scheduleBackground(evictTimestamp, () -> {
				RedisConnection connection = idlePool.remove(this);
				if (connection != null) connection.closeEx(CONNECTION_TIMED_OUT);
			});
		}

		@Override
		public int compareTo(@NotNull RedisClient.IdleKey o) {
			int tsCompare = Long.compare(evictTimestamp, o.evictTimestamp);
			if (tsCompare != 0) return tsCompare;

			return Long.compare(System.identityHashCode(this), System.identityHashCode(o));
		}
	}
}
