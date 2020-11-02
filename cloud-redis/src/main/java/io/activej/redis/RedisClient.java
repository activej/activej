package io.activej.redis;

import io.activej.async.process.AsyncCloseable;
import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.ApplicationSettings;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.CloseException;
import io.activej.csp.net.MessagingWithBinaryStreaming;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.SocketSettings;
import io.activej.eventloop.schedule.ScheduledRunnable;
import io.activej.net.connection.ConnectionPool;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class RedisClient implements EventloopService, ConnectionPool {
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
		return Promises.all(connections.stream().map(connection -> connection.close(CLOSE_EXCEPTION).toTry()));
	}

	public Promise<RedisConnection> getConnection() {
		return Promise.complete()
				.then(() -> {
					while (true) {
						Map.Entry<IdleKey, RedisConnection> idleEntry = idlePool.pollFirstEntry();
						if (idleEntry != null) {
							idleEntry.getKey().evictRunnable.cancel();
							RedisConnection idleConnection = idleEntry.getValue();
							if (idleConnection.isClosed()) continue;
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
			idlePool.put(new IdleKey(), connection);
		} else {
			connection.close();
		}
	}

	void onConnectionClose(RedisConnection connection) {
		active.remove(connection);
	}

	private Promise<MessagingWithBinaryStreaming<RedisResponse, RedisCommand>> connect() {
		return AsyncTcpSocketNio.connect(address, connectTimeoutMillis, null)
				.map(socket -> {
					ByteBufQueue tempQueue = new ByteBufQueue();
					MessagingWithBinaryStreaming<RedisResponse, RedisCommand> messaging =
							MessagingWithBinaryStreaming.create(socket, new RESPv2Codec(tempQueue, charset));
					messaging.setCloseable($ -> tempQueue.recycle());
					return messaging;
				});
	}

	private class IdleKey implements Comparable<IdleKey> {
		private final ScheduledRunnable evictRunnable;
		private final long evictTimestamp;

		private IdleKey() {
			evictTimestamp = eventloop.currentTimeMillis() + poolTTLMillis;
			evictRunnable = eventloop.scheduleBackground(evictTimestamp, () -> {
				RedisConnection connection = idlePool.remove(this);
				if (connection != null) connection.close(CONNECTION_TIMED_OUT);
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
