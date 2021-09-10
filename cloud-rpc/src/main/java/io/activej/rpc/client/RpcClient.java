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

import io.activej.async.callback.Callback;
import io.activej.async.exception.AsyncTimeoutException;
import io.activej.async.service.EventloopService;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.MemSize;
import io.activej.common.initializer.WithInitializer;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.eventloop.net.SocketSettings;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.net.socket.tcp.AsyncTcpSocketNio.JmxInspector;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.activej.rpc.client.jmx.RpcConnectStats;
import io.activej.rpc.client.jmx.RpcRequestStats;
import io.activej.rpc.client.sender.DiscoveryService;
import io.activej.rpc.client.sender.RpcSender;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.protocol.RpcException;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.protocol.RpcStream;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;

import static io.activej.async.callback.Callback.toAnotherEventloop;
import static io.activej.common.Utils.nonNullElseGet;
import static io.activej.net.socket.tcp.AsyncTcpSocketSsl.wrapClientSocket;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Sends requests to the specified servers according to defined
 * {@code RpcStrategy} strategy. Strategies, represented in
 * {@link RpcStrategies} satisfy most cases.
 * <p>
 * Example. Consider a client which sends a {@code Request} and receives a
 * {@code Response} from some {@link RpcServer}. To implement such kind of
 * client its necessary to proceed with following steps:
 * <ul>
 * <li>Create request-response classes for the client</li>
 * <li>Create a request handler for specified types</li>
 * <li>Create {@code RpcClient} and adjust it</li>
 * </ul>
 *
 * @see RpcStrategies
 * @see RpcServer
 */
public final class RpcClient implements IRpcClient, EventloopService, WithInitializer<RpcClient>, EventloopJmxBeanWithStats {
	private static final boolean CHECK = Checks.isEnabled(RpcClient.class);

	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();
	public static final Duration DEFAULT_CONNECT_TIMEOUT = ApplicationSettings.getDuration(RpcClient.class, "connectTimeout", Duration.ZERO);
	public static final Duration DEFAULT_RECONNECT_INTERVAL = ApplicationSettings.getDuration(RpcClient.class, "reconnectInterval", Duration.ZERO);
	public static final MemSize DEFAULT_PACKET_SIZE = ApplicationSettings.getMemSize(RpcClient.class, "packetSize", ChannelSerializer.DEFAULT_INITIAL_BUFFER_SIZE);

	private static final RpcException START_EXCEPTION = new RpcException("Could not establish initial connection");
	private static final RpcException NO_SENDER_AVAILABLE_EXCEPTION = new RpcException("No senders available");

	private Logger logger = getLogger(getClass());

	private final Eventloop eventloop;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	// SSL
	private SSLContext sslContext;
	private Executor sslExecutor;

	private RpcStrategy strategy = new NoServersStrategy();
	private List<InetSocketAddress> addresses = new ArrayList<>();
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();
	private Map<Object, InetSocketAddress> previouslyDiscovered;

	private MemSize defaultPacketSize = DEFAULT_PACKET_SIZE;
	private @Nullable FrameFormat frameFormat;
	private Duration autoFlushInterval = Duration.ZERO;
	private Duration keepAliveInterval = Duration.ZERO;

	private List<Class<?>> messageTypes;
	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private long reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL.toMillis();

	private boolean forcedStart;

	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	private SerializerBuilder serializerBuilder = SerializerBuilder.create(DefiningClassLoader.create(classLoader));
	private BinarySerializer<RpcMessage> serializer;

	private RpcSender requestSender = new NoSenderAvailable();

	private @Nullable SettablePromise<Void> stopPromise;

	private final RpcClientConnectionPool pool = connections::get;

	// jmx
	static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private boolean monitoring = false;
	private final RpcRequestStats generalRequestsStats = RpcRequestStats.create(SMOOTHING_WINDOW);
	private final Map<Class<?>, RpcRequestStats> requestStatsPerClass = new HashMap<>();
	private final Map<InetSocketAddress, RpcConnectStats> connectsStatsPerAddress = new HashMap<>();
	private final ExceptionStats lastProtocolError = ExceptionStats.create();

	private final JmxInspector statsSocket = new JmxInspector();

	// region builders
	private RpcClient(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public static RpcClient create(Eventloop eventloop) {
		return new RpcClient(eventloop);
	}

	public RpcClient withClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		this.serializerBuilder = SerializerBuilder.create(DefiningClassLoader.create(classLoader));
		return this;
	}

	/**
	 * Creates a client that uses provided socket settings.
	 *
	 * @param socketSettings settings for socket
	 * @return the RPC client with specified socket settings
	 */
	public RpcClient withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return this;
	}

	/**
	 * Creates a client with capability of specified message types processing.
	 *
	 * @param messageTypes classes of messages processed by a server
	 * @return client instance capable for handling provided message types
	 */
	public RpcClient withMessageTypes(Class<?>... messageTypes) {
		return withMessageTypes(Arrays.asList(messageTypes));
	}

	/**
	 * Creates a client with capability of specified message types processing.
	 *
	 * @param messageTypes classes of messages processed by a server
	 * @return client instance capable for handling provided
	 * message types
	 */
	public RpcClient withMessageTypes(List<Class<?>> messageTypes) {
		Checks.checkArgument(new HashSet<>(messageTypes).size() == messageTypes.size(), "Message types must be unique");
		this.messageTypes = messageTypes;
		return this;
	}

	/**
	 * Creates a client with serializer builder. A serializer builder is used
	 * for creating fast serializers at runtime.
	 *
	 * @param serializerBuilder serializer builder, used at runtime
	 * @return the RPC client with provided serializer builder
	 */
	public RpcClient withSerializerBuilder(SerializerBuilder serializerBuilder) {
		this.serializerBuilder = serializerBuilder;
		return this;
	}

	/**
	 * Creates a client with some strategy. Consider some ready-to-use
	 * strategies from {@link RpcStrategies}.
	 *
	 * @param requestSendingStrategy strategy of sending requests
	 * @return the RPC client, which sends requests according to given strategy
	 */
	public RpcClient withStrategy(RpcStrategy requestSendingStrategy) {
		this.strategy = requestSendingStrategy;
		return this;
	}

	public RpcClient withStreamProtocol(MemSize defaultPacketSize) {
		this.defaultPacketSize = defaultPacketSize;
		return this;
	}

	public RpcClient withStreamProtocol(MemSize defaultPacketSize, @Nullable FrameFormat frameFormat) {
		this.defaultPacketSize = defaultPacketSize;
		this.frameFormat = frameFormat;
		return this;
	}

	public RpcClient withAutoFlush(Duration autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		return this;
	}

	public RpcClient withKeepAlive(Duration keepAliveInterval) {
		this.keepAliveInterval = keepAliveInterval;
		return this;
	}

	/**
	 * Waits for a specified time before connecting.
	 *
	 * @param connectTimeout time before connecting
	 * @return the RPC client with connect timeout settings
	 */
	public RpcClient withConnectTimeout(Duration connectTimeout) {
		this.connectTimeoutMillis = connectTimeout.toMillis();
		return this;
	}

	public RpcClient withReconnectInterval(Duration reconnectInterval) {
		this.reconnectIntervalMillis = reconnectInterval.toMillis();
		return this;
	}

	public RpcClient withSslEnabled(SSLContext sslContext, Executor sslExecutor) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		return this;
	}

	public RpcClient withLogger(Logger logger) {
		this.logger = logger;
		return this;
	}

	/**
	 * Starts client in case of absence of connections
	 *
	 * @return the RPC client, which starts regardless of connection
	 * availability
	 */
	public RpcClient withForcedStart() {
		this.forcedStart = true;
		return this;
	}
	// endregion

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<Void> start() {
		if (CHECK) Checks.checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		Checks.checkNotNull(messageTypes, "Message types must be specified");

		Checks.checkState(stopPromise == null);

		serializer = serializerBuilder.withSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes).build(RpcMessage.class);

		return Promise.<Map<Object, InetSocketAddress>>ofCallback(cb -> strategy.getDiscoveryService().discover(null, cb))
				.map(result -> {
					this.previouslyDiscovered = result;
					Collection<InetSocketAddress> addresses = result.values();

					this.addresses = new ArrayList<>(addresses);

					for (InetSocketAddress address : addresses) {
						if (!connectsStatsPerAddress.containsKey(address)) {
							connectsStatsPerAddress.put(address, new RpcConnectStats(eventloop));
						}
					}

					return addresses;
				})
				.then(addresses -> Promises.all(
						addresses.stream()
								.map(address -> {
									logger.info("Connecting: {}", address);
									return connect(address)
											.then(($, e) -> Promise.complete());
								}))
						.then(() -> !forcedStart && requestSender instanceof NoSenderAvailable ?
								Promise.ofException(START_EXCEPTION) :
								Promise.complete()))
				.whenResult(this::rediscover);
	}

	@Override
	public @NotNull Promise<Void> stop() {
		if (CHECK) Checks.checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (stopPromise != null) return stopPromise;

		stopPromise = new SettablePromise<>();
		if (connections.size() == 0) {
			stopPromise.set(null);
			return stopPromise;
		}
		for (RpcClientConnection connection : connections.values()) {
			connection.shutdown();
		}
		return stopPromise;
	}

	private Promise<Void> connect(InetSocketAddress address) {
		return AsyncTcpSocketNio.connect(address, connectTimeoutMillis, socketSettings)
				.whenResult(asyncTcpSocketImpl -> {
					if (stopPromise != null || !addresses.contains(address)) {
						asyncTcpSocketImpl.close();
						return;
					}
					statsSocket.onConnect(asyncTcpSocketImpl);
					asyncTcpSocketImpl.setInspector(statsSocket);
					AsyncTcpSocket socket = sslContext == null ?
							asyncTcpSocketImpl :
							wrapClientSocket(asyncTcpSocketImpl, sslContext, sslExecutor);
					RpcStream stream = new RpcStream(socket, serializer, defaultPacketSize,
							autoFlushInterval, frameFormat, false); // , statsSerializer, statsDeserializer, statsCompressor, statsDecompressor);
					RpcClientConnection connection = new RpcClientConnection(eventloop, this, address, stream, keepAliveInterval.toMillis());
					stream.setListener(connection);

					// jmx
					if (isMonitoring()) {
						connection.startMonitoring();
					}
					connections.put(address, connection);
					requestSender = nonNullElseGet(strategy.createSender(pool), NoSenderAvailable::new);

					// jmx
					connectsStatsPerAddress.get(address).recordSuccessfulConnect();

					logger.info("Connection to {} established", address);
				})
				.whenException(e -> {
					logger.warn("Connection {} failed: {}", address, e);
					if (stopPromise == null) {
						processClosedConnection(address);
					}
				})
				.toVoid();
	}

	void removeConnection(InetSocketAddress address) {
		if (connections.remove(address) == null) return;
		requestSender = nonNullElseGet(strategy.createSender(pool), NoSenderAvailable::new);
		logger.info("Connection closed: {}", address);
		processClosedConnection(address);
	}

	private void processClosedConnection(InetSocketAddress address) {
		if (stopPromise == null) {
			if (!addresses.contains(address)) return;
			//jmx
			connectsStatsPerAddress.get(address).recordFailedConnect();
			eventloop.delayBackground(reconnectIntervalMillis, () -> {
				if (stopPromise == null) {
					logger.info("Reconnecting: {}", address);
					connect(address);
				}
			});
		} else {
			if (connections.size() == 0) {
				stopPromise.set(null);
			}
		}
	}

	/**
	 * Sends the request to server, waits the result timeout and handles result with callback
	 *
	 * @param <I>     request class
	 * @param <O>     response class
	 * @param request request to a server
	 * @param timeout timeout in milliseconds. If there is no answer from the server after the timeout passes,
	 *                the request will end exceptionally with {@link AsyncTimeoutException}.
	 *                Note, that setting a timeout schedules a task to the {@link Eventloop}, so a lot of
	 *                requests with large timeouts may degrade performance. In the most common scenarios
	 *                timeouts for RPC requests should not be very big (a few seconds should be enough)
	 * @param cb      a callback that will be completed after receiving a response or encountering some error
	 */
	@Override
	public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
		if (CHECK) Checks.checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (timeout > 0) {
			requestSender.sendRequest(request, timeout, cb);
		} else {
			cb.accept(null, new AsyncTimeoutException("RPC request has timed out"));
		}
	}

	@Override
	public <I, O> void sendRequest(I request, Callback<O> cb) {
		if (CHECK) Checks.checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		requestSender.sendRequest(request, cb);
	}

	private void rediscover() {
		if (stopPromise != null) return;

		strategy.getDiscoveryService().discover(previouslyDiscovered, (result, e) -> {
			if (stopPromise != null) return;

			if (e == null) {
				updateAddresses(result);
				rediscover();
			} else {
				logger.warn("Could not discover addresses", e);
				eventloop.delayBackground(Duration.ofSeconds(1), this::rediscover);
			}
		});
	}

	private void updateAddresses(Map<Object, InetSocketAddress> newAddresses) {
		this.previouslyDiscovered = newAddresses;
		List<InetSocketAddress> previousAddresses = this.addresses;
		this.addresses = new ArrayList<>(newAddresses.values());

		boolean changed = false;
		for (InetSocketAddress address : previousAddresses) {
			if (!this.addresses.contains(address)) {
				connections.remove(address).shutdown();
				connectsStatsPerAddress.remove(address);
				changed = true;
			}
		}
		if (changed) {
			requestSender = nonNullElseGet(strategy.createSender(pool), NoSenderAvailable::new);
		}
		for (InetSocketAddress address : this.addresses) {
			if (!previousAddresses.contains(address)) {
				connectsStatsPerAddress.put(address, new RpcConnectStats(eventloop));
				connect(address);
			}
		}
	}

	public IRpcClient adaptToAnotherEventloop(Eventloop anotherEventloop) {
		if (anotherEventloop == this.eventloop) {
			return this;
		}

		return new IRpcClient() {
			@Override
			public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
				if (CHECK) Checks.checkState(anotherEventloop.inEventloopThread(), "Not in eventloop thread");
				if (timeout > 0) {
					eventloop.execute(() -> requestSender.sendRequest(request, timeout, toAnotherEventloop(anotherEventloop, cb)));
				} else {
					cb.accept(null, new AsyncTimeoutException("RPC request has timed out"));
				}
			}

		};
	}

	@VisibleForTesting
	public RpcSender getRequestSender() {
		return requestSender;
	}

	@Override
	public String toString() {
		return "RpcClient{" + connections + '}';
	}

	private static final class NoSenderAvailable implements RpcSender {
		@Override
		public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
			cb.accept(null, NO_SENDER_AVAILABLE_EXCEPTION);
		}
	}

	private static final class NoServersStrategy implements RpcStrategy {
		@Override
		public DiscoveryService getDiscoveryService() {
			return DiscoveryService.constant(emptyMap());
		}

		@Override
		public RpcSender createSender(RpcClientConnectionPool pool) {
			return null;
		}
	}

	// jmx
	@JmxOperation(description = "enable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled) ]")
	public void startMonitoring() {
		monitoring = true;
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.startMonitoring();
			}
		}
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled) ]")
	public void stopMonitoring() {
		monitoring = false;
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = connections.get(address);
			if (connection != null) {
				connection.stopMonitoring();
			}
		}
	}

	@JmxAttribute(description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, responseTime and requestsStatsPerClass are collected only when monitoring is enabled)")
	public boolean isMonitoring() {
		return monitoring;
	}

	@JmxAttribute(name = "requests", extraSubAttributes = "totalRequests")
	public RpcRequestStats getGeneralRequestsStats() {
		return generalRequestsStats;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getTotalSuccessfulConnects() {
		return connectsStatsPerAddress.values().stream()
				.mapToLong(RpcConnectStats::getSuccessfulConnects)
				.sum();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public long getTotalFailedConnects() {
		return connectsStatsPerAddress.values().stream()
				.mapToLong(RpcConnectStats::getFailedConnects)
				.sum();
	}

	@JmxAttribute(description = "request stats distributed by request class")
	public Map<Class<?>, RpcRequestStats> getRequestsStatsPerClass() {
		return requestStatsPerClass;
	}

	@JmxAttribute
	public Map<InetSocketAddress, RpcConnectStats> getConnectsStatsPerAddress() {
		return connectsStatsPerAddress;
	}

	@JmxAttribute(description = "request stats for current connections (when connection is closed stats are removed)")
	public Map<InetSocketAddress, RpcClientConnection> getRequestStatsPerConnection() {
		return connections;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getActiveConnections() {
		return connections.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getActiveRequests() {
		int count = 0;
		for (RpcClientConnection connection : connections.values()) {
			count += connection.getActiveRequests();
		}
		return count;
	}

	@JmxAttribute(description = "exception that occurred because of protocol error " +
			"(serialization, deserialization, compression, decompression, etc)")
	public ExceptionStats getLastProtocolError() {
		return lastProtocolError;
	}

	@JmxAttribute
	public JmxInspector getStatsSocket() {
		return statsSocket;
	}

	@JmxAttribute
	public List<String> getUnresponsiveServers() {
		if (stopPromise != null) return Collections.emptyList();

		return connectsStatsPerAddress.entrySet().stream()
				.filter(entry -> !entry.getValue().isConnected())
				.map(entry -> entry.getKey().toString())
				.collect(toList());
	}

	RpcRequestStats ensureRequestStatsPerClass(Class<?> requestClass) {
		if (!requestStatsPerClass.containsKey(requestClass)) {
			requestStatsPerClass.put(requestClass, RpcRequestStats.create(SMOOTHING_WINDOW));
		}
		return requestStatsPerClass.get(requestClass);
	}
}
