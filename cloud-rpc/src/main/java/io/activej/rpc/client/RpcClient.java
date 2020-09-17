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
import io.activej.async.service.EventloopService;
import io.activej.common.Checks;
import io.activej.common.MemSize;
import io.activej.common.api.WithInitializer;
import io.activej.common.exception.StacklessException;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
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
import io.activej.rpc.client.sender.RpcSender;
import io.activej.rpc.client.sender.RpcStrategies;
import io.activej.rpc.client.sender.RpcStrategy;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.protocol.RpcStream;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;

import static io.activej.async.callback.Callback.toAnotherEventloop;
import static io.activej.common.Utils.nullToSupplier;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;
import static io.activej.net.socket.tcp.AsyncTcpSocketSsl.wrapClientSocket;
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
public final class RpcClient implements IRpcClient, EventloopService, WithInitializer<RpcClient>, EventloopJmxBeanEx {
	private static final boolean CHECK = Checks.isEnabled(RpcClient.class);

	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();
	public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
	public static final Duration DEFAULT_RECONNECT_INTERVAL = Duration.ofSeconds(1);
	public static final MemSize DEFAULT_PACKET_SIZE = ChannelSerializer.DEFAULT_INITIAL_BUFFER_SIZE;
	public static final MemSize MAX_PACKET_SIZE = ChannelSerializer.MAX_SIZE;
	public static final StacklessException START_EXCEPTION = new StacklessException("Could not establish initial connection");

	private Logger logger = getLogger(getClass());

	private final Eventloop eventloop;
	private SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	// SSL
	private SSLContext sslContext;
	private Executor sslExecutor;

	private RpcStrategy strategy = new NoServersStrategy();
	private List<InetSocketAddress> addresses = new ArrayList<>();
	private final Map<InetSocketAddress, RpcClientConnection> connections = new HashMap<>();

	private MemSize defaultPacketSize = DEFAULT_PACKET_SIZE;
	private MemSize maxPacketSize = MAX_PACKET_SIZE;
	private boolean compression = false;
	private Duration autoFlushInterval = Duration.ZERO;
	private Duration keepAliveInterval = Duration.ZERO;

	private List<Class<?>> messageTypes;
	private long connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT.toMillis();
	private long reconnectIntervalMillis = DEFAULT_RECONNECT_INTERVAL.toMillis();

	private boolean forcedStart;

	private ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	private SerializerBuilder serializerBuilder = SerializerBuilder.create(classLoader);
	private BinarySerializer<RpcMessage> serializer;

	private RpcSender requestSender = new NoSenderAvailable();

	@Nullable
	private SettablePromise<Void> stopPromise;

	private final RpcClientConnectionPool pool = connections::get;

	// jmx
	static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private boolean monitoring = false;
	private final RpcRequestStats generalRequestsStats = RpcRequestStats.create(SMOOTHING_WINDOW);
	private final RpcConnectStats generalConnectsStats = new RpcConnectStats();
	private final Map<Class<?>, RpcRequestStats> requestStatsPerClass = new HashMap<>();
	private final Map<InetSocketAddress, RpcConnectStats> connectsStatsPerAddress = new HashMap<>();
	private final ExceptionStats lastProtocolError = ExceptionStats.create();

	private final JmxInspector statsSocket = new JmxInspector();
	//	private final StreamBinarySerializer.JmxInspector statsSerializer = new StreamBinarySerializer.JmxInspector();
	//	private final StreamBinaryDeserializer.JmxInspector statsDeserializer = new StreamBinaryDeserializer.JmxInspector();
	//	private final StreamLZ4Compressor.JmxInspector statsCompressor = new StreamLZ4Compressor.JmxInspector();
	//	private final StreamLZ4Decompressor.JmxInspector statsDecompressor = new StreamLZ4Decompressor.JmxInspector();

	// region builders
	private RpcClient(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public static RpcClient create(Eventloop eventloop) {
		return new RpcClient(eventloop);
	}

	public RpcClient withClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		this.serializerBuilder = SerializerBuilder.create(classLoader);
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
	public RpcClient withMessageTypes(@NotNull Class<?>... messageTypes) {
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
		this.addresses = new ArrayList<>(strategy.getAddresses());

		// jmx
		for (InetSocketAddress address : this.addresses) {
			if (!connectsStatsPerAddress.containsKey(address)) {
				connectsStatsPerAddress.put(address, new RpcConnectStats());
			}
		}

		return this;
	}

	public RpcClient withStreamProtocol(MemSize defaultPacketSize, MemSize maxPacketSize, boolean compression) {
		this.defaultPacketSize = defaultPacketSize;
		this.maxPacketSize = maxPacketSize;
		this.compression = compression;
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

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		if (CHECK) Checks.checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		Checks.checkNotNull(messageTypes, "Message types must be specified");

		Checks.checkState(stopPromise == null);

		serializer = serializerBuilder.withSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes).build(RpcMessage.class);

		return Promises.all(
				addresses.stream()
						.map(address -> {
							logger.info("Connecting: {}", address);
							return connect(address)
									.thenEx(($, e) -> Promise.complete());
						}))
				.then(() -> !forcedStart && requestSender instanceof NoSenderAvailable ?
						Promise.ofException(START_EXCEPTION) :
						Promise.complete());
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
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
					if (stopPromise != null) {
						asyncTcpSocketImpl.close();
						return;
					}
					asyncTcpSocketImpl
							.withInspector(statsSocket);
					AsyncTcpSocket socket = sslContext == null ?
							asyncTcpSocketImpl :
							wrapClientSocket(asyncTcpSocketImpl, sslContext, sslExecutor);
					RpcStream stream = new RpcStream(socket, serializer, defaultPacketSize, maxPacketSize,
							autoFlushInterval, compression, false); // , statsSerializer, statsDeserializer, statsCompressor, statsDecompressor);
					RpcClientConnection connection = new RpcClientConnection(eventloop, this, address, stream, keepAliveInterval.toMillis());
					stream.setListener(connection);

					// jmx
					if (isMonitoring()) {
						connection.startMonitoring();
					}
					connections.put(address, connection);
					requestSender = nullToSupplier(strategy.createSender(pool), NoSenderAvailable::new);

					// jmx
					generalConnectsStats.recordSuccessfulConnection();
					connectsStatsPerAddress.get(address).recordSuccessfulConnection();

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
		requestSender = nullToSupplier(strategy.createSender(pool), NoSenderAvailable::new);
		logger.info("Connection closed: {}", address);
		processClosedConnection(address);
	}

	private void processClosedConnection(InetSocketAddress address) {
		//jmx
		generalConnectsStats.recordFailedConnection();
		connectsStatsPerAddress.get(address).recordFailedConnection();

		if (stopPromise == null) {
			eventloop.delayBackground(reconnectIntervalMillis, wrapContext(this, () -> {
				if (stopPromise == null) {
					logger.info("Reconnecting: {}", address);
					connect(address);
				}
			}));
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
	 * @param request request for server
	 */
	@Override
	public <I, O> void sendRequest(I request, int timeout, Callback<O> cb) {
		if (CHECK) Checks.checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (timeout > 0) {
			requestSender.sendRequest(request, timeout, cb);
		} else {
			cb.accept(null, RPC_TIMEOUT_EXCEPTION);
		}
	}

	@Override
	public <I, O> void sendRequest(I request, Callback<O> cb) {
		if (CHECK) Checks.checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		requestSender.sendRequest(request, cb);
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
					eventloop.execute(wrapContext(requestSender, () ->
							requestSender.sendRequest(request, timeout, toAnotherEventloop(anotherEventloop, cb))));
				} else {
					cb.accept(null, RPC_TIMEOUT_EXCEPTION);
				}
			}

		};
	}

	// visible for testing
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
		public Set<InetSocketAddress> getAddresses() {
			return Collections.emptySet();
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

	@JmxAttribute(name = "connects")
	public RpcConnectStats getGeneralConnectsStats() {
		return generalConnectsStats;
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

	//	@JmxAttribute
	//	public StreamBinarySerializer.JmxInspector getStatsSerializer() {
	//		return statsSerializer;
	//	}
	//
	//	@JmxAttribute
	//	public StreamBinaryDeserializer.JmxInspector getStatsDeserializer() {
	//		return statsDeserializer;
	//	}
	//
	//	@JmxAttribute
	//	public StreamLZ4Compressor.JmxInspector getStatsCompressor() {
	//		return compression ? statsCompressor : null;
	//	}
	//
	//	@JmxAttribute
	//	public StreamLZ4Decompressor.JmxInspector getStatsDecompressor() {
	//		return compression ? statsDecompressor : null;
	//	}

	RpcRequestStats ensureRequestStatsPerClass(Class<?> requestClass) {
		if (!requestStatsPerClass.containsKey(requestClass)) {
			requestStatsPerClass.put(requestClass, RpcRequestStats.create(SMOOTHING_WINDOW));
		}
		return requestStatsPerClass.get(requestClass);
	}
}
