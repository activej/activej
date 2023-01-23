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

package io.activej.rpc.server;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.MemSize;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.process.frames.FrameFormat;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.jmx.stats.ValueStats;
import io.activej.net.AbstractReactiveServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.protocol.RpcControlMessage;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.protocol.RpcStream;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.time.Duration;
import java.util.*;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;

/**
 * An RPC server that works asynchronously. This server uses fast serializers
 * and custom optimized communication protocol, improving application
 * performance.
 * <p>
 * In order to set up a server it's mandatory to create it using
 * {@link #builder(NioReactor)}, indicate a types of messages, and specify
 * an appropriate {@link RpcRequestHandler request handlers} for that types.
 * <p>
 * There are two ways of starting a server:
 * <ul>
 * <li>Manually: set up the server and call {@code listen()}</li>
 * <li>Create a module for your RPC server and pass it to a {@code Launcher}
 * along with {@code ServiceGraphModule}.</li>
 * </ul>
 * <p>
 * Example. Here are the steps, intended to supplement the example, listed in
 * {@link RpcClient}:
 * <ul>
 * <li>Create a {@code RequestHandler} for {@code RequestClass} and
 * {@code ResponseClass}</li>
 * <li>Create an {@code RpcServer}</li>
 * <li>Run the server</li>
 * </ul>
 *
 * @see RpcRequestHandler
 * @see RpcClient
 */
public final class RpcServer extends AbstractReactiveServer {
	public static final MemSize DEFAULT_INITIAL_BUFFER_SIZE = ChannelSerializer.DEFAULT_INITIAL_BUFFER_SIZE;

	private MemSize initialBufferSize = DEFAULT_INITIAL_BUFFER_SIZE;
	private @Nullable FrameFormat frameFormat;
	private Duration autoFlushInterval = Duration.ZERO;

	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers = new LinkedHashMap<>();

	private final List<RpcServerConnection> connections = new ArrayList<>();

	private BinarySerializer<RpcMessage> serializer;

	private SettablePromise<Void> closeCallback;

	// region JMX vars
	static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);
	private final EventStats totalConnects = EventStats.create(SMOOTHING_WINDOW);
	private final Map<InetAddress, EventStats> connectsPerAddress = new HashMap<>();
	private final EventStats successfulRequests = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats failedRequests = EventStats.create(SMOOTHING_WINDOW);
	private final ValueStats requestHandlingTime = ValueStats.builder(SMOOTHING_WINDOW)
			.withUnit("milliseconds")
			.build();
	private final ExceptionStats lastRequestHandlingException = ExceptionStats.create();
	private final ExceptionStats lastProtocolError = ExceptionStats.create();
	private boolean monitoring;
	// endregion

	private RpcServer(NioReactor reactor) {
		super(reactor);
	}

	public static Builder builder(NioReactor reactor) {
		return new RpcServer(reactor).new Builder();
	}

	public final class Builder extends AbstractReactiveServer.Builder<Builder, RpcServer> {
		private List<Class<?>> messageTypes;
		private DefiningClassLoader classLoader = DefiningClassLoader.create(Thread.currentThread().getContextClassLoader());
		private SerializerFactory serializerFactory = SerializerFactory.defaultInstance();

		private Builder() {
			handlers.put(RpcControlMessage.class, request -> {
				if (request == RpcControlMessage.PING) {
					return Promise.of(RpcControlMessage.PONG);
				}
				return Promise.ofException(new MalformedDataException("Unknown message: " + request));
			});
		}

		public Builder withClassLoader(ClassLoader classLoader) {
			checkNotBuilt(this);
			this.classLoader = DefiningClassLoader.create(classLoader);
			return this;
		}

		/**
		 * Creates a server, capable of specified message types processing.
		 *
		 * @param messageTypes classes of messages processed by a server
		 * @return server instance capable for handling provided message types
		 */
		public Builder withMessageTypes(Class<?>... messageTypes) {
			checkNotBuilt(this);
			return withMessageTypes(List.of(messageTypes));
		}

		/**
		 * Creates a server, capable of specified message types processing.
		 *
		 * @param messageTypes a list of message types processed by a server
		 * @return server instance capable for handling provided message types
		 */
		public Builder withMessageTypes(List<Class<?>> messageTypes) {
			checkNotBuilt(this);
			checkArgument(new HashSet<>(messageTypes).size() == messageTypes.size(), "Message types must be unique");
			this.messageTypes = messageTypes;
			return this;
		}

		public Builder withSerializerFactory(SerializerFactory serializerFactory) {
			checkNotBuilt(this);
			this.serializerFactory = serializerFactory;
			return this;
		}

		public Builder withStreamProtocol(MemSize defaultPacketSize) {
			checkNotBuilt(this);
			RpcServer.this.initialBufferSize = defaultPacketSize;
			return this;
		}

		public Builder withStreamProtocol(MemSize defaultPacketSize, FrameFormat frameFormat) {
			checkNotBuilt(this);
			RpcServer.this.initialBufferSize = defaultPacketSize;
			RpcServer.this.frameFormat = frameFormat;
			return this;
		}

		@SuppressWarnings("UnusedReturnValue")
		public Builder withAutoFlushInterval(Duration autoFlushInterval) {
			checkNotBuilt(this);
			RpcServer.this.autoFlushInterval = autoFlushInterval;
			return this;
		}

		/**
		 * Adds a handler for a specified request-response pair.
		 *
		 * @param requestClass a class representing a request structure
		 * @param handler      a class containing logic of request processing and
		 *                     creating a response
		 * @param <I>          class of request
		 * @param <O>          class of response
		 * @return server instance capable for handling requests of concrete types
		 */
		public <I, O> Builder withHandler(Class<I> requestClass, RpcRequestHandler<I, O> handler) {
			checkNotBuilt(this);
			checkArgument(!handlers.containsKey(requestClass), "Handler for {} has already been added", requestClass);
			handlers.put(requestClass, handler);
			return this;
		}

		@Override
		protected RpcServer doBuild() {
			Set<Class<?>> handlersClasses = new HashSet<>(handlers.keySet());
			handlersClasses.remove(RpcControlMessage.class);
			checkState(!handlersClasses.isEmpty(), "No RPC handlers added");
			checkState(messageTypes != null, "Message types must be specified");
			//noinspection SlowListContainsAll
			checkState(messageTypes.containsAll(handlersClasses), "Some message types where not specified");
			serializerFactory.addSubclasses(RpcMessage.MESSAGE_TYPES, messageTypes);
			serializer = serializerFactory.create(classLoader, RpcMessage.class);

			return super.doBuild();
		}
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		RpcStream stream = new RpcStream(socket, serializer, initialBufferSize,
				autoFlushInterval, frameFormat, true); // , statsSerializer, statsDeserializer, statsCompressor, statsDecompressor);
		RpcServerConnection connection = new RpcServerConnection(this, remoteAddress, handlers, stream);
		stream.setListener(connection);
		add(connection);

		// jmx
		ensureConnectStats(remoteAddress).recordEvent();
		totalConnects.recordEvent();
	}

	@Override
	protected void onClose(SettablePromise<Void> cb) {
		if (connections.isEmpty()) {
			logger.info("RpcServer is closing. Active connections count: 0.");
			cb.set(null);
		} else {
			logger.info("RpcServer is closing. Active connections count: {}", connections.size());
			for (RpcServerConnection connection : new ArrayList<>(connections)) {
				connection.shutdown();
			}
			closeCallback = cb;
		}
	}

	void add(RpcServerConnection connection) {
		if (logger.isInfoEnabled())
			logger.info("Client connected on {}", connection);

		if (monitoring) {
			connection.startMonitoring();
		}

		connections.add(connection);
	}

	@SuppressWarnings("UnusedReturnValue")
	boolean remove(RpcServerConnection connection) {
		if (!connections.remove(connection)) {
			return false;
		}
		if (logger.isInfoEnabled())
			logger.info("Client disconnected on {}", connection);

		if (closeCallback != null) {
			logger.info("RpcServer is closing. One more connection was closed. " +
					"Active connections count: {}", connections.size());

			if (connections.isEmpty()) {
				closeCallback.set(null);
			}
		}
		return true;
	}

	// region JMX
	@JmxOperation(description = "enable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled) ]")
	public void startMonitoring() {
		monitoring = true;
		for (RpcServerConnection connection : connections) {
			connection.startMonitoring();
		}
	}

	@JmxOperation(description = "disable monitoring " +
			"[ when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled) ]")
	public void stopMonitoring() {
		monitoring = false;
		for (RpcServerConnection connection : connections) {
			connection.stopMonitoring();
		}
	}

	@JmxAttribute(description = "when monitoring is enabled more stats are collected, but it causes more overhead " +
			"(for example, requestHandlingTime stats are collected only when monitoring is enabled)")
	public boolean isMonitoring() {
		return monitoring;
	}

	@JmxAttribute(description = "current number of connections", reducer = JmxReducerSum.class)
	public int getConnectionsCount() {
		return connections.size();
	}

	@JmxAttribute
	public EventStats getTotalConnects() {
		return totalConnects;
	}

	//	FIXME (vmykhalko) @JmxAttribute(description = "number of connects/reconnects per client address")
	public Map<InetAddress, EventStats> getConnectsPerAddress() {
		return connectsPerAddress;
	}

	private EventStats ensureConnectStats(InetAddress address) {
		return connectsPerAddress.computeIfAbsent(address, $ -> EventStats.create(SMOOTHING_WINDOW));
	}

	@JmxOperation(description = "detailed information about connections")
	public List<RpcServerConnection> getConnections() {
		return connections;
	}

	@JmxAttribute(extraSubAttributes = "totalCount", description = "number of requests which were processed correctly")
	public EventStats getSuccessfulRequests() {
		return successfulRequests;
	}

	@JmxAttribute(extraSubAttributes = "totalCount", description = "request with error responses (number of requests which were handled with error)")
	public EventStats getFailedRequests() {
		return failedRequests;
	}

	@JmxAttribute(description = "time for handling one request in milliseconds (both successful and failed)")
	public ValueStats getRequestHandlingTime() {
		return requestHandlingTime;
	}

	@JmxAttribute(description = "exception that occurred because of business logic error " +
			"(in RpcRequestHandler implementation)")
	public ExceptionStats getLastRequestHandlingException() {
		return lastRequestHandlingException;
	}

	@JmxAttribute(description = "exception that occurred because of protocol error " +
			"(serialization, deserialization, compression, decompression, etc)")
	public ExceptionStats getLastProtocolError() {
		return lastProtocolError;
	}
	// endregion
}

