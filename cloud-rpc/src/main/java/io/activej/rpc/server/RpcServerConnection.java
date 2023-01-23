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

import io.activej.common.exception.MalformedDataException;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.jmx.api.JmxRefreshable;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.rpc.protocol.RpcControlMessage;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.protocol.RpcRemoteException;
import io.activej.rpc.protocol.RpcStream;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;

public final class RpcServerConnection extends AbstractReactive implements RpcStream.Listener, JmxRefreshable {
	private static final Logger logger = LoggerFactory.getLogger(RpcServerConnection.class);

	private final RpcMessage rpcMessage = new RpcMessage();
	private StreamDataAcceptor<RpcMessage> downstreamDataAcceptor;

	private final RpcServer rpcServer;
	private final RpcStream stream;
	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers;

	private int activeRequests = 1;

	// jmx
	private final InetAddress remoteAddress;
	private final ExceptionStats lastRequestHandlingException = ExceptionStats.create();
	private final ValueStats requestHandlingTime = ValueStats.builder(RpcServer.SMOOTHING_WINDOW)
			.withUnit("milliseconds")
			.build();
	private final EventStats successfulRequests = EventStats.create(RpcServer.SMOOTHING_WINDOW);
	private final EventStats failedRequests = EventStats.create(RpcServer.SMOOTHING_WINDOW);
	private boolean monitoring = false;

	RpcServerConnection(Reactor reactor, RpcServer rpcServer, InetAddress remoteAddress,
			Map<Class<?>, RpcRequestHandler<?, ?>> handlers, RpcStream stream) {
		super(reactor);
		this.rpcServer = rpcServer;
		this.stream = stream;
		this.handlers = handlers;

		// jmx
		this.remoteAddress = remoteAddress;
	}

	@SuppressWarnings("unchecked")
	private Promise<Object> serve(Object request) {
		RpcRequestHandler<Object, Object> requestHandler = (RpcRequestHandler<Object, Object>) handlers.get(request.getClass());
		if (requestHandler == null) {
			return Promise.ofException(new MalformedDataException("Failed to process request " + request));
		}
		return requestHandler.run(request).promise();
	}

	@Override
	public void accept(RpcMessage message) {
		activeRequests++;

		int cookie = message.cookie;
		long startTime = monitoring ? System.currentTimeMillis() : 0;

		Object messageData = message.data;
		serve(messageData)
				.run((result, e) -> {
					if (startTime != 0) {
						int value = (int) (System.currentTimeMillis() - startTime);
						requestHandlingTime.recordValue(value);
						rpcServer.getRequestHandlingTime().recordValue(value);
					}
					if (e == null) {
						downstreamDataAcceptor.accept(rpcMessage.with(cookie, result));

						successfulRequests.recordEvent();
						rpcServer.getSuccessfulRequests().recordEvent();
					} else {
						logger.warn("Exception while processing request ID {}", cookie, e);
						RpcMessage errorMessage = rpcMessage.with(cookie, new RpcRemoteException(e));
						sendError(errorMessage, messageData, e);
					}
					if (--activeRequests == 0) {
						doClose();
						stream.sendEndOfStream();
					}
				});
	}

	@Override
	public void onReceiverEndOfStream() {
		activeRequests--;
		if (activeRequests == 0) {
			doClose();
			stream.sendEndOfStream();
		}
	}

	@Override
	public void onReceiverError(Exception e) {
		logger.error("Receiver error {}", remoteAddress, e);
		rpcServer.getLastProtocolError().recordException(e, remoteAddress);
		doClose();
		stream.close();
	}

	@Override
	public void onSenderError(Exception e) {
		logger.error("Sender error: {}", remoteAddress, e);
		rpcServer.getLastProtocolError().recordException(e, remoteAddress);
		doClose();
		stream.close();
	}

	@Override
	public void onSerializationError(RpcMessage message, Exception e) {
		logger.error("Serialization error: {} for data {}", remoteAddress, message.data, e);
		RpcMessage errorMessage = rpcMessage.with(message.cookie, new RpcRemoteException(e));
		sendError(errorMessage, message.data, e);
	}

	@Override
	public void onSenderReady(StreamDataAcceptor<RpcMessage> acceptor) {
		this.downstreamDataAcceptor = acceptor;
		stream.receiverResume();
	}

	@Override
	public void onSenderSuspended() {
		stream.receiverSuspend();
	}

	private void sendError(RpcMessage errorMessage, Object messageData, @Nullable Exception e) {
		downstreamDataAcceptor.accept(errorMessage);
		lastRequestHandlingException.recordException(e, messageData);
		rpcServer.getLastRequestHandlingException().recordException(e, messageData);
		failedRequests.recordEvent();
		rpcServer.getFailedRequests().recordEvent();
	}

	private void doClose() {
		rpcServer.remove(this);
		downstreamDataAcceptor = $ -> {};
	}

	public void shutdown() {
		if (downstreamDataAcceptor != null) {
			downstreamDataAcceptor.accept(rpcMessage.with(-1, RpcControlMessage.CLOSE));
		}
	}

	// jmx
	public void startMonitoring() {
		monitoring = true;
	}

	public void stopMonitoring() {
		monitoring = false;
	}

	@JmxAttribute
	public EventStats getSuccessfulRequests() {
		return successfulRequests;
	}

	@JmxAttribute
	public EventStats getFailedRequests() {
		return failedRequests;
	}

	@JmxAttribute
	public ValueStats getRequestHandlingTime() {
		return requestHandlingTime;
	}

	@JmxAttribute
	public ExceptionStats getLastRequestHandlingException() {
		return lastRequestHandlingException;
	}

	@JmxAttribute
	public String getRemoteAddress() {
		return remoteAddress.toString();
	}

	@Override
	public void refresh(long timestamp) {
		successfulRequests.refresh(timestamp);
		failedRequests.refresh(timestamp);
		requestHandlingTime.refresh(timestamp);
	}

	@Override
	public String toString() {
		return "RpcServerConnection{" +
				"address=" + remoteAddress +
				", active=" + activeRequests +
				", successes=" + successfulRequests.getTotalCount() +
				", failures=" + failedRequests.getTotalCount() +
				'}';
	}
}
