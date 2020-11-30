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
import io.activej.common.ApplicationSettings;
import io.activej.common.Checks;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.CloseException;
import io.activej.common.time.Stopwatch;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.eventloop.Eventloop;
import io.activej.jmx.api.JmxRefreshable;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.EventStats;
import io.activej.rpc.client.jmx.RpcRequestStats;
import io.activej.rpc.client.sender.RpcSender;
import io.activej.rpc.protocol.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.activej.common.Checks.checkState;
import static io.activej.eventloop.util.RunnableWithContext.wrapContext;
import static org.slf4j.LoggerFactory.getLogger;

public final class RpcClientConnection implements RpcStream.Listener, RpcSender, JmxRefreshable {
	private static final Logger logger = getLogger(RpcClientConnection.class);
	private static final boolean CHECK = Checks.isEnabled(RpcClientConnection.class);

	private static final int BUCKET_CAPACITY = ApplicationSettings.getInt(RpcClientConnection.class, "bucketCapacity", 16);

	private static final CloseException CONNECTION_CLOSED = new CloseException(RpcClientConnection.class, "Connection closed");
	private static final RpcException CONNECTION_UNRESPONSIVE = new RpcException(RpcClientConnection.class, "Unresponsive connection");
	private static final RpcOverloadException RPC_OVERLOAD_EXCEPTION = new RpcOverloadException(RpcClientConnection.class, "RPC client is overloaded");
	private static final AsyncTimeoutException RPC_TIMEOUT_EXCEPTION = new AsyncTimeoutException(RpcClientConnection.class, "RPC request has timed out");

	private StreamDataAcceptor<RpcMessage> downstreamDataAcceptor = null;
	private boolean overloaded = false;
	private boolean closed;

	private final Eventloop eventloop;
	private final RpcClient rpcClient;
	private final RpcStream stream;
	private final InetSocketAddress address;
	private final Map<Integer, Callback<?>> activeRequests = new HashMap<>();
	private final Map<Long, ExpirationList> expirationLists = new HashMap<>();

	private ArrayList<RpcMessage> initialBuffer = new ArrayList<>();

	private static final class ExpirationList {
		private int size;
		private int[] cookies;

		ExpirationList(int[] cookies) {
			this.cookies = cookies;
		}
	}

	private int cookie = 0;
	private boolean serverClosing;

	// JMX
	private boolean monitoring;
	private final RpcRequestStats connectionStats;
	private final EventStats totalRequests;
	private final EventStats connectionRequests;

	// keep-alive pings
	private final long keepAliveMillis;
	private boolean pongReceived;

	RpcClientConnection(Eventloop eventloop, RpcClient rpcClient, InetSocketAddress address, RpcStream stream,
			long keepAliveMillis) {
		this.eventloop = eventloop;
		this.rpcClient = rpcClient;
		this.stream = stream;
		this.address = address;
		this.keepAliveMillis = keepAliveMillis;

		// JMX
		this.monitoring = false;
		this.connectionStats = RpcRequestStats.create(RpcClient.SMOOTHING_WINDOW);
		this.connectionRequests = connectionStats.getTotalRequests();
		this.totalRequests = rpcClient.getGeneralRequestsStats().getTotalRequests();
	}

	@Override
	public <I, O> void sendRequest(I request, int timeout, @NotNull Callback<O> cb) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");

		// jmx
		totalRequests.recordEvent();
		connectionRequests.recordEvent();

		if (!overloaded || request instanceof RpcMandatoryData) {
			cookie++;

			// jmx
			if (monitoring) {
				cb = doJmxMonitoring(request, timeout, cb);
			}

			if (timeout != Integer.MAX_VALUE) {
				ExpirationList list = expirationLists.computeIfAbsent(eventloop.currentTimeMillis() + timeout, t -> {
					ExpirationList l = new ExpirationList(new int[BUCKET_CAPACITY]);
					eventloop.scheduleBackground(t, wrapContext(this, () -> {
						expirationLists.remove(t);

						for (int i = 0; i < l.size; i++) {
							Callback<?> expiredCb = activeRequests.remove(l.cookies[i]);
							if (expiredCb != null) {
								// jmx
								connectionStats.getExpiredRequests().recordEvent();
								rpcClient.getGeneralRequestsStats().getExpiredRequests().recordEvent();

								expiredCb.accept(null, RPC_TIMEOUT_EXCEPTION);
							}
						}

						if (serverClosing && activeRequests.size() == 0) {
							shutdown();
						}
					}));
					return l;
				});

				if (list.size >= list.cookies.length) {
					list.cookies = Arrays.copyOf(list.cookies, list.cookies.length * 2);
				}
				list.cookies[list.size++] = cookie;
			}

			activeRequests.put(cookie, cb);

			downstreamDataAcceptor.accept(RpcMessage.of(cookie, request));
		} else {
			doProcessOverloaded(cb);
		}
	}

	@Override
	public <I, O> void sendRequest(I request, @NotNull Callback<O> cb) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");

		// jmx
		totalRequests.recordEvent();
		connectionRequests.recordEvent();

		if (!overloaded || request instanceof RpcMandatoryData) {
			cookie++;

			// jmx
			if (monitoring) {
				cb = doJmxMonitoring(request, Integer.MAX_VALUE, cb);
			}

			activeRequests.put(cookie, cb);

			downstreamDataAcceptor.accept(RpcMessage.of(cookie, request));
		} else {
			doProcessOverloaded(cb);
		}
	}

	private <I, O> Callback<O> doJmxMonitoring(I request, int timeout, @NotNull Callback<O> cb) {
		RpcRequestStats requestStatsPerClass = rpcClient.ensureRequestStatsPerClass(request.getClass());
		requestStatsPerClass.getTotalRequests().recordEvent();
		return new JmxConnectionMonitoringResultCallback<>(requestStatsPerClass, cb, timeout);
	}

	private <O> void doProcessOverloaded(@NotNull Callback<O> cb) {
		// jmx
		rpcClient.getGeneralRequestsStats().getRejectedRequests().recordEvent();
		connectionStats.getRejectedRequests().recordEvent();
		if (logger.isTraceEnabled()) logger.trace("RPC client uplink is overloaded");

		cb.accept(null, RPC_OVERLOAD_EXCEPTION);
	}

	@Override
	public void accept(RpcMessage message) {
		if (message.getData().getClass() == RpcRemoteException.class) {
			processErrorMessage(message);
		} else if (message.getData().getClass() == RpcControlMessage.class) {
			processControlMessage((RpcControlMessage) message.getData());
		} else {
			@SuppressWarnings("unchecked")
			Callback<Object> cb = (Callback<Object>) activeRequests.remove(message.getCookie());
			if (cb == null) return;

			cb.accept(message.getData(), null);
			if (serverClosing && activeRequests.size() == 0) {
				shutdown();
			}
		}
	}

	private void processErrorMessage(RpcMessage message) {
		RpcRemoteException remoteException = (RpcRemoteException) message.getData();
		// jmx
		connectionStats.getFailedRequests().recordEvent();
		rpcClient.getGeneralRequestsStats().getFailedRequests().recordEvent();
		connectionStats.getServerExceptions().recordException(remoteException, null);
		rpcClient.getGeneralRequestsStats().getServerExceptions().recordException(remoteException, null);

		Callback<?> cb = activeRequests.remove(message.getCookie());
		if (cb != null) {
			cb.accept(null, remoteException);
		}
	}

	private void processControlMessage(RpcControlMessage controlMessage) {
		if (controlMessage == RpcControlMessage.CLOSE) {
			rpcClient.removeConnection(address);
			serverClosing = true;
			if (activeRequests.size() == 0) {
				shutdown();
			}
		} else if (controlMessage == RpcControlMessage.PONG) {
			pongReceived = true;
		} else {
			throw new RuntimeException("Received unknown RpcControlMessage");
		}
	}

	private void ping() {
		if (isClosed()) return;
		if (keepAliveMillis == 0) return;
		pongReceived = false;
		downstreamDataAcceptor.accept(RpcMessage.of(-1, RpcControlMessage.PING));
		eventloop.delayBackground(keepAliveMillis, () -> {
			if (isClosed()) return;
			if (!pongReceived) {
				onReceiverError(CONNECTION_UNRESPONSIVE);
			} else {
				ping();
			}
		});
	}

	@Override
	public void onReceiverEndOfStream() {
		if (isClosed()) return;
		logger.info("Receiver EOS: {}", address);
		stream.close();
		doClose();
	}

	@Override
	public void onReceiverError(@NotNull Throwable e) {
		if (isClosed()) return;
		logger.error("Receiver error: {}", address, e);
		rpcClient.getLastProtocolError().recordException(e, address);
		stream.close();
		doClose();
	}

	@Override
	public void onSenderError(@NotNull Throwable e) {
		if (isClosed()) return;
		logger.error("Sender error: {}", address, e);
		rpcClient.getLastProtocolError().recordException(e, address);
		stream.close();
		doClose();
	}

	@Override
	public void onSerializationError(RpcMessage message, @NotNull Throwable e) {
		if (isClosed()) return;
		logger.error("Serialization error: {} for data {}", address, message.getData(), e);
		rpcClient.getLastProtocolError().recordException(e, address);
		activeRequests.remove(message.getCookie()).accept(null, e);
	}

	@Override
	public void onSenderReady(@NotNull StreamDataAcceptor<RpcMessage> acceptor) {
		if (isClosed()) return;
		downstreamDataAcceptor = acceptor;
		overloaded = false;
		if (initialBuffer != null) {
			for (RpcMessage message : initialBuffer) {
				acceptor.accept(message);
			}
			initialBuffer = null;
			ping();
		}
	}

	@Override
	public void onSenderSuspended() {
		overloaded = true;
	}

	private void doClose() {
		if (isClosed()) return;
		downstreamDataAcceptor = null;
		closed = true;
		rpcClient.removeConnection(address);

		while (!activeRequests.isEmpty()) {
			for (Integer cookie : new HashSet<>(activeRequests.keySet())) {
				Callback<?> cb = activeRequests.remove(cookie);
				if (cb != null) {
					cb.accept(null, CONNECTION_CLOSED);
				}
			}
		}
	}

	public boolean isClosed() {
		return closed;
	}

	public void shutdown() {
		if (isClosed()) return;
		stream.sendEndOfStream();
	}

	// JMX

	public void startMonitoring() {
		monitoring = true;
	}

	public void stopMonitoring() {
		monitoring = false;
	}

	@JmxAttribute(name = "")
	public RpcRequestStats getRequestStats() {
		return connectionStats;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getActiveRequests() {
		return activeRequests.size();
	}

	@Override
	public void refresh(long timestamp) {
		connectionStats.refresh(timestamp);
	}

	private final class JmxConnectionMonitoringResultCallback<T> implements Callback<T> {
		private final Stopwatch stopwatch;
		private final Callback<T> callback;
		private final RpcRequestStats requestStatsPerClass;
		private final long dueTimestamp;

		public JmxConnectionMonitoringResultCallback(RpcRequestStats requestStatsPerClass, Callback<T> cb,
				long timeout) {
			this.stopwatch = Stopwatch.createStarted();
			this.callback = cb;
			this.requestStatsPerClass = requestStatsPerClass;
			this.dueTimestamp = eventloop.currentTimeMillis() + timeout;
		}

		@Override
		public void accept(T result, @Nullable Throwable e) {
			if (e == null) {
				onResult(result);
			} else {
				onException(e);
			}
		}

		private void onResult(T result) {
			int responseTime = timeElapsed();
			connectionStats.getResponseTime().recordValue(responseTime);
			requestStatsPerClass.getResponseTime().recordValue(responseTime);
			rpcClient.getGeneralRequestsStats().getResponseTime().recordValue(responseTime);
			recordOverdue();
			callback.accept(result, null);
		}

		private void onException(@NotNull Throwable e) {
			if (e instanceof RpcRemoteException) {
				int responseTime = timeElapsed();
				connectionStats.getFailedRequests().recordEvent();
				connectionStats.getResponseTime().recordValue(responseTime);
				connectionStats.getServerExceptions().recordException(e, null);
				requestStatsPerClass.getFailedRequests().recordEvent();
				requestStatsPerClass.getResponseTime().recordValue(responseTime);
				rpcClient.getGeneralRequestsStats().getResponseTime().recordValue(responseTime);
				requestStatsPerClass.getServerExceptions().recordException(e, null);
				recordOverdue();
			} else if (e instanceof AsyncTimeoutException) {
				connectionStats.getExpiredRequests().recordEvent();
				requestStatsPerClass.getExpiredRequests().recordEvent();
			} else if (e instanceof RpcOverloadException) {
				connectionStats.getRejectedRequests().recordEvent();
				requestStatsPerClass.getRejectedRequests().recordEvent();
			}
			callback.accept(null, e);
		}

		private int timeElapsed() {
			return (int) stopwatch.elapsed(TimeUnit.MILLISECONDS);
		}

		private void recordOverdue() {
			int overdue = (int) (System.currentTimeMillis() - dueTimestamp);
			if (overdue > 0) {
				connectionStats.getOverdues().recordValue(overdue);
				requestStatsPerClass.getOverdues().recordValue(overdue);
				rpcClient.getGeneralRequestsStats().getOverdues().recordValue(overdue);
			}
		}
	}

	@Override
	public String toString() {
		int active = activeRequests.size();
		long failed = connectionStats.getFailedRequests().getTotalCount();

		return "RpcClientConnection{" +
				"address=" + address +
				", active=" + active +
				", successes=" + (connectionStats.getTotalRequests().getTotalCount() - failed - active) +
				", failures=" + failed +
				'}';
	}

}
