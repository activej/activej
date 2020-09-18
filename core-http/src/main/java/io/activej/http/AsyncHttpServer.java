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

package io.activej.http;

import io.activej.common.ApplicationSettings;
import io.activej.common.MemSize;
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.inspector.BaseInspector;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.schedule.ScheduledRunnable;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerSum;
import io.activej.jmx.stats.EventStats;
import io.activej.jmx.stats.ExceptionStats;
import io.activej.net.AbstractServer;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static io.activej.eventloop.util.RunnableWithContext.wrapContext;
import static io.activej.http.AbstractHttpConnection.READ_TIMEOUT_ERROR;
import static java.util.stream.Collectors.toList;

/**
 * This is an implementation of the asynchronous HTTP server on top of {@link Eventloop}.
 * It has a root {@link AsyncServlet} that receives and handles all the responses that come to this server.
 */
@SuppressWarnings({"UnusedReturnValue", "WeakerAccess", "unused"})
public final class AsyncHttpServer extends AbstractServer<AsyncHttpServer> {
	public static final Duration READ_WRITE_TIMEOUT = ApplicationSettings.getDuration(AsyncHttpServer.class, "readWriteTimeout", Duration.ZERO);
	public static final Duration READ_WRITE_TIMEOUT_SHUTDOWN = ApplicationSettings.getDuration(AsyncHttpServer.class, "readWriteTimeout_Shutdown", Duration.ofSeconds(3));
	public static final Duration SERVE_TIMEOUT_SHUTDOWN = ApplicationSettings.getDuration(AsyncHttpServer.class, "serveTimeout_Shutdown", Duration.ofSeconds(0));
	public static final Duration KEEP_ALIVE_TIMEOUT = ApplicationSettings.getDuration(AsyncHttpServer.class, "keepAliveTimeout", Duration.ofSeconds(30));
	public static final MemSize MAX_BODY_SIZE = ApplicationSettings.getMemSize(AsyncHttpServer.class, "maxBodySize", MemSize.ZERO);
	public static final MemSize MAX_WEB_SOCKET_MESSAGE_SIZE = ApplicationSettings.getMemSize(AsyncHttpServer.class, "maxWebSocketMessageSize", MemSize.megabytes(1));
	public static final int MAX_KEEP_ALIVE_REQUESTS = ApplicationSettings.getInt(AsyncHttpServer.class, "maxKeepAliveRequests", 0);

	@NotNull
	private final AsyncServlet servlet;
	private final char[] charBuffer = new char[1024];
	@NotNull
	private HttpExceptionFormatter errorFormatter = HttpExceptionFormatter.COMMON_FORMATTER;

	int readWriteTimeoutMillis = (int) READ_WRITE_TIMEOUT.toMillis();
	int readWriteTimeoutMillisShutdown = (int) READ_WRITE_TIMEOUT_SHUTDOWN.toMillis();
	int serveTimeoutMillisShutdown = (int) SERVE_TIMEOUT_SHUTDOWN.toMillis();
	int keepAliveTimeoutMillis = (int) KEEP_ALIVE_TIMEOUT.toMillis();
	int maxBodySize = MAX_BODY_SIZE.toInt();
	int maxWebSocketMessageSize = MAX_WEB_SOCKET_MESSAGE_SIZE.toInt();
	int maxKeepAliveRequests = MAX_KEEP_ALIVE_REQUESTS;

	final ConnectionsLinkedList poolNew = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolReadWrite = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolServing = new ConnectionsLinkedList();
	final ConnectionsLinkedList poolKeepAlive = new ConnectionsLinkedList();
	private int poolKeepAliveExpired;
	private int poolReadWriteExpired;

	@Nullable
	private ScheduledRunnable expiredConnectionsCheck;

	@Nullable
	Inspector inspector;

	public interface Inspector extends BaseInspector<Inspector> {
		void onHttpError(InetAddress remoteAddress, Throwable e);

		void onHttpRequest(HttpRequest request, boolean keepAlive);

		void onHttpResponse(HttpRequest request, HttpResponse httpResponse, boolean keepAlive);

		void onServletException(HttpRequest request, Throwable e);
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final EventStats totalRequests = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats totalResponses = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats httpTimeouts = EventStats.create(SMOOTHING_WINDOW);
		private final ExceptionStats httpErrors = ExceptionStats.create();
		private final ExceptionStats servletExceptions = ExceptionStats.create();

		@Override
		public void onHttpError(InetAddress remoteAddress, Throwable e) {
			if (e == AbstractHttpConnection.READ_TIMEOUT_ERROR || e == AbstractHttpConnection.WRITE_TIMEOUT_ERROR) {
				httpTimeouts.recordEvent();
			} else {
				httpErrors.recordException(e);
			}
		}

		@Override
		public void onHttpRequest(HttpRequest request, boolean keepAlive) {
			totalRequests.recordEvent();
		}

		@Override
		public void onHttpResponse(HttpRequest request, HttpResponse httpResponse, boolean keepAlive) {
			totalResponses.recordEvent();
		}

		@Override
		public void onServletException(HttpRequest request, Throwable e) {
			servletExceptions.recordException(e, request.toString());
		}

		@JmxAttribute(extraSubAttributes = "totalCount")
		public EventStats getTotalRequests() {
			return totalRequests;
		}

		@JmxAttribute(extraSubAttributes = "totalCount")
		public EventStats getTotalResponses() {
			return totalResponses;
		}

		@JmxAttribute
		public EventStats getHttpTimeouts() {
			return httpTimeouts;
		}

		@JmxAttribute(description = "Number of requests which were invalid according to http protocol. " +
				"Responses were not sent for this requests")
		public ExceptionStats getHttpErrors() {
			return httpErrors;
		}

		@JmxAttribute(description = "Number of requests which were valid according to http protocol, " +
				"but application produced error during handling this request " +
				"(responses with 4xx and 5xx HTTP status codes)")
		public ExceptionStats getServletExceptions() {
			return servletExceptions;
		}
	}

	// region builders
	private AsyncHttpServer(@NotNull Eventloop eventloop, @NotNull AsyncServlet servlet) {
		super(eventloop);
		this.servlet = servlet;
	}

	public static AsyncHttpServer create(@NotNull Eventloop eventloop, @NotNull AsyncServlet servlet) {
		return new AsyncHttpServer(eventloop, servlet);
	}

	public AsyncHttpServer withKeepAliveTimeout(@NotNull Duration keepAliveTime) {
		keepAliveTimeoutMillis = (int) keepAliveTime.toMillis();
		return this;
	}

	public AsyncHttpServer withMaxKeepAliveRequests(int maxKeepAliveRequests) {
		this.maxKeepAliveRequests = maxKeepAliveRequests;
		return this;
	}

	public AsyncHttpServer withNoKeepAlive() {
		return withKeepAliveTimeout(Duration.ZERO);
	}

	public AsyncHttpServer withReadWriteTimeout(@NotNull Duration readWriteTimeout) {
		this.readWriteTimeoutMillis = (int) readWriteTimeout.toMillis();
		return this;
	}

	public AsyncHttpServer withReadWriteTimeout(@NotNull Duration readWriteTimeout, @NotNull Duration readWriteTimeoutShutdown) {
		this.readWriteTimeoutMillis = (int) readWriteTimeout.toMillis();
		this.readWriteTimeoutMillisShutdown = (int) readWriteTimeoutShutdown.toMillis();
		return this;
	}

	public AsyncHttpServer withServeTimeoutShutdown(@NotNull Duration serveTimeoutShutdown) {
		this.serveTimeoutMillisShutdown = (int) serveTimeoutShutdown.toMillis();
		return this;
	}

	public AsyncHttpServer withMaxBodySize(MemSize maxBodySize) {
		return withMaxBodySize(maxBodySize.toInt());
	}

	public AsyncHttpServer withMaxBodySize(int maxBodySize) {
		this.maxBodySize = maxBodySize;
		return this;
	}

	public AsyncHttpServer withMaxWebSocketMessageSize(MemSize maxWebSocketMessageSize) {
		this.maxWebSocketMessageSize = maxWebSocketMessageSize.toInt();
		return this;
	}

	public AsyncHttpServer withHttpErrorFormatter(@NotNull HttpExceptionFormatter httpExceptionFormatter) {
		errorFormatter = httpExceptionFormatter;
		return this;
	}

	public AsyncHttpServer withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	// endregion

	public Duration getKeepAliveTimeout() {
		return Duration.ofMillis(keepAliveTimeoutMillis);
	}

	public Duration getReadWriteTimeout() {
		return Duration.ofMillis(readWriteTimeoutMillis);
	}

	public Promise<Void> getCloseNotification() {
		return closeNotification;
	}

	private void scheduleExpiredConnectionsCheck() {
		assert expiredConnectionsCheck == null;
		expiredConnectionsCheck = eventloop.delayBackground(1000L, wrapContext(this, () -> {
			expiredConnectionsCheck = null;
			boolean isClosing = closeCallback != null;
			if (readWriteTimeoutMillis != 0 || isClosing) {
				poolReadWriteExpired += poolNew.closeExpiredConnections(eventloop.currentTimeMillis() -
						(!isClosing ? readWriteTimeoutMillis : readWriteTimeoutMillisShutdown));
				poolReadWriteExpired += poolReadWrite.closeExpiredConnections(eventloop.currentTimeMillis() -
						(!isClosing ? readWriteTimeoutMillis : readWriteTimeoutMillisShutdown), READ_TIMEOUT_ERROR);
			}
			poolKeepAliveExpired += poolKeepAlive.closeExpiredConnections(eventloop.currentTimeMillis() - keepAliveTimeoutMillis);
			if (getConnectionsCount() != 0) {
				scheduleExpiredConnectionsCheck();
				if (isClosing) {
					logger.info("...Waiting for {}", this);
				}
			}
		}));
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		if (expiredConnectionsCheck == null) {
			scheduleExpiredConnectionsCheck();
		}
		HttpServerConnection connection = new HttpServerConnection(eventloop, remoteAddress, socket, this, servlet, charBuffer);
		connection.serve();
	}

	private final SettablePromise<@Nullable Void> closeNotification = new SettablePromise<>();

	@Nullable
	private SettablePromise<Void> closeCallback;

	void onConnectionClosed() {
		if (getConnectionsCount() == 0 && closeCallback != null) {
			closeCallback.set(null);
			closeCallback = null;
		}
	}

	@Override
	protected void onClose(SettablePromise<@Nullable Void> cb) {
		closeNotification.set(null);
		poolKeepAlive.closeAllConnections();
		keepAliveTimeoutMillis = 0;
		if (getConnectionsCount() == 0) {
			cb.set(null);
		} else {
			if (!poolServing.isEmpty() && serveTimeoutMillisShutdown != 0) {
				eventloop.delayBackground(serveTimeoutMillisShutdown, wrapContext(this, poolServing::closeAllConnections));
			}
			closeCallback = cb;
			logger.info("Waiting for {}", this);
		}
	}

	private static String format(InetSocketAddress address, boolean ssl) {
		return (ssl ? "https://" : "http://") +
				("0.0.0.0".equals(address.getHostName())
						? "localhost"
						: address.getHostName()) +
				(address.getPort() != (ssl ? 443 : 80) ?
						":" + address.getPort()
						: "") + "/";
	}

	public List<String> getHttpAddresses() {
		return Stream.concat(
				listenAddresses.stream().map(address -> format(address, false)),
				sslListenAddresses.stream().map(address -> format(address, true))
		).collect(toList());
	}

	@JmxAttribute(description = "current number of connections", reducer = JmxReducerSum.class)
	public int getConnectionsCount() {
		return poolNew.size() + poolKeepAlive.size() + poolReadWrite.size() + poolServing.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsNewCount() {
		return poolNew.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadWriteCount() {
		return poolReadWrite.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsServingCount() {
		return poolServing.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveCount() {
		return poolKeepAlive.size();
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsKeepAliveExpired() {
		return poolKeepAliveExpired;
	}

	@JmxAttribute(reducer = JmxReducerSum.class)
	public int getConnectionsReadWriteExpired() {
		return poolReadWriteExpired;
	}

	HttpResponse formatHttpError(Throwable e) {
		return errorFormatter.formatException(e);
	}

	@JmxAttribute(name = "")
	@Nullable
	public JmxInspector getStats() {
		return BaseInspector.lookup(inspector, JmxInspector.class);
	}

	@Override
	public String toString() {
		return "AsyncHttpServer" + "{" + "new:" + poolNew.size() + " read/write:" + poolReadWrite.size() + " serving:" + poolServing.size() + " keep-alive:" + poolKeepAlive.size() + "}";
	}
}
