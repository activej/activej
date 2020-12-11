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

package io.activej.dns;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.Checks;
import io.activej.common.exception.AsyncTimeoutException;
import io.activej.common.exception.CloseException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.inspector.BaseInspector;
import io.activej.dns.protocol.*;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.eventloop.net.DatagramSocketSettings;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.EventStats;
import io.activej.net.socket.udp.AsyncUdpSocket;
import io.activej.net.socket.udp.AsyncUdpSocketNio;
import io.activej.net.socket.udp.UdpPacket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.activej.common.Checks.checkState;
import static io.activej.promise.Promises.timeout;

/**
 * Implementation of {@link AsyncDnsClient} that asynchronously
 * connects to some <i>real</i> DNS server and gets the response from it.
 */
public final class RemoteAsyncDnsClient implements AsyncDnsClient, EventloopJmxBeanEx {
	private final Logger logger = LoggerFactory.getLogger(RemoteAsyncDnsClient.class);
	private static final boolean CHECK = Checks.isEnabled(RemoteAsyncDnsClient.class);

	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(3);
	private static final int DNS_SERVER_PORT = 53;
	public static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress("8.8.8.8", DNS_SERVER_PORT);
	public static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);

	private final Eventloop eventloop;
	private final Map<DnsTransaction, SettablePromise<DnsResponse>> transactions = new HashMap<>();

	private DatagramSocketSettings datagramSocketSettings = DatagramSocketSettings.create();
	private InetSocketAddress dnsServerAddress = GOOGLE_PUBLIC_DNS;
	private Duration timeout = DEFAULT_TIMEOUT;

	@Nullable
	private AsyncUdpSocket socket;

	@Nullable
	private AsyncUdpSocketNio.Inspector socketInspector;
	@Nullable
	private Inspector inspector;

	// region creators
	private RemoteAsyncDnsClient(Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	public static RemoteAsyncDnsClient create(Eventloop eventloop) {
		return new RemoteAsyncDnsClient(eventloop);
	}

	public RemoteAsyncDnsClient withDatagramSocketSetting(DatagramSocketSettings setting) {
		this.datagramSocketSettings = setting;
		return this;
	}

	public RemoteAsyncDnsClient withTimeout(Duration timeout) {
		this.timeout = timeout;
		return this;
	}

	public RemoteAsyncDnsClient withDnsServerAddress(InetSocketAddress address) {
		this.dnsServerAddress = address;
		return this;
	}

	public RemoteAsyncDnsClient withDnsServerAddress(InetAddress address) {
		this.dnsServerAddress = new InetSocketAddress(address, DNS_SERVER_PORT);
		return this;
	}

	public RemoteAsyncDnsClient withInspector(Inspector inspector) {
		this.inspector = inspector;
		return this;
	}

	public RemoteAsyncDnsClient setSocketInspector(AsyncUdpSocketNio.Inspector socketInspector) {
		this.socketInspector = socketInspector;
		return this;
	}

	// endregion

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void close() {
		if (CHECK) checkState(eventloop.inEventloopThread());
		if (socket == null) {
			return;
		}
		socket.close();
		socket = null;
		CloseException closeException = new CloseException();
		transactions.values().forEach(s -> s.setException(closeException));
	}

	private Promise<AsyncUdpSocket> getSocket() {
		AsyncUdpSocket socket = this.socket;
		if (socket != null) {
			return Promise.of(socket);
		}
		try {
			logger.trace("Incoming query, opening UDP socket");
			DatagramChannel channel = Eventloop.createDatagramChannel(datagramSocketSettings, null, dnsServerAddress);
			return AsyncUdpSocketNio.connect(eventloop, channel)
					.map(s -> {
						if (socketInspector != null) {
							socketInspector.onCreate(s);
							s.setInspector(socketInspector);
						}
						return this.socket = s;
					});
		} catch (IOException e) {
			logger.error("UDP socket creation failed.", e);
			return Promise.ofException(e);
		}
	}

	@Override
	public Promise<DnsResponse> resolve(DnsQuery query) {
		if (CHECK) checkState(eventloop.inEventloopThread());
		DnsResponse fromQuery = AsyncDnsClient.resolveFromQuery(query);
		if (fromQuery != null) {
			logger.trace("{} already contained an IP address within itself", query);
			return Promise.of(fromQuery);
		}
		// ignore the result because sooner or later it will be sent and just completed
		// here we use that transactions map because it easily could go completely out of order and we should be ok with that
		return getSocket()
				.then(socket -> {
					logger.trace("Resolving {} with DNS server {}", query, dnsServerAddress);

					DnsTransaction transaction = DnsTransaction.of(DnsProtocol.generateTransactionId(), query);
					SettablePromise<DnsResponse> promise = new SettablePromise<>();

					transactions.put(transaction, promise);

					ByteBuf payload = DnsProtocol.createDnsQueryPayload(transaction);
					if (inspector != null) {
						inspector.onDnsQuery(query, payload);
					}

					// ignore the result because soon or later it will be sent and just completed
					socket.send(UdpPacket.of(payload, dnsServerAddress));

					// here we use that transactions map because it easily could go completely out of order and we should be ok with that
					socket.receive()
							.whenResult(packet -> {
								try {
									DnsResponse queryResult = DnsProtocol.readDnsResponse(packet.getBuf());
									SettablePromise<DnsResponse> cb = transactions.remove(queryResult.getTransaction());
									if (cb == null) {
										logger.warn("Received a DNS response that had no listener (most likely because it timed out) : {}", queryResult);
										return;
									}
									if (queryResult.isSuccessful()) {
										cb.set(queryResult);
									} else {
										cb.setException(new DnsQueryException(queryResult));
									}
									closeIfDone();
								} catch (MalformedDataException e) {
									logger.warn("Received a UDP packet than cannot be decoded as a DNS server response.", e);
								} finally {
									packet.recycle();
								}
							});

					return timeout(timeout, promise)
							.thenEx((queryResult, e) -> {
								if (e == null) {
									if (inspector != null) {
										inspector.onDnsQueryResult(query, queryResult);
									}
									logger.trace("DNS query {} resolved as {}", query, queryResult.getRecord());
									return Promise.of(queryResult);
								}
								if (e instanceof AsyncTimeoutException) {
									if (inspector != null) {
										inspector.onDnsQueryExpiration(query);
									}
									logger.trace("{} timed out", query);
									e = new DnsQueryException(DnsResponse.ofFailure(transaction, DnsProtocol.ResponseErrorCode.TIMED_OUT));
									transactions.remove(transaction);
									closeIfDone();
								} else if (inspector != null) {
									inspector.onDnsQueryError(query, e);
								}
								return Promise.ofException(e);
							});
				});
	}

	private void closeIfDone() {
		if (!transactions.isEmpty()) {
			return;
		}
		logger.trace("All queries are completed, closing UDP socket");
		close(); // transactions is empty so no loops here
	}

	// region JMX
	public interface Inspector extends BaseInspector<Inspector> {
		void onDnsQuery(DnsQuery query, ByteBuf payload);

		void onDnsQueryResult(DnsQuery query, DnsResponse result);

		void onDnsQueryError(DnsQuery query, Throwable e);

		void onDnsQueryExpiration(DnsQuery query);
	}

	public static class JmxInspector extends AbstractInspector<Inspector> implements Inspector {
		private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

		private final EventStats queries = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats failedQueries = EventStats.create(SMOOTHING_WINDOW);
		private final EventStats expirations = EventStats.create(SMOOTHING_WINDOW);

		@Override
		public void onDnsQuery(DnsQuery query, ByteBuf payload) {
			queries.recordEvent();
		}

		@Override
		public void onDnsQueryResult(DnsQuery query, DnsResponse result) {
			if (!result.isSuccessful()) {
				failedQueries.recordEvent();
			}
		}

		@Override
		public void onDnsQueryError(DnsQuery query, Throwable e) {
			failedQueries.recordEvent();
		}

		@Override
		public void onDnsQueryExpiration(DnsQuery query) {
			expirations.recordEvent();
		}

		@JmxAttribute
		public EventStats getQueries() {
			return queries;
		}

		@JmxAttribute
		public EventStats getFailedQueries() {
			return failedQueries;
		}

		@JmxAttribute
		public EventStats getExpirations() {
			return expirations;
		}
	}
	// endregion

	@JmxAttribute
	@Nullable
	public AsyncUdpSocketNio.JmxInspector getSocketStats() {
		return BaseInspector.lookup(socketInspector, AsyncUdpSocketNio.JmxInspector.class);
	}

	@JmxAttribute(name = "")
	@Nullable
	public JmxInspector getStats() {
		return BaseInspector.lookup(inspector, JmxInspector.class);
	}
}
