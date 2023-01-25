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

import io.activej.async.exception.AsyncCloseException;
import io.activej.async.exception.AsyncTimeoutException;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.inspector.AbstractInspector;
import io.activej.common.inspector.BaseInspector;
import io.activej.dns.protocol.*;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.EventStats;
import io.activej.net.socket.udp.IUdpSocket;
import io.activej.net.socket.udp.UdpPacket;
import io.activej.net.socket.udp.UdpSocket;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.AbstractNioReactive;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import io.activej.reactor.net.DatagramSocketSettings;
import io.activej.reactor.nio.NioReactor;
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

import static io.activej.promise.Promises.timeout;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * Implementation of {@link IDnsClient} that asynchronously
 * connects to some <i>real</i> DNS server and gets the response from it.
 */
public final class DnsClient extends AbstractNioReactive
		implements IDnsClient, ReactiveJmxBeanWithStats {
	private final Logger logger = LoggerFactory.getLogger(DnsClient.class);

	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(3);
	private static final int DNS_SERVER_PORT = 53;
	public static final InetSocketAddress GOOGLE_PUBLIC_DNS = new InetSocketAddress("8.8.8.8", DNS_SERVER_PORT);
	public static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);

	private final Map<DnsTransaction, SettablePromise<DnsResponse>> transactions = new HashMap<>();

	private DatagramSocketSettings datagramSocketSettings = DatagramSocketSettings.create();
	private InetSocketAddress dnsServerAddress = GOOGLE_PUBLIC_DNS;
	private Duration timeout = DEFAULT_TIMEOUT;

	private @Nullable IUdpSocket socket;

	private @Nullable UdpSocket.Inspector socketInspector;
	private @Nullable Inspector inspector;

	private DnsClient(NioReactor reactor) {
		super(reactor);
	}

	public static DnsClient create(NioReactor reactor) {
		return builder(reactor).build();
	}

	public static Builder builder(NioReactor reactor) {
		return new DnsClient(reactor).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, DnsClient> {
		private Builder() {}

		public Builder withDatagramSocketSetting(DatagramSocketSettings setting) {
			checkNotBuilt(this);
			DnsClient.this.datagramSocketSettings = setting;
			return this;
		}

		public Builder withTimeout(Duration timeout) {
			checkNotBuilt(this);
			DnsClient.this.timeout = timeout;
			return this;
		}

		public Builder withDnsServerAddress(InetSocketAddress address) {
			checkNotBuilt(this);
			DnsClient.this.dnsServerAddress = address;
			return this;
		}

		public Builder withDnsServerAddress(InetAddress address) {
			checkNotBuilt(this);
			DnsClient.this.dnsServerAddress = new InetSocketAddress(address, DNS_SERVER_PORT);
			return this;
		}

		public Builder withInspector(Inspector inspector) {
			checkNotBuilt(this);
			DnsClient.this.inspector = inspector;
			return this;
		}

		public Builder withSocketInspector(UdpSocket.Inspector socketInspector) {
			checkNotBuilt(this);
			DnsClient.this.socketInspector = socketInspector;
			return this;
		}

		@Override
		protected DnsClient doBuild() {
			return DnsClient.this;
		}
	}

	@Override
	public void close() {
		checkInReactorThread(this);
		if (socket == null) {
			return;
		}
		socket.close();
		socket = null;
		AsyncCloseException closeException = new AsyncCloseException();
		transactions.values().forEach(s -> s.setException(closeException));
	}

	private Promise<IUdpSocket> getSocket() {
		IUdpSocket socket = this.socket;
		if (socket != null) {
			return Promise.of(socket);
		}
		try {
			logger.trace("Incoming query, opening UDP socket");
			DatagramChannel channel = NioReactor.createDatagramChannel(datagramSocketSettings, null, dnsServerAddress);
			return UdpSocket.connect(reactor, channel)
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
		checkInReactorThread(this);
		DnsResponse fromQuery = IDnsClient.resolveFromQuery(query);
		if (fromQuery != null) {
			logger.trace("{} already contained an IP address within itself", query);
			return Promise.of(fromQuery);
		}

		int labelSize = 0;
		String domainName = query.getDomainName();
		for (int i = 0; i < domainName.length(); i++) {
			if (domainName.charAt(i) == '.') {
				labelSize = 0;
			} else if (++labelSize > 63) {
				// Domain Implementation and Specification - Size limits
				// https://www.ietf.org/rfc/rfc1035.html#section-2.3.4
				return Promise.ofException(new IllegalArgumentException("Label size cannot exceed 63 octets"));
			}
		}

		// ignore the result because sooner or later it will be sent and just completed
		// here we use that transactions map because it easily could go completely out of order, and we should be ok with that
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

					// ignore the result because soon, or later it will be sent and just completed
					socket.send(UdpPacket.of(payload, dnsServerAddress));

					// here we use that transactions map because it easily could go completely out of order, and we should be ok with that
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
							.then(
									queryResult -> {
										if (inspector != null) {
											inspector.onDnsQueryResult(query, queryResult);
										}
										logger.trace("DNS query {} resolved as {}", query, queryResult.getRecord());
										return Promise.of(queryResult);
									},
									e -> {
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
		close(); // transactions are empty so no loops here
	}

	// region JMX
	public interface Inspector extends BaseInspector<Inspector> {
		void onDnsQuery(DnsQuery query, ByteBuf payload);

		void onDnsQueryResult(DnsQuery query, DnsResponse result);

		void onDnsQueryError(DnsQuery query, Exception e);

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
		public void onDnsQueryError(DnsQuery query, Exception e) {
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
	public @Nullable UdpSocket.JmxInspector getSocketStats() {
		return BaseInspector.lookup(socketInspector, UdpSocket.JmxInspector.class);
	}

	@JmxAttribute(name = "")
	public @Nullable JmxInspector getStats() {
		return BaseInspector.lookup(inspector, JmxInspector.class);
	}
}
