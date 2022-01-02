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

package io.activej.net;

import io.activej.common.Checks;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.inspector.BaseInspector;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanWithStats;
import io.activej.eventloop.net.ServerSocketSettings;
import io.activej.eventloop.net.SocketSettings;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.EventStats;
import io.activej.net.socket.tcp.AsyncTcpSocket;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.net.socket.tcp.AsyncTcpSocketNio.Inspector;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import static io.activej.common.Checks.checkState;
import static io.activej.eventloop.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.activej.net.socket.tcp.AsyncTcpSocketNio.wrapChannel;
import static io.activej.net.socket.tcp.AsyncTcpSocketSsl.wrapServerSocket;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * This is an implementation of {@link EventloopServer}.
 * It is a non-blocking server which works on top of the eventloop.
 * Thus, it runs in the eventloop thread, and all events are fired on that thread.
 * <p>
 * This is simply a higher-level wrapper around eventloop {@link Eventloop#listen} call.
 */
@SuppressWarnings("WeakerAccess, unused")
public abstract class AbstractServer<Self extends AbstractServer<Self>> implements EventloopServer, WorkerServer, WithInitializer<Self>, EventloopJmxBeanWithStats {
	protected Logger logger = getLogger(getClass());
	private static final boolean CHECK = Checks.isEnabled(AbstractServer.class);

	protected final @NotNull Eventloop eventloop;

	public static final ServerSocketSettings DEFAULT_SERVER_SOCKET_SETTINGS = ServerSocketSettings.create(DEFAULT_BACKLOG);
	public static final SocketSettings DEFAULT_SOCKET_SETTINGS = SocketSettings.createDefault();

	protected ServerSocketSettings serverSocketSettings = DEFAULT_SERVER_SOCKET_SETTINGS;
	protected SocketSettings socketSettings = DEFAULT_SOCKET_SETTINGS;

	protected boolean acceptOnce;

	@FunctionalInterface
	public interface AcceptFilter {
		boolean filterAccept(SocketChannel socketChannel, InetSocketAddress localAddress, InetAddress remoteAddress, boolean ssl);
	}

	private AcceptFilter acceptFilter;

	protected List<InetSocketAddress> listenAddresses = new ArrayList<>();

	// ssl
	private SSLContext sslContext;
	private Executor sslExecutor;
	protected List<InetSocketAddress> sslListenAddresses = new ArrayList<>();

	private boolean running = false;
	private List<ServerSocketChannel> serverSocketChannels;
	private List<ServerSocketChannel> sslServerSocketChannels;

	// jmx
	private static final Duration SMOOTHING_WINDOW = Duration.ofMinutes(1);

	AbstractServer<?> acceptServer = this;

	private @Nullable Inspector socketInspector;
	private @Nullable Inspector socketSslInspector;
	private final EventStats accepts = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats acceptsSsl = EventStats.create(SMOOTHING_WINDOW);
	private final EventStats filteredAccepts = EventStats.create(SMOOTHING_WINDOW);

	// region creators & builder methods
	protected AbstractServer(@NotNull Eventloop eventloop) {
		this.eventloop = eventloop;
	}

	@SuppressWarnings("unchecked")
	public final Self withAcceptFilter(AcceptFilter acceptFilter) {
		this.acceptFilter = acceptFilter;
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public final Self withServerSocketSettings(ServerSocketSettings serverSocketSettings) {
		this.serverSocketSettings = serverSocketSettings;
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public final Self withSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public final Self withListenAddresses(List<InetSocketAddress> addresses) {
		this.listenAddresses = addresses;
		return (Self) this;
	}

	public final Self withListenAddresses(InetSocketAddress... addresses) {
		return withListenAddresses(asList(addresses));
	}

	public final Self withListenAddress(InetSocketAddress address) {
		return withListenAddresses(singletonList(address));
	}

	public final Self withListenPort(int port) {
		return withListenAddress(new InetSocketAddress(port));
	}

	@SuppressWarnings("unchecked")
	public final Self withSslListenAddresses(SSLContext sslContext, Executor sslExecutor, List<InetSocketAddress> addresses) {
		this.sslContext = sslContext;
		this.sslExecutor = sslExecutor;
		this.sslListenAddresses = addresses;
		return (Self) this;
	}

	public final Self withSslListenAddresses(SSLContext sslContext, Executor sslExecutor, InetSocketAddress... addresses) {
		return withSslListenAddresses(sslContext, sslExecutor, asList(addresses));
	}

	public final Self withSslListenAddress(SSLContext sslContext, Executor sslExecutor, InetSocketAddress address) {
		return withSslListenAddresses(sslContext, sslExecutor, singletonList(address));
	}

	public final Self withSslListenPort(SSLContext sslContext, Executor sslExecutor, int port) {
		return withSslListenAddress(sslContext, sslExecutor, new InetSocketAddress(port));
	}

	public final Self withAcceptOnce() {
		return withAcceptOnce(true);
	}

	@SuppressWarnings("unchecked")
	public final Self withAcceptOnce(boolean acceptOnce) {
		this.acceptOnce = acceptOnce;
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public final Self withSocketInspector(Inspector socketInspector) {
		this.socketInspector = socketInspector;
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public final Self withSocketSslInspector(Inspector socketSslInspector) {
		this.socketSslInspector = socketSslInspector;
		return (Self) this;
	}

	@SuppressWarnings("unchecked")
	public final Self withLogger(Logger logger) {
		this.logger = logger;
		return (Self) this;
	}
	// endregion

	protected abstract void serve(AsyncTcpSocket socket, InetAddress remoteAddress);

	protected void onListen() {
	}

	protected void onClose(SettablePromise<Void> cb) {
		cb.set(null);
	}

	protected void onAccept(SocketChannel socketChannel, InetSocketAddress localAddress, InetAddress remoteAddress, boolean ssl) {
	}

	protected void onFilteredAccept(SocketChannel socketChannel, InetSocketAddress localAddress, InetAddress remoteAddress, boolean ssl) {
	}

	/**
	 * Begins listening asynchronously for incoming connections.
	 * Creates an {@link ServerSocketChannel} for each listening address and registers them in
	 * {@link Eventloop Eventloop} {@link java.nio.channels.Selector selector}.
	 * Eventloop then asynchronously listens for network events and dispatches them to their listeners (us).
	 */
	@Override
	public final void listen() throws IOException {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		if (running) {
			return;
		}
		running = true;
		onListen();
		if (listenAddresses != null && !listenAddresses.isEmpty()) {
			serverSocketChannels = listenAddresses(listenAddresses, false);
			if (logger.isInfoEnabled()) {
				logger.info("Listening on {}: {}", getBoundAddresses(serverSocketChannels), this);
			}
		}
		if (sslListenAddresses != null && !sslListenAddresses.isEmpty()) {
			sslServerSocketChannels = listenAddresses(sslListenAddresses, true);
			if (logger.isInfoEnabled()) {
				logger.info("Listening with SSL on {}: {}", getBoundAddresses(sslServerSocketChannels), this);
			}
		}
	}

	private List<ServerSocketChannel> listenAddresses(List<InetSocketAddress> addresses, boolean ssl) throws IOException {
		List<ServerSocketChannel> channels = new ArrayList<>(addresses.size());
		for (InetSocketAddress address : addresses) {
			try {
				channels.add(eventloop.listen(address, serverSocketSettings, channel -> doAccept(channel, address, ssl)));
			} catch (IOException e) {
				logger.error("Can't listen on [" + address + "]: " + this, e);
				close();
				throw e;
			}
		}
		return channels;
	}

	@Override
	public final Promise<?> close() {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Cannot close server from different thread");
		if (!running) return Promise.complete();
		running = false;
		closeServerSockets();
		return Promise.ofCallback(this::onClose)
				.whenResult($ -> logger.info("Server closed: {}", this))
				.whenException(e -> logger.error("Server closed exceptionally: " + this, e));
	}

	public final Future<?> closeFuture() {
		return eventloop.submit(this::close);
	}

	public final boolean isRunning() {
		return running;
	}

	protected void closeServerSockets() {
		closeServerSockets(serverSocketChannels);
		closeServerSockets(sslServerSocketChannels);
	}

	private void closeServerSockets(List<ServerSocketChannel> channels) {
		if (channels == null || channels.isEmpty()) {
			return;
		}
		for (Iterator<ServerSocketChannel> it = channels.iterator(); it.hasNext(); ) {
			ServerSocketChannel serverSocketChannel = it.next();
			if (serverSocketChannel == null) {
				continue;
			}
			eventloop.closeChannel(serverSocketChannel, serverSocketChannel.keyFor(eventloop.getSelector()));
			it.remove();
		}
	}

	protected WorkerServer getWorkerServer() {
		return this;
	}

	protected Inspector getSocketInspector(InetAddress remoteAddress, InetSocketAddress localAddress, boolean ssl) {
		return ssl ? socketSslInspector : socketInspector;
	}

	private void doAccept(SocketChannel channel, InetSocketAddress localAddress, boolean ssl) {
		InetSocketAddress remoteSocketAddress;
		try {
			remoteSocketAddress = (InetSocketAddress) channel.getRemoteAddress();
		} catch (IOException e) {
			eventloop.closeChannel(channel, null);
			return;
		}
		InetAddress remoteAddress = remoteSocketAddress.getAddress();

		if (acceptFilter != null && acceptFilter.filterAccept(channel, localAddress, remoteAddress, ssl)) {
			filteredAccepts.recordEvent();
			onFilteredAccept(channel, localAddress, remoteAddress, ssl);
			eventloop.closeChannel(channel, null);
			return;
		}

		WorkerServer workerServer = getWorkerServer();
		Eventloop workerServerEventloop = workerServer.getEventloop();

		if (workerServerEventloop == eventloop) {
			workerServer.doAccept(channel, localAddress, remoteSocketAddress, ssl, socketSettings);
		} else {
			if (logger.isTraceEnabled()) {
				logger.trace("received connection from [{}]{}: {}", remoteAddress, ssl ? " over SSL" : "", this);
			}
			accepts.recordEvent();
			if (ssl) acceptsSsl.recordEvent();
			onAccept(channel, localAddress, remoteAddress, ssl);
			workerServerEventloop.execute(() -> workerServer.doAccept(channel, localAddress, remoteSocketAddress, ssl, socketSettings));
		}

		if (acceptOnce) {
			closeServerSockets();
		}
	}

	@Override
	public final void doAccept(SocketChannel socketChannel, InetSocketAddress localAddress, InetSocketAddress remoteSocketAddress,
			boolean ssl, SocketSettings socketSettings) {
		if (CHECK) checkState(eventloop.inEventloopThread(), "Not in eventloop thread");
		accepts.recordEvent();
		if (ssl) acceptsSsl.recordEvent();
		InetAddress remoteAddress = remoteSocketAddress.getAddress();
		onAccept(socketChannel, localAddress, remoteAddress, ssl);
		AsyncTcpSocket asyncTcpSocket;
		try {
			AsyncTcpSocketNio socketNio = wrapChannel(eventloop, socketChannel, remoteSocketAddress, socketSettings);
			Inspector inspector = ssl ? socketSslInspector : socketInspector;
			if (inspector != null) {
				inspector.onConnect(socketNio);
				socketNio.setInspector(inspector);
			}
			asyncTcpSocket = socketNio;
		} catch (IOException e) {
			logger.warn("Failed to wrap channel {}", socketChannel, e);
			eventloop.closeChannel(socketChannel, null);
			return;
		}
		asyncTcpSocket = ssl ? wrapServerSocket(asyncTcpSocket, sslContext, sslExecutor) : asyncTcpSocket;
		serve(asyncTcpSocket, remoteAddress);
	}

	public ServerSocketSettings getServerSocketSettings() {
		return serverSocketSettings;
	}

	public List<InetSocketAddress> getListenAddresses() {
		return listenAddresses;
	}

	public List<InetSocketAddress> getSslListenAddresses() {
		return sslListenAddresses;
	}

	/**
	 * The IP socket addresses {@code this} server is bound to after it started
	 * listening. Use this method to get the actual port numbers in case any
	 * {@link #getListenAddresses() listen address} uses port {@literal 0}.
	 */
	public List<InetSocketAddress> getBoundAddresses() {
		return getBoundAddresses(serverSocketChannels);
	}

	/**
	 * The IP socket addresses {@code this} server is bound to with SSL after it
	 * started listening. Use this method to get the actual port numbers in case
	 * any {@link #getSslListenAddresses() SSL listen address} uses port {@literal
	 * 0}.
	 */
	public List<InetSocketAddress> getSslBoundAddresses() {
		return getBoundAddresses(sslServerSocketChannels);
	}

	private List<InetSocketAddress> getBoundAddresses(List<ServerSocketChannel> channels) {
		if (channels == null) {
			return emptyList();
		}
		return channels.stream()
				.map(ch -> {
					try {
						return (InetSocketAddress) ch.getLocalAddress();
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				})
				.collect(toList());
	}

	public SocketSettings getSocketSettings() {
		return socketSettings;
	}

	@Override
	public final @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@JmxAttribute(extraSubAttributes = "totalCount")
	public final @Nullable EventStats getAccepts() {
		return acceptServer.listenAddresses.isEmpty() ? null : accepts;
	}

	@JmxAttribute
	public final @Nullable EventStats getAcceptsSsl() {
		return acceptServer.sslListenAddresses.isEmpty() ? null : acceptsSsl;
	}

	@JmxAttribute
	public final @Nullable EventStats getFilteredAccepts() {
		return acceptFilter == null ? null : filteredAccepts;
	}

	@JmxAttribute
	public final @Nullable AsyncTcpSocketNio.JmxInspector getSocketStats() {
		return this instanceof PrimaryServer || acceptServer.listenAddresses.isEmpty() ? null :
				BaseInspector.lookup(socketInspector, AsyncTcpSocketNio.JmxInspector.class);
	}

	@JmxAttribute
	public final @Nullable AsyncTcpSocketNio.JmxInspector getSocketStatsSsl() {
		return this instanceof PrimaryServer || acceptServer.sslListenAddresses.isEmpty() ? null :
				BaseInspector.lookup(socketSslInspector, AsyncTcpSocketNio.JmxInspector.class);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + '{' +
				(listenAddresses.isEmpty() ? "" : "listenAddresses=" + listenAddresses) +
				(sslListenAddresses.isEmpty() ? "" : ", sslListenAddresses=" + sslListenAddresses) +
				(acceptOnce ? ", acceptOnce" : "") +
				'}';
	}

}
