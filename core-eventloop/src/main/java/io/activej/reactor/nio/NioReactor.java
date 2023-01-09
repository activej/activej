package io.activej.reactor.nio;

import io.activej.async.callback.Callback;
import io.activej.reactor.NioReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.net.DatagramSocketSettings;
import io.activej.reactor.net.ServerSocketSettings;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.time.Duration;
import java.util.function.Consumer;

public interface NioReactor extends Reactor, NioReactive {
	Logger logger = LoggerFactory.getLogger(NioReactor.class);

	/**
	 * Registers new UDP connection.
	 *
	 * @param bindAddress address for binding DatagramSocket for this connection.
	 * @return DatagramSocket of this connection
	 * @throws IOException if an I/O error occurs on opening DatagramChannel
	 */
	static DatagramChannel createDatagramChannel(DatagramSocketSettings datagramSocketSettings,
			@Nullable InetSocketAddress bindAddress,
			@Nullable InetSocketAddress connectAddress) throws IOException {
		DatagramChannel datagramChannel = null;
		try {
			datagramChannel = DatagramChannel.open();
			datagramSocketSettings.applySettings(datagramChannel);
			datagramChannel.configureBlocking(false);
			datagramChannel.bind(bindAddress);
			if (connectAddress != null) {
				datagramChannel.connect(connectAddress);
			}
			return datagramChannel;
		} catch (IOException e) {
			if (datagramChannel != null) {
				try {
					datagramChannel.close();
				} catch (Exception nested) {
					logger.error("Failed closing datagram channel after I/O error", nested);
					e.addSuppressed(nested);
				}
			}
			throw e;
		}
	}

	@Override
	default NioReactor getReactor() {
		return this;
	}

	@Nullable Selector getSelector();

	Selector ensureSelector();

	void closeChannel(@Nullable SelectableChannel channel, @Nullable SelectionKey key);

	ServerSocketChannel listen(@Nullable InetSocketAddress address, ServerSocketSettings serverSocketSettings, Consumer<SocketChannel> acceptCallback) throws IOException;

	void connect(SocketAddress address, Callback<SocketChannel> cb);

	void connect(SocketAddress address, @Nullable Duration timeout, Callback<SocketChannel> cb);

	void connect(SocketAddress address, long timeout, Callback<SocketChannel> cb);
}
