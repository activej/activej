package io.activej.net.socket.tcp;

import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.ServerSocketSettings;
import io.activej.eventloop.net.SocketSettings;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.activej.eventloop.net.ServerSocketSettings.DEFAULT_BACKLOG;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertComplete;
import static io.activej.test.TestUtils.getFreePort;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AsyncTcpSocketNioTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testIdleTimeouts() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		SocketSettings socketSettings = SocketSettings.createDefault().withImplIdleTimeout(Duration.ofMillis(10));
		int[] ports = new int[5];
		List<ServerSocketChannel> channels = new ArrayList<>();

		for (int i = 0; i < ports.length; i++) {
			int freePort = getFreePort();
			ports[i] = freePort;
			channels.add(eventloop.listen(new InetSocketAddress(freePort), ServerSocketSettings.create(DEFAULT_BACKLOG), $ -> {}));
		}

		await(Promises.toList(Arrays.stream(ports)
				.mapToObj(port -> AsyncTcpSocketNio.connect(new InetSocketAddress(port), 0, socketSettings)))
				.whenComplete(assertComplete(sockets -> sockets.forEach(socket -> assertFalse(socket.isClosed()))))
				.then(sockets -> Promises.delay(Duration.ofMillis(20), sockets))
				.whenComplete(assertComplete(sockets -> sockets.forEach(socket -> assertTrue(socket.isClosed()))))
				.whenComplete(() -> channels.forEach(serverSocketChannel -> {
					try {
						serverSocketChannel.close();
					} catch (IOException e) {
						throw new AssertionError();					}
				})));
	}
}
