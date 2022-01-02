package io.activej.net;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.ref.RefLong;
import io.activej.eventloop.net.SocketSettings;
import io.activej.net.socket.tcp.AsyncTcpSocketNio;
import io.activej.promise.Promises;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static io.activej.eventloop.Eventloop.getCurrentEventloop;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public final class AbstractServerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testTimeouts() throws IOException {
		String message = "Hello!";
		InetSocketAddress address = new InetSocketAddress("localhost", getFreePort());
		SocketSettings settings = SocketSettings.create().withImplReadTimeout(Duration.ofMillis(100000L)).withImplWriteTimeout(Duration.ofMillis(100000L));

		RefLong delay = new RefLong(5);
		SimpleServer.create(socket -> Promises.repeat(
						() -> socket.read()
								.whenResult(buf ->
										getCurrentEventloop().delay(delay.inc(),
												() -> socket.write(buf)
														.whenComplete(() -> {
															if (buf == null) {
																socket.close();
															}
														})))
								.map(Objects::nonNull)))
				.withSocketSettings(settings)
				.withListenAddress(address)
				.withAcceptOnce()
				.listen();

		ByteBuf response = sendMessage(address, message);
		assertEquals(message, response.asString(UTF_8));
	}

	@Test
	public void testBoundAddressPortZero() throws IOException {
		InetSocketAddress address = new InetSocketAddress("localhost", 0);
		AbstractServer<?> server = createServer(address);

		server.listen();

		List<InetSocketAddress> boundAddresses = server.getBoundAddresses();
		assertEquals(1, boundAddresses.size());
		assertNotEquals(server.getListenAddresses(), boundAddresses);

		InetSocketAddress boundAddress = boundAddresses.get(0);
		assertNotEquals(0, boundAddress.getPort());

		String message = "Hello!";
		ByteBuf response = sendMessage(boundAddress, message);
		assertEquals(message, response.asString(UTF_8));
	}

	@Test
	public void testBoundAddressPortNonZero() throws IOException {
		InetSocketAddress address = new InetSocketAddress("localhost", getFreePort());
		AbstractServer<?> server = createServer(address);

		server.listen();

		List<InetSocketAddress> boundAddresses = server.getBoundAddresses();
		assertEquals(1, boundAddresses.size());
		assertEquals(server.getListenAddresses(), boundAddresses);

		String message = "Hello!";
		ByteBuf response = sendMessage(boundAddresses.get(0), message);
		assertEquals(message, response.asString(UTF_8));
	}

	private static AbstractServer<?> createServer(InetSocketAddress address) {
		return SimpleServer.create(
						socket -> Promises.repeat(
								() -> socket.read().whenResult(
												buf -> socket.write(buf).whenComplete(() -> {
													if (buf == null) {
														socket.close();
													}
												})
										)
										.map(Objects::nonNull)))
				.withListenAddress(address)
				.withAcceptOnce();
	}

	private static ByteBuf sendMessage(InetSocketAddress address, String message) {
		return await(AsyncTcpSocketNio.connect(address)
				.then(socket ->
						socket.write(ByteBufStrings.wrapAscii(message))
								.then(() -> socket.write(null))
								.then(() -> {
									ByteBufs bufs = new ByteBufs();
									return Promises.<ByteBuf>until(null,
													$2 -> socket.read()
															.whenResult(Objects::nonNull, bufs::add),
													Objects::isNull)
											.map($2 -> bufs.takeRemaining());
								})
								.whenComplete(socket::close)));
	}
}
