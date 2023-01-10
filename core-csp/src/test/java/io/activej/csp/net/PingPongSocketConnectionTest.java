package io.activej.csp.net;

import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.net.SimpleServer;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static io.activej.promise.Promises.loop;
import static io.activej.promise.TestUtils.await;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static io.activej.test.TestUtils.assertCompleteFn;
import static io.activej.test.TestUtils.getFreePort;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public final class PingPongSocketConnectionTest {
	private static final int ITERATIONS = 100;

	private static final String REQUEST_MSG = "PING";
	private static final String RESPONSE_MSG = "PONG";

	private static final ByteBufsDecoder<String> DECODER = ByteBufsDecoder.ofFixedSize(4)
			.andThen(buf -> buf.asString(UTF_8));

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private InetSocketAddress address;

	@Before
	public void setUp() {
		address = new InetSocketAddress("localhost", getFreePort());
	}

	@Test
	public void test() throws IOException {
		SimpleServer.create(
						socket -> {
							BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket));
							loop(ITERATIONS,
									i -> i != 0,
									i -> bufsSupplier.decode(DECODER)
											.whenResult(res -> assertEquals(REQUEST_MSG, res))
											.then(() -> socket.write(wrapAscii(RESPONSE_MSG)))
											.map($ -> i - 1))
									.whenComplete(socket::close)
									.whenComplete(assertCompleteFn());
						})
				.withListenAddress(address)
				.withAcceptOnce()
				.listen();

		await(TcpSocket.connect(getCurrentReactor(), address)
				.then(socket -> {
					BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket));
					return loop(ITERATIONS,
							i -> i != 0,
							i -> socket.write(wrapAscii(REQUEST_MSG))
									.then(() -> bufsSupplier.decode(DECODER))
									.whenResult(res -> assertEquals(RESPONSE_MSG, res))
									.map($ -> i - 1))
							.whenResult(socket::close);
				}));
	}
}
