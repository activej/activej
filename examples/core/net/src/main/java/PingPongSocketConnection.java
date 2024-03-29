import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.activej.csp.binary.decoder.ByteBufsDecoders;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.net.SimpleServer;
import io.activej.net.socket.tcp.TcpSocket;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.activej.bytebuf.ByteBufStrings.wrapAscii;
import static io.activej.promise.Promises.loop;
import static io.activej.promise.Promises.repeat;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class PingPongSocketConnection {
	private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);
	private static final int ITERATIONS = 3;
	private static final String REQUEST_MSG = "PING";
	private static final String RESPONSE_MSG = "PONG";

	private static final ByteBufsDecoder<String> DECODER = ByteBufsDecoders.ofFixedSize(4)
		.andThen(buf -> buf.asString(UTF_8));

	//[START REGION_1]
	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.builder()
			.withCurrentThread()
			.build();

		SimpleServer server = SimpleServer.builder(
				eventloop,
				socket -> {
					BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket));
					repeat(() ->
						bufsSupplier.decode(DECODER)
							.whenResult(x -> System.out.println(x))
							.then(() -> socket.write(wrapAscii(RESPONSE_MSG)))
							.map($ -> true))
						.whenComplete(socket::close);
				})
			.withListenAddress(ADDRESS)
			.withAcceptOnce()
			.build();

		server.listen();

		TcpSocket.connect(eventloop, ADDRESS)
			.whenResult(socket -> {
				BinaryChannelSupplier bufsSupplier = BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket));
				loop(0,
					i -> i < ITERATIONS,
					i -> socket.write(wrapAscii(REQUEST_MSG))
						.then(() -> bufsSupplier.decode(DECODER)
							.whenResult(x -> System.out.println(x))
							.map($2 -> i + 1)))
					.whenComplete(socket::close);
			})
			.whenException(e -> {throw new RuntimeException(e);});

		eventloop.run();
	}
	//[END REGION_1]
}
