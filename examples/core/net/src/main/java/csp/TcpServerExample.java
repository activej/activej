package csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.eventloop.Eventloop;
import io.activej.net.SimpleServer;

import static io.activej.bytebuf.ByteBufStrings.CR;
import static io.activej.bytebuf.ByteBufStrings.LF;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example of creating a simple echo TCP server.
 */
public final class TcpServerExample {
	private static final int PORT = 9922;
	private static final byte[] CRLF = {CR, LF};

	/* Run server in an event loop. */
	//[START REGION_1]
	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withCurrentThread();

		SimpleServer server = SimpleServer.create(socket ->
						BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket))
								.decodeStream(ByteBufsDecoder.ofCrlfTerminatedBytes())
								.peek(buf -> System.out.println("client:" + buf.getString(UTF_8)))
								.map(buf -> {
									ByteBuf serverBuf = ByteBufStrings.wrapUtf8("Server> ");
									return ByteBufPool.append(serverBuf, buf);
								})
								.map(buf -> ByteBufPool.append(buf, CRLF))
								.streamTo(ChannelConsumer.ofSocket(socket)))
				.withListenPort(PORT);

		server.listen();

		System.out.println("Server is running");
		System.out.println("You can connect from telnet with command: telnet localhost 9922 or by running csp.TcpClientExample");

		eventloop.run();
	}
	//[END REGION_1]
}


