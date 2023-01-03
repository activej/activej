package csp;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.binary.BinaryChannelSupplier;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.ReactiveTcpSocket;
import io.activej.net.socket.tcp.TcpSocket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Scanner;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.reactor.Reactor.getCurrentReactor;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example of creating a simple TCP console client.
 * By default, this client connects to the same address as the server in the previous example.
 */
public final class TcpClientExample {
	private final Eventloop eventloop = Eventloop.create();

	/* Thread, which reads user input */
	private void startCommandLineInterface(TcpSocket socket) {
		Thread thread = new Thread(() -> {
			Scanner scanIn = new Scanner(System.in);
			while (true) {
				String line = scanIn.nextLine();
				if (line.isEmpty()) {
					break;
				}
				ByteBuf buf = ByteBuf.wrapForReading(encodeAscii(line + "\r\n"));
				eventloop.execute(() -> socket.write(buf));
			}
			eventloop.execute(socket::close);
		});
		thread.start();
	}

	//[START REGION_1]
	private void run() {
		System.out.println("Connecting to server at localhost (port 9922)...");
		eventloop.connect(new InetSocketAddress("localhost", 9922), (socketChannel, e) -> {
			if (e == null) {
				System.out.println("Connected to server, enter some text and send it by pressing 'Enter'.");
				TcpSocket socket;
				try {
					socket = ReactiveTcpSocket.wrapChannel(getCurrentReactor(), socketChannel, null);
				} catch (IOException ioException) {
					throw new RuntimeException(ioException);
				}

				BinaryChannelSupplier.of(ChannelSupplier.ofSocket(socket))
						.decodeStream(ByteBufsDecoder.ofCrlfTerminatedBytes())
						.streamTo(ChannelConsumer.ofConsumer(buf -> System.out.println(buf.asString(UTF_8))));

				startCommandLineInterface(socket);
			} else {
				System.out.printf("Could not connect to server, make sure it is started: %s%n", e);
			}
		});
		eventloop.run();
	}

	public static void main(String[] args) {
		new TcpClientExample().run();
	}
	//[END REGION_1]
}
