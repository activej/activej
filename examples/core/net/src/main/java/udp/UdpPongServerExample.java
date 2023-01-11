package udp;

import io.activej.bytebuf.ByteBufStrings;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.udp.UdpPacket;
import io.activej.net.socket.udp.UdpSocket_Reactive;
import io.activej.promise.Promises;
import io.activej.reactor.net.DatagramSocketSettings;
import io.activej.reactor.nio.NioReactor;

import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Example of creating a simple echo UDP server.
 * <p>
 * After launching the server you can launch {@link UdpPingClientExample}
 */
public final class UdpPongServerExample {
	public static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("localhost", 45555);

	//[START REGION_1]
	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		DatagramSocketSettings socketSettings = DatagramSocketSettings.create();
		DatagramChannel serverDatagramChannel = NioReactor.createDatagramChannel(socketSettings, SERVER_ADDRESS, null);

		UdpSocket_Reactive.connect(eventloop, serverDatagramChannel)
				.whenResult(() -> {
					System.out.println("UDP server socket is up");
					System.out.println("You can run UdpPingClientExample");
				})
				.whenResult(socket ->
						Promises.repeat(() -> socket.receive()
								.then(packet -> {
									String message = packet.getBuf().asString(UTF_8);
									InetSocketAddress clientAddress = packet.getSocketAddress();

									System.out.println("Received message: " + message + " from " + clientAddress);
									System.out.println("Replying with PONG");

									return socket.send(UdpPacket.of(ByteBufStrings.wrapUtf8("PONG"), clientAddress));
								})
								.map($ -> true)
						));

		eventloop.run();
	}
	//[END REGION_1]
}


