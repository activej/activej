package udp;

import io.activej.bytebuf.ByteBufStrings;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.udp.UdpPacket;
import io.activej.net.socket.udp.UdpSocket_Reactive;
import io.activej.reactor.net.DatagramSocketSettings;
import io.activej.reactor.nio.NioReactor;

import java.nio.channels.DatagramChannel;

import static java.nio.charset.StandardCharsets.UTF_8;
import static udp.UdpPongServerExample.SERVER_ADDRESS;

/**
 * Example of creating a simple UDP ping client.
 * <p>
 * Before launching the client make sure that {@link UdpPongServerExample} is running
 */
public final class UdpPingClientExample {

	//[START REGION_1]
	public static void main(String[] args) throws Exception {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		DatagramSocketSettings socketSettings = DatagramSocketSettings.create();
		DatagramChannel clientDatagramChannel = NioReactor.createDatagramChannel(socketSettings, null, null);

		UdpSocket_Reactive.connect(eventloop, clientDatagramChannel)
				.whenResult(socket -> {
					System.out.println("Sending PING to UDP server " + SERVER_ADDRESS);

					socket.send(UdpPacket.of(ByteBufStrings.wrapUtf8("PING"), SERVER_ADDRESS))
							.then(socket::receive)
							.whenResult(packet -> System.out.println("Received message: " + packet.getBuf().asString(UTF_8)))
							.whenComplete(socket::close);
				});

		eventloop.run();
	}
	//[END REGION_1]

}
