package io.activej.net;

import io.activej.bytebuf.ByteBuf;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.net.DatagramSocketSettings;
import io.activej.promise.Promise;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;

import static io.activej.eventloop.Eventloop.createDatagramChannel;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertComplete;
import static org.junit.Assert.assertArrayEquals;

public final class UdpSocketHandlerTest {
	private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("localhost", 45555);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final byte[] bytesToSend = {-127, 100, 0, 5, 11, 13, 17, 99};

	@Test
	public void testEchoUdpServer() throws IOException {
		DatagramChannel serverDatagramChannel = createDatagramChannel(DatagramSocketSettings.create(), SERVER_ADDRESS, null);

		AsyncUdpSocketNio.connect(Eventloop.getCurrentEventloop(), serverDatagramChannel)
				.then(serverSocket -> serverSocket.receive()
						.then(serverSocket::send)
						.whenComplete(serverSocket::close))
				.whenComplete(assertComplete());

		DatagramChannel clientDatagramChannel = createDatagramChannel(DatagramSocketSettings.create(), null, null);

		Promise<AsyncUdpSocketNio> promise = AsyncUdpSocketNio.connect(Eventloop.getCurrentEventloop(), clientDatagramChannel)
				.whenComplete(assertComplete(clientSocket -> {

					clientSocket.send(UdpPacket.of(ByteBuf.wrapForReading(bytesToSend), SERVER_ADDRESS))
							.whenComplete(assertComplete());

					clientSocket.receive()
							.whenComplete(clientSocket::close)
							.whenComplete(assertComplete(packet -> {
								byte[] message = packet.getBuf().asArray();

								assertArrayEquals(bytesToSend, message);

								System.out.println("message = " + Arrays.toString(message));
							}));
				}));

		await(promise);
	}

}
