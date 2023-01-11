package io.activej.net.socket.udp;

import io.activej.bytebuf.ByteBuf;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.net.DatagramSocketSettings;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;

import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.assertCompleteFn;
import static org.junit.Assert.assertArrayEquals;

public final class UdpSocket_Reactive_Test {
	private static final InetSocketAddress SERVER_ADDRESS = new InetSocketAddress("localhost", 45555);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private final byte[] bytesToSend = {-127, 100, 0, 5, 11, 13, 17, 99};

	@Test
	public void testEchoUdpServer() throws IOException {
		DatagramChannel serverDatagramChannel = NioReactor.createDatagramChannel(DatagramSocketSettings.create(), SERVER_ADDRESS, null);

		UdpSocket_Reactive.connect(Reactor.getCurrentReactor(), serverDatagramChannel)
				.then(serverSocket -> serverSocket.receive()
						.then(serverSocket::send)
						.whenComplete(serverSocket::close))
				.whenComplete(assertCompleteFn());

		DatagramChannel clientDatagramChannel = NioReactor.createDatagramChannel(DatagramSocketSettings.create(), null, null);

		Promise<UdpSocket_Reactive> promise = UdpSocket_Reactive.connect(Reactor.getCurrentReactor(), clientDatagramChannel)
				.whenComplete(assertCompleteFn(clientSocket -> {

					clientSocket.send(UdpPacket.of(ByteBuf.wrapForReading(bytesToSend), SERVER_ADDRESS))
							.whenComplete(assertCompleteFn());

					clientSocket.receive()
							.whenComplete(clientSocket::close)
							.whenComplete(assertCompleteFn(packet -> {
								byte[] message = packet.getBuf().asArray();

								assertArrayEquals(bytesToSend, message);

								System.out.println("message = " + Arrays.toString(message));
							}));
				}));

		await(promise);
	}

}
