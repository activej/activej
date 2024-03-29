package datastream;

import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.activej.common.exception.FatalErrorHandlers.rethrow;
import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

/**
 * Demonstrates client ("Server #1" from the picture) which sends some data to other server
 * and receives some computed result.
 * Before running, you should launch {@link TcpServerExample} first!
 */
//[START EXAMPLE]
public final class TcpClientExample {
	public static final int PORT = 9922;

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.builder()
			.withFatalErrorHandler(rethrow())
			.build();

		eventloop.connect(new InetSocketAddress("localhost", PORT), (socketChannel, e) -> {
			if (e == null) {
				ITcpSocket socket;
				try {
					socket = TcpSocket.wrapChannel(eventloop, socketChannel, null);
				} catch (IOException ioEx) {
					throw new RuntimeException(ioEx);
				}

				StreamSuppliers.ofValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
					.transformWith(ChannelSerializer.create(INT_SERIALIZER))
					.streamTo(ChannelConsumers.ofSocket(socket));

				ChannelSuppliers.ofSocket(socket)
					.transformWith(ChannelDeserializer.create(UTF8_SERIALIZER))
					.toList()
					.whenResult(list -> list.forEach(System.out::println));
			} else {
				System.out.printf("Could not connect to server, make sure it is started: %s%n", e);
			}
		});

		eventloop.run();
	}
}
//[END EXAMPLE]
