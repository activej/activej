package datastream;

import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.processor.transformer.StreamTransformers;
import io.activej.eventloop.Eventloop;
import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.net.socket.tcp.TcpSocket;
import io.activej.reactor.net.ServerSocketSettings;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;
import static io.activej.serializer.BinarySerializers.UTF8_SERIALIZER;

/**
 * Demonstrates server ("Server #2" from the picture) which receives some data from clients,
 * computes it in a certain way and sends back the result.
 */
//[START EXAMPLE]
public final class TcpServerExample {

	public static void main(String[] args) throws IOException {
		Eventloop eventloop = Eventloop.create();

		InetSocketAddress address = new InetSocketAddress("localhost", TcpClientExample.PORT);
		ServerSocketSettings socketSettings = ServerSocketSettings.defaultInstance();
		eventloop.listen(address, socketSettings,
			channel -> {
				ITcpSocket socket;

				try {
					socket = TcpSocket.wrapChannel(eventloop, channel, null);
					System.out.println("Client connected: " + channel.getRemoteAddress());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				ChannelSuppliers.ofSocket(socket)
					.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
					.transformWith(StreamTransformers.mapper(x -> x + " times 10 = " + x * 10))
					.transformWith(ChannelSerializer.create(UTF8_SERIALIZER))
					.streamTo(ChannelConsumers.ofSocket(socket));
			});

		System.out.println("Connect to the server by running datastream.TcpClientExample");

		eventloop.run();
	}
}
//[END EXAMPLE]
