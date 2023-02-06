package io.activej.net;

import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.processor.transformer.StreamTransformers;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.reactor.nio.NioReactor;
import io.activej.service.ServiceGraphModule;

import java.util.function.Function;

import static io.activej.serializer.BinarySerializers.INT_SERIALIZER;

public class TcpDataBenchmarkServer extends Launcher {
	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	@Eager
	SimpleServer server(NioReactor reactor) {
		return SimpleServer.builder(reactor,
						socket -> ChannelSuppliers.ofSocket(socket)
								.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
								.transformWith(StreamTransformers.mapper(Function.identity()))
								.transformWith(ChannelSerializer.create(INT_SERIALIZER))
								.streamTo(ChannelConsumers.ofSocket(socket)))
				.withListenPort(9001)
				.build();
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher server = new TcpDataBenchmarkServer();
		server.launch(args);
	}
}
