package io.activej.net;

import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.datastream.csp.ChannelDeserializer;
import io.activej.datastream.csp.ChannelSerializer;
import io.activej.datastream.processor.StreamFilter;
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
		return SimpleServer.create(reactor,
				socket -> ChannelSupplier.ofSocket(socket)
						.transformWith(ChannelDeserializer.create(INT_SERIALIZER))
						.transformWith(StreamFilter.mapper(Function.identity()))
						.transformWith(ChannelSerializer.create(INT_SERIALIZER))
						.streamTo(ChannelConsumer.ofSocket(socket)))
				.withListenPort(9001);
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
