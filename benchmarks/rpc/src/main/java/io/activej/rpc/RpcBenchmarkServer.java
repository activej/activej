package io.activej.rpc;

import io.activej.common.MemSize;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.SerializerFactory;
import io.activej.service.ServiceGraphModule;

import java.util.List;

import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.config.converter.ConfigConverters.ofMemSize;
import static io.activej.inject.module.Modules.combine;
import static io.activej.launchers.initializers.ConfigConverters.ofFrameFormat;

public class RpcBenchmarkServer extends Launcher {
	private static final int SERVICE_PORT = 25565;

	@Provides
	@Named("server")
	NioReactor reactorServer() {
		return Eventloop.create();
	}

	@Provides
	@Eager
	public RpcServer rpcServer(@Named("server") NioReactor reactor, Config config) {
		return RpcServer.builder(reactor)
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.defaultPacketSize", MemSize.kilobytes(256)),
						config.get(ofFrameFormat(), "rpc.compression", null))
				.withListenPort(config.get(ofInteger(), "rpc.server.port"))
				.withSerializer(SerializerFactory.builder()
						.withSubclasses(RpcMessage.SUBCLASSES_ID, List.of(Integer.class))
						.build()
						.create(RpcMessage.class))
				.withHandler(Integer.class, req -> Promise.of(req * 2))
				.build();

	}

	@Provides
	Config config() {
		return Config.create()
				.with("rpc.server.port", "" + SERVICE_PORT)
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				ConfigModule.builder()
						.withEffectiveConfigLogger()
						.build());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		RpcBenchmarkServer benchmark = new RpcBenchmarkServer();
		benchmark.launch(args);
	}
}
