package advancedrpc;

import io.activej.eventloop.Eventloop;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.SerializerFactory;
import io.activej.worker.WorkerPool;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;

import java.util.List;

public class AdvancedRpcServerModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(new Key<WorkerPool.Instances<RpcServer>>() {});
	}

	private AdvancedRpcServerModule() {
	}

	public static AdvancedRpcServerModule create() {
		return new AdvancedRpcServerModule();
	}

	@Provides
	@Worker
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	Integer port(@WorkerId int workerId) {
		return 9000 + workerId;
	}

	@Provides
	@Worker
	RpcServer rpcServer(NioReactor reactor, Integer port) {
		return RpcServer.builder(reactor)
				.withSerializer(SerializerFactory.builder()
						.withSubclasses(RpcMessage.SUBCLASSES_ID, List.of(Integer.class))
						.build()
						.create(RpcMessage.class))
				.withHandler(Integer.class, in -> {
					System.out.println("Incoming message: on port #" + port + " : " + in);
					return Promise.of(in);
				})
				.withListenPort(port)
				.build();
	}
}
