package advancedrpc;

import io.activej.eventloop.Eventloop;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.protocol.RpcMessageSerializer;
import io.activej.rpc.server.RpcServer;
import io.activej.worker.WorkerPool;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;

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
				.withSerializer(RpcMessageSerializer.of(Integer.class))
				.withHandler(Integer.class, in -> {
					System.out.println("Incoming message: on port #" + port + " : " + in);
					return Promise.of(in);
				})
				.withListenPort(port)
				.build();
	}
}
