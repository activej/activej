package advancedrpc;

import io.activej.di.Key;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.SerializerBuilder;
import io.activej.worker.Worker;
import io.activej.worker.WorkerId;
import io.activej.worker.WorkerPool;

public class AdvancedRpcServerModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(new Key<WorkerPool.Instances<RpcServer>>() {}).asEager();
	}

	private AdvancedRpcServerModule() {
	}

	public static AdvancedRpcServerModule create() {
		return new AdvancedRpcServerModule();
	}

	@Provides
	@Worker
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	Integer port(@WorkerId int workerId) {
		return 9000 + workerId;
	}

	@Provides
	@Worker
	RpcServer rpcServer(Eventloop eventloop, Integer port) {
		return RpcServer.create(eventloop)
				.withSerializerBuilder(SerializerBuilder.create())
				.withMessageTypes(Integer.class)
				.withHandler(Integer.class, Integer.class, in -> {
					System.out.println("Incoming message: on port #" + port + " : " + in);
					return Promise.of(in);
				})
				.withListenPort(port);
	}
}
