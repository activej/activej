package memcached;

import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.memcache.protocol.SliceSerializerDef;
import io.activej.memcache.server.RingBuffer;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.protocol.RpcMessageSerializer;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.SerializerFactory;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;

import java.net.InetSocketAddress;

import static io.activej.config.converter.ConfigConverters.ofInteger;
import static io.activej.config.converter.ConfigConverters.ofMemSize;
import static io.activej.memcache.protocol.MemcacheRpcMessage.*;

public class MemcacheMultiServerModule extends AbstractModule {
	private MemcacheMultiServerModule() {}

	public static MemcacheMultiServerModule create() {
		return new MemcacheMultiServerModule();
	}

	@Provides
	@Worker
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	@Worker
	InetSocketAddress port(@WorkerId int workerId) {
		return new InetSocketAddress("localhost", 9000 + workerId);
	}

	@Provides
	@Worker
	RingBuffer ringBuffer(Config config) {
		return RingBuffer.create(
				config.get(ofInteger(), "memcache.buffers"),
				config.get(ofMemSize(), "memcache.bufferCapacity").toInt());
	}

	@Provides
	@Worker
	RpcServer server(NioReactor reactor, RingBuffer storage, InetSocketAddress address) {
		return RpcServer.builder(reactor)
				.withHandler(GetRequest.class,
						request -> Promise.of(new GetResponse(storage.get(request.getKey()))))
				.withHandler(PutRequest.class,
						request -> {
							Slice slice = request.getData();
							System.out.println("Server on port #" + address.getPort() + " accepted message!");
							storage.put(request.getKey(), slice.array(), slice.offset(), slice.length());
							return Promise.of(PutResponse.INSTANCE);
						})
				.withSerializer(RpcMessageSerializer.builder()
						.withSerializerFactory(SerializerFactory.builder().with(Slice.class, ctx -> new SliceSerializerDef()).build())
						.withMessageTypes(MESSAGE_TYPES)
						.build())
				.withListenAddresses(address)
				.build();
	}
}
