package memcached;

import io.activej.config.Config;
import io.activej.di.annotation.Provides;
import io.activej.di.module.AbstractModule;
import io.activej.eventloop.Eventloop;
import io.activej.memcache.protocol.SerializerDefSlice;
import io.activej.memcache.server.RingBuffer;
import io.activej.promise.Promise;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.SerializerBuilder;
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
	Eventloop eventloop() {
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
	RpcServer server(Eventloop eventloop, RingBuffer storage, InetSocketAddress address) {
		return RpcServer.create(eventloop)
				.withHandler(GetRequest.class,
						request -> Promise.of(new GetResponse(storage.get(request.getKey()))))
				.withHandler(PutRequest.class,
						request -> {
							Slice slice = request.getData();
							System.out.println("Server on port #" + address.getPort() + " accepted message!");
							storage.put(request.getKey(), slice.array(), slice.offset(), slice.length());
							return Promise.of(PutResponse.INSTANCE);
						})
				.withSerializerBuilder(SerializerBuilder.create()
						.withSerializer(Slice.class, new SerializerDefSlice()))
				.withMessageTypes(MESSAGE_TYPES)
				.withListenAddresses(address);
	}
}
