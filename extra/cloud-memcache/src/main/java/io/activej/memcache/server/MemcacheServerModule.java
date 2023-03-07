/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.memcache.server;

import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.memcache.protocol.SliceSerializerDef;
import io.activej.promise.Promise;
import io.activej.reactor.net.ServerSocketSettings;
import io.activej.reactor.net.SocketSettings;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.protocol.RpcMessageSerializer;
import io.activej.rpc.server.RpcServer;
import io.activej.serializer.SerializerFactory;

import static io.activej.common.MemSize.kilobytes;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.launchers.initializers.ConfigConverters.ofFrameFormat;
import static io.activej.memcache.protocol.MemcacheRpcMessage.*;

public class MemcacheServerModule extends AbstractModule {
	private MemcacheServerModule() {}

	public static MemcacheServerModule create() {
		return new MemcacheServerModule();
	}

	@Provides
	NioReactor reactor() {
		return Eventloop.create();
	}

	@Provides
	RingBuffer ringBuffer(Config config) {
		return RingBuffer.create(
				config.get(ofInteger(), "memcache.buffers"),
				config.get(ofMemSize(), "memcache.bufferCapacity").toInt());
	}

	@Provides
	RpcServer server(NioReactor reactor, Config config, RingBuffer storage) {
		return RpcServer.builder(reactor)
				.withHandler(GetRequest.class,
						request -> Promise.of(new GetResponse(storage.get(request.getKey()))))
				.withHandler(PutRequest.class,
						request -> {
							Slice slice = request.getData();
							storage.put(request.getKey(), slice.array(), slice.offset(), slice.length());
							return Promise.of(PutResponse.INSTANCE);
						})
				.withSerializer(RpcMessageSerializer.builder()
						.withSerializerFactory(SerializerFactory.builder().with(Slice.class, ctx -> new SliceSerializerDef()).build())
						.withMessageTypes(MESSAGE_TYPES)
						.build())
				.withStreamProtocol(
						config.get(ofMemSize(), "protocol.packetSize", kilobytes(64)),
						config.get(ofFrameFormat(), "protocol.frameFormat", null))
				.withServerSocketSettings(config.get(ofServerSocketSettings(), "server.serverSocketSettings", ServerSocketSettings.defaultInstance()))
				.withSocketSettings(config.get(ofSocketSettings(), "server.socketSettings", SocketSettings.defaultInstance()))
				.withListenAddresses(config.get(ofList(ofInetSocketAddress()), "server.listenAddresses"))
				.build();
	}
}
