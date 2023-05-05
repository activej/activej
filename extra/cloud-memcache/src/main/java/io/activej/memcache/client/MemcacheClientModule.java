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

package io.activej.memcache.client;

import io.activej.config.Config;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.memcache.protocol.MemcacheRpcMessage.Slice;
import io.activej.memcache.protocol.SliceSerializerDef;
import io.activej.reactor.Reactor;
import io.activej.reactor.net.SocketSettings;
import io.activej.reactor.nio.NioReactor;
import io.activej.rpc.client.IRpcClient;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.client.sender.strategy.impl.RendezvousHashing;
import io.activej.rpc.protocol.RpcMessage;
import io.activej.serializer.SerializerFactory;

import java.time.Duration;

import static io.activej.common.MemSize.kilobytes;
import static io.activej.config.converter.ConfigConverters.*;
import static io.activej.launchers.initializers.ConfigConverters.ofFrameFormat;
import static io.activej.memcache.protocol.MemcacheRpcMessage.HASH_FUNCTION;
import static io.activej.memcache.protocol.MemcacheRpcMessage.MESSAGE_TYPES;
import static org.slf4j.LoggerFactory.getLogger;

public class MemcacheClientModule extends AbstractModule {
	private MemcacheClientModule() {}

	public static MemcacheClientModule create() {return new MemcacheClientModule();}

	@Provides
	IRpcClient rpcClient(NioReactor reactor, Config config) {
		return RpcClient.builder(reactor)
				.withStrategy(
						RendezvousHashing.builder(HASH_FUNCTION)
								.withMinActiveShards(config.get(ofInteger(), "client.minAliveConnections", 1))
								.withShards(config.get(ofList(ofInetSocketAddress()), "client.addresses"))
								.build())
				.withSerializer(SerializerFactory.builder()
						.with(Slice.class, ctx -> new SliceSerializerDef())
						.withSubclasses(RpcMessage.SUBCLASSES_ID, MESSAGE_TYPES)
						.build()
						.create(RpcMessage.class))
				.withStreamProtocol(
						config.get(ofMemSize(), "protocol.packetSize", kilobytes(64)),
						config.get(ofFrameFormat(), "protocol.frameFormat", null))
				.withSocketSettings(config.get(ofSocketSettings(), "client.socketSettings", SocketSettings.defaultInstance()))
				.withConnectTimeout(config.get(ofDuration(), "client.connectSettings.connectTimeout", Duration.ofSeconds(10)))
				.withReconnectInterval(config.get(ofDuration(), "client.connectSettings.reconnectInterval", Duration.ofSeconds(1)))
				.withLogger(getLogger(IMemcacheClient.class))
				.build();
	}

	@Provides
	RawMemcacheClient memcacheClient(Reactor reactor, IRpcClient client) {
		return RawMemcacheClient.create(reactor, client);
	}

}
