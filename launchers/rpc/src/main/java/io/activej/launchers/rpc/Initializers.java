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

package io.activej.launchers.rpc;

import io.activej.common.initializer.Initializer;
import io.activej.config.Config;
import io.activej.rpc.client.RpcClient;
import io.activej.rpc.server.RpcServer;
import io.activej.trigger.TriggerResult;
import io.activej.trigger.TriggersModuleSettings;

import java.time.Duration;
import java.util.List;

import static io.activej.config.converter.ConfigConverters.ofDuration;
import static io.activej.config.converter.ConfigConverters.ofMemSize;
import static io.activej.launchers.initializers.ConfigConverters.ofFrameFormat;
import static io.activej.launchers.initializers.Initializers.ofAbstractServer;
import static io.activej.rpc.server.RpcServer.DEFAULT_INITIAL_BUFFER_SIZE;
import static io.activej.trigger.Severity.HIGH;

public final class Initializers {

	public static Initializer<RpcServer> ofRpcServer(Config config) {
		return server -> server
				.withInitializer(ofAbstractServer(config.getChild("rpc.server")))
				.withStreamProtocol(
						config.get(ofMemSize(), "rpc.streamProtocol.defaultPacketSize", DEFAULT_INITIAL_BUFFER_SIZE),
						config.get(ofFrameFormat(), "rpc.streamProtocol.frameFormat", null))
				.withAutoFlushInterval(config.get(ofDuration(), "rpc.flushDelay", Duration.ZERO));
	}

	public static Initializer<TriggersModuleSettings> ofReactiveRpcClient() {
		return triggersSettings -> triggersSettings
				.with(RpcClient.class, HIGH, "unresponsiveServers",
						rpcClient -> {
							List<String> unresponsiveServers = rpcClient.getUnresponsiveServers();
							return unresponsiveServers.isEmpty() ? TriggerResult.none() :
									TriggerResult.ofValue(unresponsiveServers);
						});
	}
}
