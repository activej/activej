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

package io.activej.launchers.dataflow;

import io.activej.config.Config;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.inject.BinarySerializerModule;
import io.activej.dataflow.inject.DataflowModule;
import io.activej.dataflow.proto.DataflowMessagingProto;
import io.activej.dataflow.proto.serializer.CustomNodeSerializer;
import io.activej.dataflow.proto.serializer.FunctionSerializer;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.OptionalDependency;
import io.activej.inject.module.AbstractModule;

import java.util.List;
import java.util.concurrent.Executor;

import static io.activej.config.converter.ConfigConverters.*;

public final class DataflowClientModule extends AbstractModule {
	private DataflowClientModule() {
	}

	public static DataflowClientModule create() {
		return new DataflowClientModule();
	}

	@Override
	protected void configure() {
		install(DataflowModule.create());
	}

	@Provides
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	DataflowClient client(Executor executor, Config config, ByteBufsCodec<DataflowMessagingProto.DataflowResponse, DataflowMessagingProto.DataflowRequest> codec,
			BinarySerializerModule.BinarySerializerLocator serializers, FunctionSerializer functionSerializer, OptionalDependency<CustomNodeSerializer> optionalCustomNodeSerializer) {
		DataflowClient dataflowClient = DataflowClient.create(executor, config.get(ofPath(), "dataflow.secondaryBufferPath"), codec, serializers, functionSerializer);
		if (optionalCustomNodeSerializer.isPresent()) {
			return dataflowClient.withCustomNodeSerializer(optionalCustomNodeSerializer.get());
		}
		return dataflowClient;
	}

	@Provides
	DataflowGraph graph(DataflowClient client, List<Partition> partitions) {
		return new DataflowGraph(client, partitions);
	}

	@Provides
	List<Partition> partitions(Config config) {
		return config.get(ofList(ofInetSocketAddress()), "dataflow.partitions").stream()
				.map(Partition::new)
				.toList();
	}
}
