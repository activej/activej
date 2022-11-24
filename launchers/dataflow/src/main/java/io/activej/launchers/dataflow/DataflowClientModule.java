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
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.dataflow.inject.DataflowModule;
import io.activej.dataflow.messaging.DataflowRequest;
import io.activej.dataflow.messaging.DataflowResponse;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

import java.util.List;

import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.config.converter.ConfigConverters.ofList;

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
	DataflowClient client(ByteBufsCodec<DataflowResponse, DataflowRequest> codec,
			BinarySerializerLocator serializers
	) {
		return DataflowClient.create(codec, serializers);
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
