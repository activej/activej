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

package io.activej.dataflow.inject;

import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowRequest;
import io.activej.dataflow.proto.DataflowMessagingProto.DataflowResponse;
import io.activej.dataflow.proto.ProtobufFunctionModule;
import io.activej.dataflow.proto.ProtobufUtils;
import io.activej.dataflow.stats.BinaryNodeStat;
import io.activej.dataflow.stats.StatReducer;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;

public final class DataflowModule extends AbstractModule {
	private DataflowModule() {
	}

	public static Module create() {
		return new DataflowModule();
	}

	@Override
	protected void configure() {
		install(ProtobufFunctionModule.create());
		install(DatasetIdModule.create());
		install(BinarySerializerModule.create());

		bind(new Key<StatReducer<BinaryNodeStat>>() {}).toInstance(BinaryNodeStat.REDUCER);
	}

	@Provides
	ByteBufsCodec<DataflowResponse, DataflowRequest> responseRequestCodec() {
		return ProtobufUtils.codec(DataflowResponse.parser());
	}

	@Provides
	ByteBufsCodec<DataflowRequest, DataflowResponse> requestResponseCodec() {
		return ProtobufUtils.codec(DataflowRequest.parser());
	}
}
