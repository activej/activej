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

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.codec.StructuredCodec;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.dataflow.inject.CodecsModule.Subtypes;
import io.activej.dataflow.stats.BinaryNodeStat;
import io.activej.dataflow.stats.StatReducer;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;

import static io.activej.codec.json.JsonUtils.fromJson;
import static io.activej.codec.json.JsonUtils.toJsonBuf;
import static io.activej.csp.binary.ByteBufsDecoder.ofNullTerminatedBytes;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DataflowModule extends AbstractModule {
	private DataflowModule() {
	}

	public static Module create() {
		return new DataflowModule();
	}

	@Override
	protected void configure() {
		install(DataflowCodecs.create());
		install(DatasetIdModule.create());
		install(BinarySerializerModule.create());

		bind(new Key<StatReducer<BinaryNodeStat>>() {}).toInstance(BinaryNodeStat.REDUCER);
	}

	@Provides
	<I, O> ByteBufsCodec<I, O> byteBufsCodec(@Subtypes StructuredCodec<I> inputCodec, @Subtypes StructuredCodec<O> outputCodec) {
		return ByteBufsCodec.ofDelimiter(ofNullTerminatedBytes(), buf -> {
			ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
			buf1.put((byte) 0);
			return buf1;
		}).andThen(buf -> fromJson(inputCodec, buf.asString(UTF_8)), item -> toJsonBuf(outputCodec, item));
	}
}
