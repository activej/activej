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

package io.activej.cube.etcd;

import io.activej.cube.aggregation.ChunkIdGenerator;
import io.activej.etcd.EtcdUtils;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public final class EtcdChunkIdGenerator extends AbstractReactive
	implements ChunkIdGenerator {

	private final KV kvClient;
	private final ByteSequence idKey;

	private EtcdChunkIdGenerator(Reactor reactor, KV kvClient, ByteSequence idKey) {
		super(reactor);
		this.kvClient = kvClient;
		this.idKey = idKey;
	}

	public static EtcdChunkIdGenerator create(Reactor reactor, KV kvClient, ByteSequence idKey) {
		return new EtcdChunkIdGenerator(reactor, kvClient, idKey);
	}

	@Override
	public Promise<String> createProtoChunkId() {
		return Promise.of(UUID.randomUUID().toString());
	}

	@Override
	public Promise<Map<String, Long>> convertToActualChunkIds(Set<String> protoChunkIds) {
		return Promise.ofCompletionStage(EtcdUtils.atomicAdd(kvClient, idKey, (long) protoChunkIds.size()))
			.map(response -> {
				Iterator<String> protoChunkIdsIt = protoChunkIds.iterator();
				return LongStream.range(response.prevValue(), response.newValue())
					.boxed()
					.collect(Collectors.toMap($ -> protoChunkIdsIt.next(), Function.identity()));
			});
	}
}
