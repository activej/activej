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

import io.activej.common.Checks;
import io.activej.memcache.protocol.MemcacheRpcMessage.GetRequest;
import io.activej.memcache.protocol.MemcacheRpcMessage.GetResponse;
import io.activej.memcache.protocol.MemcacheRpcMessage.PutRequest;
import io.activej.memcache.protocol.MemcacheRpcMessage.Slice;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.rpc.client.IRpcClient;

import static io.activej.reactor.Reactive.checkInReactorThread;

public abstract class AbstractMemcacheClient<K, V> extends AbstractReactive
	implements IMemcacheClient<K, V> {
	private static final boolean CHECKS = Checks.isEnabled(AbstractMemcacheClient.class);

	private final IRpcClient rpcClient;

	protected AbstractMemcacheClient(Reactor reactor, IRpcClient rpcClient) {
		super(reactor);
		this.rpcClient = rpcClient;
	}

	protected abstract byte[] encodeKey(K key);

	protected abstract Slice encodeValue(V value);

	protected abstract V decodeValue(Slice slice);

	@Override
	public Promise<Void> put(K key, V value, int timeout) {
		if (CHECKS) checkInReactorThread(this);
		PutRequest request = new PutRequest(encodeKey(key), encodeValue(value));
		return rpcClient.sendRequest(request, timeout).toVoid();
	}

	@Override
	public Promise<V> get(K key, int timeout) {
		if (CHECKS) checkInReactorThread(this);
		GetRequest request = new GetRequest(encodeKey(key));
		return rpcClient.<GetRequest, GetResponse>sendRequest(request, timeout)
			.map(response -> decodeValue(response.getData()));
	}

	@Override
	public Promise<Void> put(K key, V value) {
		if (CHECKS) checkInReactorThread(this);
		PutRequest request = new PutRequest(encodeKey(key), encodeValue(value));
		return rpcClient.sendRequest(request).toVoid();
	}

	@Override
	public Promise<V> get(K key) {
		if (CHECKS) checkInReactorThread(this);
		GetRequest request = new GetRequest(encodeKey(key));
		return rpcClient.<GetRequest, GetResponse>sendRequest(request)
			.map(response -> decodeValue(response.getData()));
	}
}
