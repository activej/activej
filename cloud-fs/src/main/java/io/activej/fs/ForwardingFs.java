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

package io.activej.fs;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link AsyncFs} that forwards all the calls to the underlying {@link AsyncFs}.
 * May be suitable for creating decorators that override certain behaviour of file system.
 * <p>
 * Inherits all the limitations of underlying {@link AsyncFs}
 */
public abstract class ForwardingFs implements AsyncFs {
	private final AsyncFs peer;

	protected ForwardingFs(AsyncFs peer) {
		this.peer = peer;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
		return peer.upload(name);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
		return peer.upload(name, size);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(String name, long offset) {
		return peer.append(name, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
		return peer.download(name, offset, limit);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name) {
		return peer.download(name);
	}

	@Override
	public Promise<Void> delete(String name) {
		return peer.delete(name);
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return peer.deleteAll(toDelete);
	}

	@Override
	public Promise<Void> copy(String name, String target) {
		return peer.copy(name, target);
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		return peer.copyAll(sourceToTarget);
	}

	@Override
	public Promise<Void> move(String name, String target) {
		return peer.move(name, target);
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		return peer.moveAll(sourceToTarget);
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(String glob) {
		return peer.list(glob);
	}

	@Override
	public Promise<@Nullable FileMetadata> info(String name) {
		return peer.info(name);
	}

	@Override
	public Promise<Map<String, FileMetadata>> infoAll(Set<String> names) {
		return peer.infoAll(names);
	}

	@Override
	public Promise<Void> ping() {
		return peer.ping();
	}
}
