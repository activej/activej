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

package io.activej.remotefs;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class ForwardingFsClient implements FsClient {
	private final FsClient peer;

	public ForwardingFsClient(FsClient peer) {
		this.peer = peer;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long revision) {
		return peer.upload(name, revision);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return peer.upload(name);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		return peer.download(name, offset, length);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset) {
		return peer.download(name, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name) {
		return peer.download(name);
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return peer.delete(name);
	}

	@Override
	public Promise<Void> delete(@NotNull String name, long revision) {
		return peer.delete(name, revision);
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return peer.copy(name, target);
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target, long targetRevision) {
		return peer.copy(name, target, targetRevision);
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return peer.move(name, target);
	}

	@Override
	public Promise<Void> move(@NotNull String filename, @NotNull String target, long targetRevision, long tombstoneRevision) {
		return peer.move(filename, target, targetRevision, tombstoneRevision);
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(@NotNull String glob) {
		return peer.listEntities(glob);
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return peer.list(glob);
	}

	@Override
	public Promise<@Nullable FileMetadata> getMetadata(@NotNull String name) {
		return peer.getMetadata(name);
	}

	@Override
	public Promise<Void> ping() {
		return peer.ping();
	}

	@Override
	public FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from, @NotNull Function<String, Optional<String>> globInto) {
		return peer.transform(into, from, globInto);
	}

	@Override
	public FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from) {
		return peer.transform(into, from);
	}

	@Override
	public FsClient addingPrefix(@NotNull String prefix) {
		return peer.addingPrefix(prefix);
	}

	@Override
	public FsClient subfolder(@NotNull String folder) {
		return peer.subfolder(folder);
	}

	@Override
	public FsClient strippingPrefix(@NotNull String prefix) {
		return peer.strippingPrefix(prefix);
	}

	@Override
	public FsClient filter(@NotNull Predicate<String> predicate) {
		return peer.filter(predicate);
	}

	@Override
	public FsClient mount(@NotNull String mountpoint, @NotNull FsClient client) {
		return peer.mount(mountpoint, client);
	}
}
