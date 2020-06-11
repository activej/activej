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
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.activej.remotefs.RemoteFsUtils.copyFile;

final class MountingFsClient implements FsClient {
	private final FsClient root;
	private final Map<String, FsClient> mounts;

	MountingFsClient(FsClient root, Map<String, FsClient> mounts) {
		this.root = root;
		this.mounts = mounts;
	}

	private FsClient findMount(String filename) {
		int idx = filename.lastIndexOf('/');
		while (idx != -1) {
			String path = filename.substring(0, idx);
			FsClient mount = mounts.get(path);
			if (mount != null) {
				return mount;
			}
			idx = filename.lastIndexOf('/', idx - 1);
		}
		return root;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return findMount(name).upload(name);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		return findMount(name).download(name, offset, length);
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return Promises.toList(Stream.concat(Stream.of(root), mounts.values().stream()).map(f -> f.list(glob)))
				.map(listOfLists -> FileMetadata.flatten(listOfLists.stream()));
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		FsClient first = findMount(name);
		FsClient second = findMount(target);
		if (first == second) {
			return first.move(name, target);
		}
		return first.download(name)
				.then(supplier ->
						second.upload(name)
								.then(supplier::streamTo))
				.then(() -> first.delete(name));
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		FsClient first = findMount(name);
		FsClient second = findMount(target);
		if (first == second) {
			return first.copy(name, target);
		}
		return copyFile(first, second, name);
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return findMount(name).delete(name);
	}

	@Override
	public FsClient mount(@NotNull String mountpoint, @NotNull FsClient client) {
		Map<String, FsClient> map = new HashMap<>(mounts);
		map.put(mountpoint, client.strippingPrefix(mountpoint + '/'));
		return new MountingFsClient(root, map);
	}
}
