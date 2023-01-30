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

import io.activej.async.function.AsyncBiConsumer;
import io.activej.async.function.AsyncConsumer;
import io.activej.async.function.AsyncSupplier;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.collection.Try;
import io.activej.common.tuple.Tuple2;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.exception.FileSystemBatchException;
import io.activej.fs.exception.FileSystemScalarException;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.isBijection;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

/**
 * A file system that allows to mount several {@link IFileSystem} implementations to correspond to different filenames.
 * <p>
 * Inherits the most strict limitations of all the mounted file systems implementations and root file system.
 */
public final class FileSystem_Mounting implements IFileSystem {
	private final IFileSystem root;
	private final Map<String, IFileSystem> mounts;

	FileSystem_Mounting(IFileSystem root, Map<String, IFileSystem> mounts) {
		this.root = root;
		this.mounts = mounts;
	}

	private IFileSystem findMount(String filename) {
		int idx = filename.lastIndexOf('/');
		while (idx != -1) {
			String path = filename.substring(0, idx);
			IFileSystem mount = mounts.get(path);
			if (mount != null) {
				return mount;
			}
			idx = filename.lastIndexOf('/', idx - 1);
		}
		return root;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
		return findMount(name).upload(name);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
		return findMount(name).upload(name, size);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(String name, long offset) {
		return findMount(name).append(name, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
		return findMount(name).download(name, offset, limit);
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(String glob) {
		return Promises.toList(Stream.concat(Stream.of(root), mounts.values().stream()).map(f -> f.list(glob)))
				.map(listOfMaps -> FileMetadata.flatten(listOfMaps.stream()));
	}

	@Override
	public Promise<@Nullable FileMetadata> info(String name) {
		return findMount(name).info(name);
	}

	@Override
	public Promise<Map<String, FileMetadata>> infoAll(Set<String> names) {
		Map<String, FileMetadata> result = new HashMap<>();
		return Promises.all(names.stream()
						.collect(groupingBy(this::findMount, toSet()))
						.entrySet().stream()
						.map(entry -> entry.getKey()
								.infoAll(entry.getValue())
								.whenResult(result::putAll)))
				.map($ -> result);
	}

	@Override
	public Promise<Void> copy(String name, String target) {
		return transfer(name, target, (s, t) -> fs -> fs.copy(s, t), false);
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return transfer(sourceToTarget, IFileSystem::copyAll, false);
	}

	@Override
	public Promise<Void> move(String name, String target) {
		return transfer(name, target, (s, t) -> fs -> fs.move(s, t), true);
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return transfer(sourceToTarget, IFileSystem::moveAll, true);
	}

	@Override
	public Promise<Void> delete(String name) {
		return findMount(name).delete(name);
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return Promises.all(toDelete.stream()
				.collect(groupingBy(this::findMount, IdentityHashMap::new, toSet()))
				.entrySet().stream()
				.map(entry -> entry.getKey().deleteAll(entry.getValue())));
	}

	private Promise<Void> transfer(String source, String target, BiFunction<String, String, AsyncConsumer<IFileSystem>> action, boolean deleteSource) {
		IFileSystem first = findMount(source);
		IFileSystem second = findMount(target);
		if (first == second) {
			return action.apply(source, target).accept(first);
		}
		return first.download(source)
				.then(supplier -> supplier.streamTo(second.upload(target)))
				.then(() -> deleteSource ? first.delete(source) : Promise.complete());
	}

	private Promise<Void> transfer(Map<String, String> sourceToTarget, AsyncBiConsumer<IFileSystem, Map<String, String>> action, boolean deleteSource) {
		List<AsyncSupplier<Tuple2<String, Try<Void>>>> movePromises = new ArrayList<>();

		Map<IFileSystem, Map<String, String>> groupedBySameFileSystems = new IdentityHashMap<>();

		for (Map.Entry<String, String> entry : sourceToTarget.entrySet()) {
			String source = entry.getKey();
			String target = entry.getValue();
			IFileSystem first = findMount(source);
			IFileSystem second = findMount(target);
			if (first == second) {
				groupedBySameFileSystems
						.computeIfAbsent(first, $ -> new HashMap<>())
						.put(source, target);
			} else {
				movePromises.add(() -> first.download(source)
						.then(supplier -> supplier.streamTo(second.upload(target)))
						.then(() -> deleteSource ? first.delete(target) : Promise.complete())
						.toTry()
						.map(aTry -> new Tuple2<>(source, aTry)));
			}
		}
		for (Map.Entry<IFileSystem, Map<String, String>> entry : groupedBySameFileSystems.entrySet()) {
			movePromises.add(() -> action.accept(entry.getKey(), entry.getValue()).toTry().map(aTry -> new Tuple2<>("", aTry)));
		}

		return Promises.toList(movePromises.stream().map(AsyncSupplier::get))
				.whenResult(list -> {
					List<Tuple2<String, Exception>> exceptions = list.stream()
							.filter(tuple -> tuple.value2().isException())
							.map(tuple -> new Tuple2<>(tuple.value1(), tuple.value2().getException()))
							.toList();
					if (!exceptions.isEmpty()) {
						Map<String, FileSystemScalarException> scalarExceptions = new HashMap<>();
						for (Tuple2<String, Exception> tuple : exceptions) {
							Exception exception = tuple.value2();
							if (exception instanceof FileSystemScalarException) {
								scalarExceptions.put(tuple.value1(), (FileSystemScalarException) exception);
							} else if (exception instanceof FileSystemBatchException) {
								scalarExceptions.putAll(((FileSystemBatchException) exception).getExceptions());
							} else {
								throw exception;
							}
						}
						throw new FileSystemBatchException(scalarExceptions);
					}
				})
				.toVoid();
	}

}
