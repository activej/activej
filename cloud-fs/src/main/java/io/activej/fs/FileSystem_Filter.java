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
import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.exception.FileSystemBatchException;
import io.activej.fs.exception.FileSystemScalarException;
import io.activej.fs.exception.ForbiddenPathException;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.isBijection;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.*;

/**
 * A file system that can be configured to forbid certain paths and filenames.
 * <p>
 * Inherits all the limitations of parent {@link IFileSystem}
 */
public final class FileSystem_Filter implements IFileSystem {

	private final IFileSystem parent;
	private final Predicate<String> predicate;

	FileSystem_Filter(IFileSystem parent, Predicate<String> predicate) {
		this.parent = parent;
		this.predicate = predicate;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
		if (!predicate.test(name)) {
			return Promise.ofException(new ForbiddenPathException());
		}
		return parent.upload(name);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
		if (!predicate.test(name)) {
			return Promise.ofException(new ForbiddenPathException());
		}
		return parent.upload(name);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(String name, long offset) {
		if (!predicate.test(name)) {
			return Promise.ofException(new ForbiddenPathException());
		}
		return parent.append(name, offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
		if (!predicate.test(name)) {
			return Promise.ofException(new ForbiddenPathException());
		}
		return parent.download(name, offset, limit);
	}

	@Override
	public Promise<Void> copy(String name, String target) {
		return filteringOp(name, target, parent::copy);
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return filteringOp(sourceToTarget, parent::copyAll);
	}

	@Override
	public Promise<Void> move(String name, String target) {
		return filteringOp(name, target, parent::move);
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return filteringOp(sourceToTarget, parent::moveAll);
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(String glob) {
		return parent.list(glob)
				.map(map -> map.entrySet().stream()
						.filter(entry -> predicate.test(entry.getKey()))
						.collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
	}

	@Override
	public Promise<@Nullable FileMetadata> info(String name) {
		if (!predicate.test(name)) {
			return Promise.of(null);
		}
		return parent.info(name);
	}

	@Override
	public Promise<Map<String, FileMetadata>> infoAll(Set<String> names) {
		Map<Boolean, Set<String>> partitioned = names.stream().collect(partitioningBy(predicate, toSet()));
		Set<String> query = partitioned.get(TRUE);
		return query.isEmpty() ?
				Promise.of(Map.of()) :
				parent.infoAll(query);
	}

	@Override
	public Promise<Void> ping() {
		return parent.ping();
	}

	@Override
	public Promise<Void> delete(String name) {
		if (!predicate.test(name)) {
			return Promise.complete();
		}
		return parent.delete(name);
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return parent.deleteAll(toDelete.stream()
				.filter(predicate)
				.collect(toSet()));
	}

	private Promise<Void> filteringOp(String source, String target, AsyncBiConsumer<String, String> original) {
		if (!predicate.test(source)) {
			return Promise.ofException(new ForbiddenPathException("Path '" + source + "' is forbidden"));
		}
		if (!predicate.test(target)) {
			return Promise.ofException(new ForbiddenPathException("Path '" + target + "' is forbidden"));
		}
		return original.accept(source, target);
	}

	private Promise<Void> filteringOp(Map<String, String> sourceToTarget, AsyncConsumer<Map<String, String>> original) {
		Map<String, String> renamed = new LinkedHashMap<>();
		Map<String, FileSystemScalarException> exceptions = new HashMap<>();
		for (Map.Entry<String, String> entry : sourceToTarget.entrySet()) {
			String source = entry.getKey();
			if (!predicate.test(source)) {
				exceptions.put(source, new ForbiddenPathException("Path '" + source + "' is forbidden"));
			}
			String target = entry.getValue();
			if (!predicate.test(target)) {
				exceptions.put(source, new ForbiddenPathException("Path '" + target + "' is forbidden"));
			}
			renamed.put(source, target);
		}
		if (!exceptions.isEmpty()) {
			return Promise.ofException(new FileSystemBatchException(exceptions));
		}
		return original.accept(renamed);
	}
}
