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
import io.activej.common.function.FunctionEx;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.exception.ForbiddenPathException;
import io.activej.fs.exception.FsBatchException;
import io.activej.fs.exception.FsScalarException;
import io.activej.fs.util.RemoteFsUtils;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.isBijection;
import static java.util.stream.Collectors.toSet;

/**
 * A file system that can be configured to transform paths and filename via mapping functions.
 * <p>
 * Inherits all the limitations of parent {@link AsyncFs}
 */
final class TransformFs implements AsyncFs {
	private final AsyncFs parent;
	private final Function<String, Optional<String>> into;
	private final Function<String, Optional<String>> from;
	private final Function<String, Optional<String>> globInto;

	TransformFs(AsyncFs parent, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		this.parent = parent;
		this.into = into;
		this.from = from;
		this.globInto = globInto;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name) {
		Optional<String> transformed = into.apply(name);
		if (transformed.isEmpty()) {
			return Promise.ofException(new ForbiddenPathException());
		}
		return parent.upload(transformed.get());
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
		Optional<String> transformed = into.apply(name);
		if (transformed.isEmpty()) {
			return Promise.ofException(new ForbiddenPathException());
		}
		return parent.upload(transformed.get(), size);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(String name, long offset) {
		Optional<String> transformed = into.apply(name);
		if (transformed.isEmpty()) {
			return Promise.ofException(new ForbiddenPathException());
		}
		return parent.append(transformed.get(), offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
		Optional<String> transformed = into.apply(name);
		if (transformed.isEmpty()) {
			return Promise.ofException(new ForbiddenPathException());
		}
		return parent.download(transformed.get(), offset, limit);
	}

	@Override
	public Promise<Void> copy(String name, String target) {
		return transfer(name, target, parent::copy);
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return transfer(sourceToTarget, parent::copyAll);
	}

	@Override
	public Promise<Void> move(String name, String target) {
		return transfer(name, target, parent::move);
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return transfer(sourceToTarget, parent::moveAll);
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(String glob) {
		return globInto.apply(glob)
				.map(transformedGlob -> parent.list(transformedGlob)
						.map(transformMap($ -> true)))
				.orElseGet(() -> parent.list("**")
						.map(transformMap(RemoteFsUtils.getGlobStringPredicate(glob))));
	}

	@Override
	public Promise<@Nullable FileMetadata> info(String name) {
		return into.apply(name)
				.map(parent::info)
				.orElse(Promise.of(null));
	}

	@Override
	public Promise<Map<String, FileMetadata>> infoAll(Set<String> names) {
		Map<String, FileMetadata> result = new HashMap<>();
		Set<String> transformed = names.stream()
				.map(into)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(toSet());
		return transformed.isEmpty() ?
				Promise.of(result) :
				parent.infoAll(transformed)
						.whenResult(map -> map.forEach((name, meta) -> {
							Optional<String> maybeName = from.apply(name);
							assert maybeName.isPresent();
							result.put(maybeName.get(), meta);
						}))
						.map($ -> result);
	}

	@Override
	public Promise<Void> ping() {
		return parent.ping();
	}

	@Override
	public Promise<Void> delete(String name) {
		Optional<String> transformed = into.apply(name);
		if (transformed.isEmpty()) {
			return Promise.complete();
		}
		return parent.delete(transformed.get());
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		return parent.deleteAll(toDelete.stream()
				.map(into)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(toSet()));
	}

	private Promise<Void> transfer(String source, String target, AsyncBiConsumer<String, String> action) {
		Optional<String> transformed = into.apply(source);
		if (transformed.isEmpty()) {
			return Promise.ofException(new ForbiddenPathException("Path '" + source + "' is forbidden"));
		}
		Optional<String> transformedNew = into.apply(target);
		if (transformedNew.isEmpty()) {
			return Promise.ofException(new ForbiddenPathException("Path '" + target + "' is forbidden"));
		}
		return action.accept(transformed.get(), transformedNew.get());
	}

	private Promise<Void> transfer(Map<String, String> sourceToTarget, AsyncConsumer<Map<String, String>> action) {
		Map<String, String> renamed = new LinkedHashMap<>();
		Map<String, FsScalarException> exceptions = new HashMap<>();
		for (Map.Entry<String, String> entry : sourceToTarget.entrySet()) {
			String source = entry.getKey();
			Optional<String> transformed = into.apply(source);
			if (transformed.isEmpty()) {
				exceptions.put(source, new ForbiddenPathException("Path '" + source + "' is forbidden"));
				continue;
			}
			String target = entry.getValue();
			Optional<String> transformedNew = into.apply(target);
			if (transformedNew.isEmpty()) {
				exceptions.put(source, new ForbiddenPathException("Path '" + target + "' is forbidden"));
				continue;
			}
			renamed.put(transformed.get(), transformedNew.get());
		}
		if (!exceptions.isEmpty()) {
			return Promise.ofException(new FsBatchException(exceptions));
		}
		return action.accept(renamed);
	}

	private FunctionEx<Map<String, FileMetadata>, Map<String, FileMetadata>> transformMap(Predicate<String> postPredicate) {
		return map -> map.entrySet().stream()
				.map(entry -> from.apply(entry.getKey())
						.map(mappedName -> Map.entry(mappedName, entry.getValue())))
				.filter(entry -> entry.isPresent() && postPredicate.test(entry.get().getKey()))
				.map(Optional::get)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}
}
