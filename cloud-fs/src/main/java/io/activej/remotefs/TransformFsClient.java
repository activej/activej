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
import io.activej.common.CollectorsEx;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.remotefs.util.RemoteFsUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;

final class TransformFsClient implements FsClient {
	private final FsClient parent;
	private final Function<String, Optional<String>> into;
	private final Function<String, Optional<String>> from;
	private final Function<String, Optional<String>> globInto;

	TransformFsClient(FsClient parent, Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		this.parent = parent;
		this.into = into;
		this.from = from;
		this.globInto = globInto;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		Optional<String> transformed = into.apply(name);
		if (!transformed.isPresent()) {
			return Promise.ofException(BAD_PATH);
		}
		return parent.upload(transformed.get());
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size) {
		Optional<String> transformed = into.apply(name);
		if (!transformed.isPresent()) {
			return Promise.ofException(BAD_PATH);
		}
		return parent.upload(transformed.get(), size);
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset) {
		Optional<String> transformed = into.apply(name);
		if (!transformed.isPresent()) {
			return Promise.ofException(BAD_PATH);
		}
		return parent.append(transformed.get(), offset);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
		Optional<String> transformed = into.apply(name);
		if (!transformed.isPresent()) {
			return Promise.ofException(FILE_NOT_FOUND);
		}
		return parent.download(transformed.get(), offset, limit);
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return transfer(name, target, parent::copy);
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		return transfer(sourceToTarget, parent::copyAll);
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return transfer(name, target, parent::move);
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		return transfer(sourceToTarget, parent::moveAll);
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(@NotNull String glob) {
		return globInto.apply(glob)
				.map(transformedGlob -> parent.list(transformedGlob)
						.map(transformMap($ -> true)))
				.orElseGet(() -> parent.list("**")
						.map(transformMap(RemoteFsUtils.getGlobStringPredicate(glob))));
	}

	@Override
	public Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return into.apply(name)
				.map(parent::info)
				.orElse(Promise.of(null));
	}

	@Override
	public Promise<Map<String, @NotNull FileMetadata>> infoAll(@NotNull Set<String> names) {
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
	public Promise<Void> delete(@NotNull String name) {
		Optional<String> transformed = into.apply(name);
		if (!transformed.isPresent()) {
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

	@Override
	public FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from, @NotNull Function<String, Optional<String>> globInto) {
		return new TransformFsClient(parent,
				name -> into.apply(name).flatMap(this.into),
				name -> this.from.apply(name).flatMap(from),
				name -> globInto.apply(name).flatMap(this.globInto)
		);
	}

	private Promise<Void> transfer(String source, String target, BiFunction<String, String, Promise<Void>> action) {
		Optional<String> transformed = into.apply(source);
		Optional<String> transformedNew = into.apply(target);
		if (!transformed.isPresent() || !transformedNew.isPresent()) {
			return Promise.ofException(BAD_PATH);
		}
		return action.apply(transformed.get(), transformedNew.get());
	}

	private Promise<Void> transfer(Map<String, String> sourceToTarget, Function<Map<String, String>, Promise<Void>> action) {
		Map<String, String> renamed = new LinkedHashMap<>();
		for (Map.Entry<String, String> entry : sourceToTarget.entrySet()) {
			Optional<String> transformed = into.apply(entry.getKey());
			Optional<String> transformedNew = into.apply(entry.getValue());
			if (!transformed.isPresent() || !transformedNew.isPresent()) {
				return Promise.ofException(BAD_PATH);
			}
			renamed.put(transformed.get(), transformedNew.get());
		}
		return action.apply(renamed);
	}

	private Function<Map<String, FileMetadata>, Map<String, FileMetadata>> transformMap(Predicate<String> postPredicate) {
		return map -> map.entrySet().stream()
				.map(entry -> from.apply(entry.getKey())
						.map(mappedName -> new SimpleEntry<>(mappedName, entry.getValue())))
				.filter(entry -> entry.isPresent() && postPredicate.test(entry.get().getKey()))
				.map(Optional::get)
				.collect(CollectorsEx.toMap());
	}
}
