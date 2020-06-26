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
import io.activej.csp.ChannelConsumers;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.*;

final class FilterFsClient implements FsClient {
	private final FsClient parent;
	private final Predicate<String> predicate;

	FilterFsClient(FsClient parent, Predicate<String> predicate) {
		this.parent = parent;
		this.predicate = predicate;
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		if (!predicate.test(name)) {
			return Promise.of(ChannelConsumers.recycling());
		}
		return parent.upload(name);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
		if (!predicate.test(name)) {
			return Promise.ofException(FILE_NOT_FOUND);
		}
		return parent.download(name, offset, limit);
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return filteringOp(name, target, parent::copy);
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		return filteringOp(sourceToTarget, parent::copyAll);
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return filteringOp(name, target, parent::move);
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		return filteringOp(sourceToTarget, parent::moveAll);
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return parent.list(glob)
				.map(list -> list.stream()
						.filter(meta -> predicate.test(meta.getName()))
						.collect(toList()));
	}

	@Override
	public Promise<@Nullable FileMetadata> inspect(@NotNull String name) {
		if (!predicate.test(name)) {
			return Promise.of(null);
		}
		return parent.inspect(name);
	}

	@Override
	public Promise<Map<String, @Nullable FileMetadata>> inspectAll(@NotNull List<String> names) {
		Map<Boolean, List<String>> partitioned = names.stream().collect(partitioningBy(predicate));
		List<String> query = partitioned.get(TRUE);
		return (query.isEmpty() ?
				Promise.of(Collections.<String, FileMetadata>emptyMap()) :
				parent.inspectAll(query))
				.map(inspected -> {
					Map<String, FileMetadata> result = new HashMap<>(inspected);
					partitioned.get(FALSE).forEach(name -> result.put(name, null));
					return result;
				});
	}

	@Override
	public Promise<Void> ping() {
		return parent.ping();
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
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

	private Promise<Void> filteringOp(String source, String target, BiFunction<String, String, Promise<Void>> original) {
		if (!predicate.test(source)) {
			return Promise.ofException(FILE_NOT_FOUND);
		}
		if (!predicate.test(target)) {
			return Promise.complete();
		}
		return original.apply(source, target);
	}

	private Promise<Void> filteringOp(Map<String, String> sourceToTarget, Function<Map<String, String>, Promise<Void>> original) {
		Map<String, String> renamed = new LinkedHashMap<>();
		for (Map.Entry<String, String> entry : sourceToTarget.entrySet()) {
			if (!predicate.test(entry.getKey())) {
				return Promise.ofException(FILE_NOT_FOUND);
			}
			if (!predicate.test(entry.getValue())) {
				return Promise.complete();
			}
			renamed.put(entry.getKey(), entry.getValue());
		}
		return original.apply(renamed);
	}
}
