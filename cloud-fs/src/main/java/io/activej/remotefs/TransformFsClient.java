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

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

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
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long revision) {
		Optional<String> transformed = into.apply(name);
		if (!transformed.isPresent()) {
			return Promise.ofException(BAD_PATH);
		}
		return parent.upload(transformed.get(), revision);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		Optional<String> transformed = into.apply(name);
		if (!transformed.isPresent()) {
			return Promise.ofException(FILE_NOT_FOUND);
		}
		return parent.download(transformed.get(), offset, length);
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target, long targetRevision, long tombstoneRevision) {
		return renamingOp(name, target, (from, to) -> parent.move(from, to, targetRevision, tombstoneRevision));
	}

	@Override
	public Promise<Void> moveDir(@NotNull String name, @NotNull String target, long targetRevision, long tombstoneRevision) {
		return renamingOp(name, target, (from, to) -> parent.moveDir(from, to, targetRevision, tombstoneRevision));
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target, long targetRevision) {
		return renamingOp(name, target, (from, to) -> parent.copy(from, to, targetRevision));
	}

	private Promise<Void> renamingOp(String filename, String newFilename, BiFunction<String, String, Promise<Void>> original) {
		Optional<String> transformed = into.apply(filename);
		Optional<String> transformedNew = into.apply(newFilename);
		if (!transformed.isPresent() || !transformedNew.isPresent()) {
			return Promise.ofException(BAD_PATH);
		}
		return original.apply(transformed.get(), transformedNew.get());
	}

	@Override
	public Promise<List<FileMetadata>> listEntities(@NotNull String glob) {
		return parent.listEntities(globInto.apply(glob).orElse("**"))
				.map(transformList(glob));
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return parent.list(globInto.apply(glob).orElse("**"))
				.map(transformList(glob));
	}

	private Function<List<FileMetadata>, List<FileMetadata>> transformList(String glob) {
		Predicate<String> pred = RemoteFsUtils.getGlobStringPredicate(glob);
		return list -> list.stream()
				.map(meta ->
						from.apply(meta.getName())
								.map(name -> FileMetadata.of(name, meta.getSize(), meta.getTimestamp(), meta.getRevision())))
				.filter(meta -> meta.isPresent() && pred.test(meta.get().getName()))
				.map(Optional::get)
				.collect(toList());
	}

	@Override
	public Promise<FileMetadata> getMetadata(@NotNull String name) {
		Optional<String> transformed = into.apply(name);
		return transformed.map(s ->
				parent.getMetadata(s)
						.map(meta -> {
							if (meta == null) {
								return null;
							}
							return from.apply(meta.getName())
									.map(meta::withName)
									.orElse(null);
						})).orElse(Promise.of(null));
	}

	@Override
	public Promise<Void> ping() {
		return parent.ping();
	}

	@Override
	public Promise<Void> delete(@NotNull String name, long revision) {
		Optional<String> transformed = into.apply(name);
		if (!transformed.isPresent()) {
			return Promise.complete();
		}
		return parent.delete(transformed.get(), revision);
	}

	@Override
	public FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from, @NotNull Function<String, Optional<String>> globInto) {
		if (into == this.from && from == this.into) { // huh
			return parent;
		}
		return new TransformFsClient(parent,
				name -> into.apply(name).flatMap(this.into),
				name -> this.from.apply(name).flatMap(from),
				name -> globInto.apply(name).flatMap(this.globInto)
		);
	}
}
