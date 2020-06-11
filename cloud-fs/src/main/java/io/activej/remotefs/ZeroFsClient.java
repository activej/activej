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
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;

/**
 * This fs client simulates a situation in which all paths point outside root
 */
public final class ZeroFsClient implements FsClient {
	public static final ZeroFsClient INSTANCE = new ZeroFsClient();

	private ZeroFsClient() {
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return Promise.ofException(FILE_NOT_FOUND);
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		return Promise.ofException(FILE_NOT_FOUND);
	}

	@Override
	public Promise<Void> move(@NotNull String filename, @NotNull String target) {
		return Promise.ofException(BAD_PATH);
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return Promise.ofException(BAD_PATH);
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return Promise.of(emptyList());
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return Promise.ofException(BAD_PATH);
	}

	@Override
	public FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from) {
		return this;
	}

	@Override
	public FsClient strippingPrefix(@NotNull String prefix) {
		return this;
	}

	@Override
	public FsClient filter(@NotNull Predicate<String> predicate) {
		return this;
	}
}
