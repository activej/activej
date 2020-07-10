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
import io.activej.common.exception.StacklessException;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.remotefs.util.RemoteFsUtils.escapeGlob;
import static java.util.Collections.emptyMap;

/**
 * This interface represents a simple filesystem client with upload, download, copy, delete and list operations.
 */
public interface FsClient {
	StacklessException FILE_NOT_FOUND = new StacklessException(FsClient.class, "File not found");
	StacklessException FILE_EXISTS = new StacklessException(FsClient.class, "File already exists");
	StacklessException BAD_PATH = new StacklessException(FsClient.class, "Given file name points to file outside root");
	StacklessException BAD_RANGE = new StacklessException(FsClient.class, "Given offset or limit doesn't make sense");
	StacklessException IS_DIRECTORY = new StacklessException(FsClient.class, "Operated file is a directory");
	StacklessException MALFORMED_GLOB = new StacklessException(FsClient.class, "Malformed glob pattern");
	StacklessException ILLEGAL_OFFSET = new StacklessException(FsClient.class, "Offset exceeds file size");

	/**
	 * Returns a consumer of bytebufs which are written (or sent) to the file.
	 * <p>
	 * So, outer promise might fail on connection try, end-of-stream promise
	 * might fail while uploading and result promise might fail when closing.
	 * <p>
	 * Note that this method expects that you're uploading immutable files.
	 *
	 * @param name name of the file to upload
	 * @return promise for stream consumer of byte buffers
	 */
	Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name);

	Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size);

	Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset);

	/**
	 * Returns a supplier of bytebufs which are read (or received) from the file.
	 * If file does not exist an error will be returned from the server.
	 * <p>
	 * Length can be set to {@code Long.MAX_VALUE} to download all available data.
	 *
	 * @param name   name of the file to be downloaded
	 * @param offset from which byte to download the file
	 * @param limit  how much bytes of the file to download at most
	 * @return promise for stream supplier of byte buffers
	 * @see #download(String)
	 */
	Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit);

	// region download shortcuts

	/**
	 * Shortcut for downloading the whole available file.
	 *
	 * @param name name of the file to be downloaded
	 * @return stream supplier of byte buffers
	 * @see #download(String, long, long)
	 */
	default Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name) {
		return download(name, 0, Long.MAX_VALUE);
	}
	// endregion

	/**
	 * Tries to delete given file. The deletion of file is not guaranteed.
	 *
	 * @param name name of the file to be deleted
	 * @return marker promise that completes when deletion completes
	 */
	Promise<Void> delete(@NotNull String name);

	/**
	 * Tries to delete specified files. Basically, calls {@link #delete} for each file.
	 * A best effort will be made to delete all files, but some files may remain intact.
	 * <p>
	 * If error occurs while deleting any of the files, result promise is completed exceptionally.
	 * Always completes successfully if empty set is passed as an argument.
	 * <p>
	 * Implementations should override this method with optimized version if possible.
	 *
	 * @param toDelete set of files to be deleted
	 */
	default Promise<Void> deleteAll(Set<String> toDelete) {
		return Promises.all(toDelete.stream().map(this::delete));
	}

	/**
	 * Duplicates a file
	 *
	 * @param name   file to be copied
	 * @param target file name of copy
	 */
	default Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return download(name).then(supplier -> supplier.streamTo(upload(target)));
	}

	/**
	 * Duplicates files from source locations to target locations.
	 * Basically, calls {@link #copy} for each pair of source file and target file.
	 * Source to target mapping is passed as a map where keys correspond to source files
	 * and values correspond to target files.
	 * <p>
	 * If error occurs while copying any of the files, result promise is completed exceptionally.
	 * Always completes successfully if empty map is passed as an argument.
	 * <p>
	 * Implementations should override this method with optimized version if possible.
	 *
	 * @param sourceToTarget source files to target files mapping
	 */
	default Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return Promises.all(sourceToTarget.entrySet().stream()
				.map(entry -> copy(entry.getKey(), entry.getValue())));
	}

	/**
	 * Moves file from one location to another. Basically, copies the file and then tries to remove the original file.
	 * As {@link #delete} does not guarantee that file will actually be deleted, so does this method.
	 * <p>
	 * <b>This method is non-idempotent</b>
	 *
	 * @param name   file to be moved
	 * @param target new file name
	 */
	default Promise<Void> move(@NotNull String name, @NotNull String target) {
		return copy(name, target)
				.then(() -> delete(name));
	}

	/**
	 * Moves files from source locations to target locations.
	 * Basically, calls {@link #copyAll} with given map and then calls {@link #deleteAll} with source files.
	 * As {@link #deleteAll)} does not guarantee that files will actually be deleted, so does this method.
	 * Source to target mapping is passed as a map where keys correspond to source files
	 * and values correspond to target files.
	 * <p>
	 * If error occurs while moving any of the files, result promise is completed exceptionally.
	 * Always completes successfully if empty map is passed as an argument.
	 * <p>
	 * Implementations should override this method with optimized version if possible.
	 * <p>
	 * <b>This method is non-idempotent</b>
	 *
	 * @param sourceToTarget source files to target files mapping
	 */
	default Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return copyAll(sourceToTarget)
				.then(() -> deleteAll(sourceToTarget.keySet()));
	}

	/**
	 * Lists files that are matched by glob.
	 * Be sure to escape metachars if your paths contain them.
	 * <p>
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return map of filenames to {@link FileMetadata file metadata}
	 */
	Promise<Map<String, FileMetadata>> list(@NotNull String glob);

	/**
	 * Shortcut to get {@link FileMetadata metadata} of a single file.
	 *
	 * @param name name of a file to fetch its metadata.
	 * @return promise of file description or <code>null</code>
	 */
	default Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return list(escapeGlob(name))
				.map(list -> list.get(name));
	}

	/**
	 * Shortcut to get {@link FileMetadata metadata} of multiple files.
	 *
	 * @param names names of files to fetch metadata for.
	 * @return map of filenames to their corresponding {@link FileMetadata metadata}.
	 * Map contains metadata for existing files only
	 */
	default Promise<Map<String, @NotNull FileMetadata>> infoAll(@NotNull Set<String> names) {
		if (names.isEmpty()) return Promise.of(emptyMap());

		Map<String, FileMetadata> result = new HashMap<>();
		return Promises.all(names.stream()
				.map(name -> info(name)
						.whenResult(metadata -> {
							if (metadata != null) {
								result.put(name, metadata);
							}
						})))
				.map($ -> result);
	}

	/**
	 * Send a ping request.
	 * <p>
	 * Used to check availability of the fs
	 * (is server up in case of remote implementation, for example).
	 */
	default Promise<Void> ping() {
		return list("").toVoid();
	}

	static FsClient zero() {
		return ZeroFsClient.INSTANCE;
	}

	default FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from, @NotNull Function<String, Optional<String>> globInto) {
		return new TransformFsClient(this, into, from, globInto);
	}

	default FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from) {
		return transform(into, from, $ -> Optional.empty());
	}

	// similar to 'chroot'
	default FsClient addingPrefix(@NotNull String prefix) {
		if (prefix.length() == 0) {
			return this;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(
				name -> Optional.of(prefix + name),
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(escapedPrefix + name)
		);
	}

	// similar to 'cd'
	default FsClient subfolder(@NotNull String folder) {
		if (folder.length() == 0) {
			return this;
		}
		return addingPrefix(folder.endsWith("/") ? folder : folder + '/');
	}

	default FsClient strippingPrefix(@NotNull String prefix) {
		if (prefix.length() == 0) {
			return this;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(prefix + name),
				name -> Optional.of(name.startsWith(escapedPrefix) ? name.substring(escapedPrefix.length()) : "**")
		);
	}

	default FsClient filter(@NotNull Predicate<String> predicate) {
		return new FilterFsClient(this, predicate);
	}

	default FsClient mount(@NotNull String mountpoint, @NotNull FsClient client) {
		return new MountingFsClient(this, map(mountpoint, client.strippingPrefix(mountpoint + '/')));
	}
}
