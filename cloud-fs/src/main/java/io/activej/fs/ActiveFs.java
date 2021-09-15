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

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.exception.IllegalOffsetException;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.isBijection;
import static io.activej.common.Utils.transformIterator;
import static io.activej.fs.util.RemoteFsUtils.escapeGlob;
import static io.activej.fs.util.RemoteFsUtils.reduceErrors;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;

/**
 * This interface represents a simple filesystem with basic upload, append, download, copy, move, delete, info and list operations,
 * as well as with compound operations that operate on group of files.
 * <p>
 * There is no notion of a "directory", only files can be operated on.
 * <p>
 * <b>All of the paths in this file system are separated by forward slash.</b>
 * <p>
 * Most operations are executed in the same manner as would be expected from a regular file system,
 * however, some implementations may have certain limitations.
 */
public interface ActiveFs {
	String SEPARATOR = "/";

	/**
	 * Returns a consumer of bytebufs which are written (or sent) to the file.
	 * <p>
	 * So, outer promise might fail on connection try, end-of-stream promise
	 * might fail while uploading and result promise might fail when closing.
	 * <p>
	 * This method accepts files of arbitrary size.
	 * However, uploaded files appear in file system only after upload has finished successfully,
	 * e.g. {@code null} has been consumed.
	 * If an error occurs during upload, no file will be created.
	 * <p>
	 * So, the uploaded file either exists and is complete or does not exist due to some error.
	 * <p>
	 * Concurrent uploads to the same filename are allowed. If there are multiple concurrent uploads in progress, the last
	 * to successfully complete would win, e.g. would overwrite results of other uploads.
	 * <p>
	 * It is possible to overwrite existing files.
	 *
	 * @param name name of the file to upload
	 * @return promise for stream consumer of byte buffers
	 */
	Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name);

	/**
	 * Returns a consumer of bytebufs which are written (or sent) to the file.
	 * <p>
	 * So, outer promise might fail on connection try, end-of-stream promise
	 * might fail while uploading and result promise might fail when closing.
	 * <p>
	 * This method accepts files of predefined size.
	 * If there were received more or less bytes than specified, file is considered malformed,
	 * and such file would not be created.
	 * If an error occurs during upload, no file will be created.
	 * <p>
	 * So, the uploaded file either exists and its size is equal to {@code size} parameter or does not exist due to some error.
	 * <p>
	 * Concurrent uploads to the same filename are allowed. If there are multiple concurrent uploads in progress, the last
	 * to successfully complete would win, e.g. would overwrite results of other uploads.
	 * <p>
	 * It is possible to overwrite existing files.
	 *
	 * @param name name of the file to upload
	 * @return promise for stream consumer of byte buffers
	 */
	Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size);

	/**
	 * Returns a consumer of bytebufs which are appended to an existing file.
	 * <p>
	 * If the file does not exist and specified {@code offset} is 0, the new file will be created
	 * and data will be appended to the beginning of the file.
	 * <p>
	 * If an error occurs while append is in progress, all made changes will be preserved in file.
	 * <p>
	 * It makes possible to overwrite file contents starting from specific {@code offset}.
	 * If {@code offset} is greater than file size, the promise will complete with {@link IllegalOffsetException} exception.
	 * <p>
	 * Appends modify file upon each received bytebuf, hence it is possible to download still-appending file.
	 * <p>
	 * Concurrent appends to the same filename are allowed. If there are multiple concurrent appends in progress,
	 * they may overwrite each other changes.
	 *
	 * @param name name of the file to upload
	 * @return promise for stream consumer of byte buffers
	 */
	Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset);

	/**
	 * Returns a supplier of bytebufs which are read (or received) from the file.
	 * er.If the file does not exist, an error will be returned from the server.
	 * <p>
	 * Limit can be set to {@code Long.MAX_VALUE} to download all available data.
	 * <p>
	 * If the file is being appended, it is possible to download still-appending e.g. inconsistent file.
	 *
	 * @param name   name of the file to be downloaded
	 * @param offset from which byte to download the file
	 * @param limit  how many bytes of the file to download at most
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
	 * Deletes given file. The deletion of file is not guaranteed.
	 * If the file does not exist, result promise completes successfully.
	 *
	 * @param name name of the file to be deleted
	 * @return marker promise that completes when deletion completes
	 */
	Promise<Void> delete(@NotNull String name);

	/**
	 * Tries to delete specified files. Basically, calls {@link #delete} for each file.
	 * <p>
	 * If error occurs while deleting any of the files, result promise is completed exceptionally.
	 * Always completes successfully if empty set is passed as an argument.
	 * <p>
	 * Implementations should override this method with optimized version if possible.
	 *
	 * @param toDelete set of files to be deleted
	 */
	default Promise<Void> deleteAll(Set<String> toDelete) {
		return Promises.toList(toDelete.stream().map(name -> delete(name).toTry()))
				.whenResult(tries -> reduceErrors(tries, toDelete.iterator()))
				.toVoid();
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
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		Set<Entry<String, String>> entrySet = sourceToTarget.entrySet();
		return Promises.toList(entrySet.stream()
				.map(entry -> copy(entry.getKey(), entry.getValue()).toTry()))
				.whenResult(tries -> reduceErrors(tries, transformIterator(entrySet.iterator(), Entry::getKey)))
				.toVoid();
	}

	/**
	 * Moves file from one location to another. Basically, copies the file and then tries to remove the original file.
	 * <p>
	 * <b>This method is non-idempotent</b>
	 *
	 * @param name   file to be moved
	 * @param target new file name
	 */
	default Promise<Void> move(@NotNull String name, @NotNull String target) {
		return copy(name, target)
				.then(() -> name.equals(target) ? Promise.complete() : delete(name));
	}

	/**
	 * Moves files from source locations to target locations.
	 * Basically, calls {@link #copyAll} with given map and then calls {@link #deleteAll} with source files.
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
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return copyAll(sourceToTarget)
				.then(() -> deleteAll(sourceToTarget.entrySet().stream()
						.filter(entry -> !entry.getKey().equals(entry.getValue()))
						.map(Entry::getKey)
						.collect(toSet())));
	}

	/**
	 * Lists files that are matched by glob.
	 * Be sure to escape meta chars if your paths contain them.
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
						.whenResult(Objects::nonNull, metadata -> result.put(name, metadata))))
				.map($ -> result);
	}

	/**
	 * Send a ping request.
	 * <p>
	 * This method is suitable for checking availability of the file system
	 * (whether server is up in case of remote implementation, for example).
	 */
	default Promise<Void> ping() {
		return list("").toVoid();
	}

}
