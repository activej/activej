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
import io.activej.csp.ChannelSuppliers;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.remotefs.RemoteFsUtils.escapeGlob;

/**
 * This interface represents a simple filesystem client with upload, download, move, delete and list operations.
 */
public interface FsClient {
	StacklessException FILE_NOT_FOUND = new StacklessException(FsClient.class, "File not found");
	StacklessException FILE_EXISTS = new StacklessException(FsClient.class, "File already exists");
	StacklessException BAD_PATH = new StacklessException(FsClient.class, "Given file name points to file outside root");
	StacklessException OFFSET_TOO_BIG = new StacklessException(FsClient.class, "Offset exceeds the actual file size");
	StacklessException LENGTH_TOO_BIG = new StacklessException(FsClient.class, "Length with offset exceeds the actual file size");
	StacklessException BAD_RANGE = new StacklessException(FsClient.class, "Given offset or length don't make sense");
	StacklessException IS_DIRECTORY = new StacklessException(FsClient.class, "Operated file is a directory");

	/**
	 * Returns a consumer of bytebufs which are written (or sent) to the file.
	 * <p>
	 * So, outer promise might fail on connection try, end-of-stream promise
	 * might fail while uploading and result promise might fail when closing.
	 *
	 * Note that this method expects that you're uploading immutable files.
	 *
	 * @param name   name of the file to upload
	 * @return promise for stream consumer of byte buffers
	 */
	Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name);

	/**
	 * Returns a supplier of bytebufs which are read (or received) from the file.
	 * If file does not exist, or specified range goes beyond it's size,
	 * an error will be returned from the server.
	 * <p>
	 * Length can be set to -1 to download all available data.
	 *
	 * @param name   name of the file to be downloaded
	 * @param offset from which byte to download the file
	 * @param length how much bytes of the file do download
	 * @return promise for stream supplier of byte buffers
	 * @see #download(String, long)
	 * @see #download(String)
	 */
	Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length);

	// region download shortcuts

	/**
	 * Shortcut for downloading the whole file from given offset.
	 *
	 * @return stream supplier of byte buffers
	 * @see #download(String, long, long)
	 * @see #download(String)
	 */
	default Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset) {
		return download(name, offset, -1);
	}

	/**
	 * Shortcut for downloading the whole available file.
	 *
	 * @param name name of the file to be downloaded
	 * @return stream supplier of byte buffers
	 * @see #download(String, long)
	 * @see #download(String, long, long)
	 */
	default Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name) {
		return download(name, 0, -1);
	}
	// endregion

	/**
	 * Deletes given file.
	 *
	 * @param name name of the file to be deleted
	 * @return marker promise that completes when deletion completes
	 */
	Promise<Void> delete(@NotNull String name);

	/**
	 * Duplicates a file
	 *
	 * @param name   file to be copied
	 * @param target new file name
	 */

	default Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return ChannelSuppliers.streamTo(download(name), upload(target));
	}

	/**
	 * Moves (renames) a file from one name to another.
	 * Equivalent to copying a file to new location and
	 * then deleting the original file.
	 *
	 * @param name   file to be moved
	 * @param target new file name
	 */

	default Promise<Void> move(@NotNull String name, @NotNull String target) {
		return copy(name, target)
				.then(() -> delete(name));
	}

	default Promise<Void> moveDir(@NotNull String name, @NotNull String target) {
		String finalName = name.endsWith("/") ? name : name + '/';
		String finalTarget = target.endsWith("/") ? target : target + '/';
		return list(finalName + "**")
				.then(list -> Promises.all(list.stream()
						.map(meta -> {
							String filename = meta.getName();
							return move(filename, finalTarget + filename.substring(finalName.length()));
						})));
	}

	/**
	 * Lists files that are matched by glob.
	 * Be sure to escape metachars if your paths contain them.
	 * <p>
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return list of {@link FileMetadata file metadata}
	 */
	Promise<List<FileMetadata>> list(@NotNull String glob);

	/**
	 * Shortcut to get {@link FileMetadata metadata} of a single file.
	 *
	 * @param name name of a file to fetch its metadata.
	 * @return promise of file description or <code>null</code>
	 */
	default Promise<@Nullable FileMetadata> getMetadata(@NotNull String name) {
		return list(escapeGlob(name))
				.map(list -> list.isEmpty() ? null : list.get(0));
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
		return new TransformFsClient(this, into, from, $ -> Optional.of("**"));
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
