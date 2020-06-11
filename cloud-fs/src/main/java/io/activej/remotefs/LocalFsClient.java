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

import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.MemSize;
import io.activej.common.exception.StacklessException;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelConsumers;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.promise.Promise.BlockingCallable;
import io.activej.promise.Promise.BlockingRunnable;
import io.activej.promise.jmx.PromiseStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Preconditions.checkArgument;
import static io.activej.remotefs.RemoteFsUtils.isWildcard;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * An implementation of {@link FsClient} which operates on a real underlying filesystem, no networking involved.
 */
public final class LocalFsClient implements FsClient, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(LocalFsClient.class);

	public static final char FILE_SEPARATOR_CHAR = '/';

	public static final String FILE_SEPARATOR = String.valueOf(FILE_SEPARATOR_CHAR);

	private static final Function<String, String> toLocalName = File.separatorChar == FILE_SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(FILE_SEPARATOR_CHAR, File.separatorChar);

	private static final Function<String, String> toRemoteName = File.separatorChar == FILE_SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(File.separatorChar, FILE_SEPARATOR_CHAR);

	private final Eventloop eventloop;
	private final Path storage;
	private final Executor executor;

	private MemSize readerBufferSize = MemSize.kilobytes(256);

	CurrentTimeProvider now;

	//region JMX
	private final PromiseStats writeBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats writeFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats readBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats readFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleMovePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleCopyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats singleDeletePromise = PromiseStats.create(Duration.ofMinutes(5));
	//endregion

	// region creators
	private LocalFsClient(Eventloop eventloop, Path storage, Executor executor) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storage = storage;

		now = eventloop;
	}

	public static LocalFsClient create(Eventloop eventloop, Executor executor, Path storageDir) {
		return new LocalFsClient(eventloop, storageDir, executor);
	}

	/**
	 * Sets the buffer size for reading files from the filesystem.
	 */
	public LocalFsClient withReaderBufferSize(MemSize size) {
		readerBufferSize = size;
		return this;
	}
	// endregion

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return execute(
				() -> {
					Path path = resolve(name);
					if (Files.isDirectory(path)) throw IS_DIRECTORY;
					Files.createDirectories(path.getParent());
					return path;
				})
				.then(path -> ChannelFileWriter.open(executor, path, CREATE_NEW, WRITE))
				.thenEx((writer, e) -> {
					if (e instanceof FileAlreadyExistsException) {
						// since file already exists, it is the same file that is being uploaded
						return Promise.of(ChannelConsumers.<ByteBuf>recycling());
					}
					return Promise.of(writer, e);
				})
				.map(consumer -> consumer
						.withAcknowledgement(ack -> ack
								.whenComplete(writeFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "writing to file", name, this))))
				.whenComplete(writeBeginPromise.recordStats())
				.whenComplete(toLogger(logger, TRACE, "upload", name, this));
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length) {
		checkArgument(offset >= 0, "offset < 0");
		checkArgument(length >= -1, "length < -1");

		return resolveAsync(name)
				.then(path -> ChannelFileReader.open(executor, path)
						.thenEx(translateKnownErrors(path)))
				.map(consumer -> consumer
						.withBufferSize(readerBufferSize)
						.withOffset(offset)
						.withLength(length == -1 ? Long.MAX_VALUE : length)
						.withEndOfStream(eos -> eos.whenComplete(readFinishPromise.recordStats())))
				.whenComplete(toLogger(logger, TRACE, "download", name, offset, length, this))
				.whenComplete(readBeginPromise.recordStats());
	}

	@Override
	public Promise<List<FileMetadata>> list(@NotNull String glob) {
		return execute(() -> findMatching(glob).stream()
				.map(this::toFileMetadataSafe)
				.filter(Objects::nonNull)
				.collect(toList()))
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return execute(
				() -> {
					Path path = resolve(name);
					Path targetPath = resolve(target);

					if (Files.isDirectory(path) || Files.isDirectory(targetPath)) {
						throw IS_DIRECTORY;
					}
					// noop when paths are equal
					if (path.equals(targetPath)) {
						return;
					}
					// cannot move into existing file
					if (Files.isRegularFile(targetPath)) {
						throw FILE_EXISTS;
					}

					if (Files.isRegularFile(path)) {
						Files.createDirectories(targetPath.getParent());
						Files.move(path, targetPath, ATOMIC_MOVE);
					} else {
						Files.deleteIfExists(targetPath);
					}
				})
				.whenComplete(toLogger(logger, TRACE, "move", name, target, this))
				.whenComplete(singleMovePromise.recordStats());
	}

	@Override
	public Promise<Void> moveDir(@NotNull String name, @NotNull String target) {
		String finalName = name.endsWith("/") ? name : name + '/';
		String finalTarget = target.endsWith("/") ? target : target + '/';

		Path from, to;
		try {
			from = resolve(finalName);
			to = resolve(finalTarget);
		} catch (StacklessException e) {
			return Promise.ofException(e);
		}

		return execute(
				() -> {
					if (Files.isRegularFile(to)) {
						throw FILE_EXISTS;
					}
					return Files.isDirectory(to);
				})
				.then(isDir -> {
					if (isDir) {
						return FsClient.super.moveDir(name, target);
					}
					return execute(() -> {
						if (!Files.isDirectory(from)) return;
						try {
							Files.move(from, to, ATOMIC_MOVE);
						} catch (AtomicMoveNotSupportedException e) {
							Files.move(from, to);
						}
					});
				});
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return execute(() -> doCopy(name, target))
				.whenComplete(toLogger(logger, TRACE, "copy", name, target, this))
				.whenComplete(singleCopyPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return execute(() -> doDelete(name))
				.whenComplete(toLogger(logger, TRACE, "delete", name, this))
				.whenComplete(singleDeletePromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete(); // local fs is always available
	}

	@Override
	public Promise<FileMetadata> getMetadata(@NotNull String name) {
		return execute(() -> toFileMetadata(resolve(name)));
	}

	@Override
	public FsClient subfolder(@NotNull String folder) {
		if (folder.length() == 0) {
			return this;
		}
		try {
			LocalFsClient client = new LocalFsClient(eventloop, resolve(folder), executor);
			client.readerBufferSize = readerBufferSize;
			return client;
		} catch (StacklessException e) {
			// when folder points outside of the storage directory
			throw new IllegalArgumentException("illegal subfolder: " + folder, e);
		}
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@NotNull
	@Override
	public Promise<Void> start() {
		return execute((BlockingRunnable) () -> Files.createDirectories(storage));
	}

	@NotNull
	@Override
	public Promise<Void> stop() {
		return Promise.complete();
	}

	public Promise<Void> remove(String name) {
		return execute((BlockingRunnable) () -> Files.deleteIfExists(resolve(name)));
	}

	@Override
	public String toString() {
		return "LocalFsClient{storage=" + storage + '}';
	}

	private Path resolve(String name) throws StacklessException {
		Path path = storage.resolve(toLocalName.apply(name)).normalize();
		if (!path.startsWith(storage)) {
			throw BAD_PATH;
		}
		return path;
	}

	private Promise<Path> resolveAsync(String name) {
		try {
			return Promise.of(resolve(name));
		} catch (StacklessException e) {
			return Promise.ofException(e);
		}
	}

	private void tryHardlinkOrCopy(Path path, Path targetPath) throws IOException {
		if (!Files.deleteIfExists(targetPath)) {
			Files.createDirectories(targetPath.getParent());
		}
		try {
			// try to create a hardlink
			Files.createLink(targetPath, path);
		} catch (UnsupportedOperationException | SecurityException e) {
			// if couldn't, then just actually copy it
			Files.copy(path, targetPath);
		}
	}

	private void doCopy(String name, String target) throws StacklessException, IOException {
		Path path = resolve(name);
		if (!Files.exists(path)) {
			throw FILE_NOT_FOUND;
		}
		Path targetPath = resolve(target);

		if (Files.isDirectory(path) || Files.isDirectory(targetPath)) {
			throw IS_DIRECTORY;
		}
		// noop when paths are equal
		if (path.equals(targetPath)) {
			return;
		}

		// cannot move into existing file
		if (Files.isRegularFile(targetPath)) {
			throw FILE_EXISTS;
		}

		tryHardlinkOrCopy(path, targetPath);
	}

	private void doDelete(String name) throws IOException, StacklessException {
		Path path = resolve(name);

		if (Files.isDirectory(path)) {
			throw IS_DIRECTORY;
		}

		Files.deleteIfExists(path);
	}

	private Collection<Path> findMatching(String glob) throws IOException, StacklessException {
		// optimization for 'ping' empty list requests
		if (glob.isEmpty()) {
			return emptyList();
		}

		// get strict prefix folder from the glob
		StringBuilder sb = new StringBuilder();
		String[] split = glob.split(FILE_SEPARATOR);
		for (int i = 0; i < split.length - 1; i++) {
			String part = split[i];
			if (isWildcard(part)) {
				break;
			}
			sb.append(part).append(FILE_SEPARATOR_CHAR);
		}
		String subglob = glob.substring(sb.length());
		Path subfolder = resolve(sb.toString());

		// optimization for listing all files
		if ("**".equals(subglob)) {
			List<Path> list = new ArrayList<>();
			walkFiles(subfolder, list::add);
			return list;
		}

		// optimization for single-file requests
		if (subglob.isEmpty()) {
			return Files.isRegularFile(subfolder) ?
					singletonList(subfolder) :
					emptyList();
		}

		// common route
		List<Path> list = new ArrayList<>();
		PathMatcher matcher = storage.getFileSystem().getPathMatcher("glob:" + subglob);

		walkFiles(subfolder, subglob, path -> {
			if (matcher.matches(subfolder.relativize(path))) {
				list.add(path);
			}
		});

		return list;
	}

	@Nullable
	private FileMetadata toFileMetadata(Path path) throws IOException, StacklessException {
		if (!Files.exists(path)) return null;
		if (Files.isDirectory(path)) throw IS_DIRECTORY;

		String filename = toRemoteName.apply(storage.relativize(path).toString());
		long timestamp = Files.getLastModifiedTime(path).toMillis();
		return FileMetadata.of(filename, Files.size(path), timestamp);
	}

	@Nullable
	private FileMetadata toFileMetadataSafe(Path path) {
		try {
			return toFileMetadata(path);
		} catch (StacklessException | IOException e) {
			return null;
		}
	}

	@FunctionalInterface
	interface Walker {

		void accept(Path path) throws IOException;
	}

	private void walkFiles(Path dir, Walker walker) throws IOException {
		walkFiles(dir, null, walker);
	}

	private void walkFiles(Path dir, @Nullable String glob, Walker walker) throws IOException {
		if (!Files.isDirectory(dir)) {
			return;
		}
		String[] parts;
		if (glob == null || (parts = glob.split(FILE_SEPARATOR))[0].contains("**")) {
			Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					walker.accept(file);
					return CONTINUE;
				}

				@Override
				public FileVisitResult visitFileFailed(Path file, IOException exc) {
					logger.warn("Failed to visit file {}", storage.relativize(file), exc);
					return CONTINUE;
				}
			});
			return;
		}

		FileSystem fs = dir.getFileSystem();

		PathMatcher[] matchers = new PathMatcher[parts.length];
		matchers[0] = fs.getPathMatcher("glob:" + parts[0]);

		for (int i = 1; i < parts.length; i++) {
			String part = parts[i];
			if (part.contains("**")) {
				break;
			}
			matchers[i] = fs.getPathMatcher("glob:" + part);
		}

		Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				walker.accept(file);
				return CONTINUE;
			}

			@Override
			public FileVisitResult preVisitDirectory(Path subdir, BasicFileAttributes attrs) {
				if (subdir.equals(dir)) {
					return CONTINUE;
				}
				Path relative = dir.relativize(subdir);
				for (int i = 0; i < relative.getNameCount(); i++) {
					PathMatcher matcher = matchers[i];
					if (matcher == null) {
						return CONTINUE;
					}
					if (!matcher.matches(relative.getName(i))) {
						return SKIP_SUBTREE;
					}
				}
				return CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) {
				logger.warn("Failed to visit file {}", storage.relativize(file), exc);
				return CONTINUE;
			}
		});
	}

	private <T> Promise<T> execute(BlockingCallable<T> callable) {
		return Promise.ofBlockingCallable(executor, callable);
	}

	private Promise<Void> execute(BlockingRunnable runnable) {
		return Promise.ofBlockingRunnable(executor, runnable);
	}

	private <T> BiFunction<T, @Nullable Throwable, Promise<? extends T>> translateKnownErrors(Path path) {
		return (value, e) -> {
			if (e instanceof FileAlreadyExistsException) {
				return Promise.ofException(FILE_EXISTS);
			} else if (e instanceof NoSuchFileException) {
				return Promise.ofException(FILE_NOT_FOUND);
			} else if (e instanceof FileSystemException) {
				return execute(() -> {
					if (Files.isDirectory(path)) {
						throw IS_DIRECTORY;
					}
					throw (FileSystemException) e;
				});
			}
			return Promise.of(value, e);
		};
	}

	//region JMX
	@JmxAttribute
	public PromiseStats getWriteBeginPromise() {
		return writeBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getWriteFinishPromise() {
		return writeFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getReadBeginPromise() {
		return readBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getReadFinishPromise() {
		return readFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getSingleMovePromise() {
		return singleMovePromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getSingleCopyPromise() {
		return singleCopyPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getSingleDeletePromise() {
		return singleDeletePromise;
	}
	//endregion
}
