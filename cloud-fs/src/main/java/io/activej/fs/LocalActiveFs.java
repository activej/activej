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

import io.activej.async.service.EventloopService;
import io.activej.bytebuf.ByteBuf;
import io.activej.common.CollectorsEx;
import io.activej.common.MemSize;
import io.activej.common.exception.StacklessException;
import io.activej.common.exception.UncheckedException;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelConsumerTransformer;
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
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collector;

import static io.activej.async.util.LogUtils.Level.TRACE;
import static io.activej.async.util.LogUtils.toLogger;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.map;
import static io.activej.common.collection.CollectionUtils.toLimitedString;
import static io.activej.csp.dsl.ChannelConsumerTransformer.identity;
import static io.activej.fs.util.RemoteFsUtils.isWildcard;
import static io.activej.fs.util.RemoteFsUtils.ofFixedSize;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Collections.*;

/**
 * An implementation of {@link ActiveFs} which operates on a real underlying filesystem, no networking involved.
 */
public final class LocalActiveFs implements ActiveFs, EventloopService, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(LocalActiveFs.class);

	public static final String SYSTEM_DIR = ".system";
	public static final String TEMP_DIR = "temp";

	private static final char SEPARATOR_CHAR = SEPARATOR.charAt(0);
	private static final Function<String, String> toLocalName = File.separatorChar == SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(SEPARATOR_CHAR, File.separatorChar);

	private static final Function<String, String> toRemoteName = File.separatorChar == SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(File.separatorChar, SEPARATOR_CHAR);

	private final Eventloop eventloop;
	private final Path storage;
	private final Path systemDir;
	private final Path tempDir;
	private final Executor executor;

	private MemSize readerBufferSize = MemSize.kilobytes(256);
	private boolean hardlinkOnCopy = false;

	CurrentTimeProvider now;

	//region JMX
	private final PromiseStats uploadBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats uploadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadBeginPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats downloadFinishPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats listPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats infoPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats infoAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats copyAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats movePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats moveAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deletePromise = PromiseStats.create(Duration.ofMinutes(5));
	private final PromiseStats deleteAllPromise = PromiseStats.create(Duration.ofMinutes(5));
	//endregion

	// region creators
	private LocalActiveFs(Eventloop eventloop, Path storage, Executor executor) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.storage = storage;
		this.systemDir = storage.resolve(SYSTEM_DIR);
		this.tempDir = systemDir.resolve(TEMP_DIR);

		now = eventloop;
	}

	public static LocalActiveFs create(Eventloop eventloop, Executor executor, Path storageDir) {
		return new LocalActiveFs(eventloop, storageDir, executor);
	}

	/**
	 * Sets the buffer size for reading files from the filesystem.
	 */
	public LocalActiveFs withReaderBufferSize(MemSize size) {
		readerBufferSize = size;
		return this;
	}

	/**
	 * If set to {@code true}, an attempt to create a hard link will be made when copying files
	 */
	public LocalActiveFs withHardLinkOnCopy(boolean hardLinkOnCopy) {
		this.hardlinkOnCopy = hardLinkOnCopy;
		return this;
	}
	// endregion

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return doUpload(name, identity())
				.whenComplete(toLogger(logger, TRACE, "upload", name, this));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long size) {
		return doUpload(name, ofFixedSize(size))
				.whenComplete(toLogger(logger, TRACE, "upload", name, size, this));
	}

	@Override
	public Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name, long offset) {
		checkArgument(offset >= 0, "Offset cannot be less than 0");
		return execute(
				() -> {
					Path path = resolve(name);
					FileChannel channel;
					if (offset == 0) {
						channel = ensureParent(path, () -> FileChannel.open(path, CREATE, WRITE));
					} else {
						channel = FileChannel.open(path, WRITE);
					}
					if (channel.size() < offset) {
						throw ILLEGAL_OFFSET;
					}
					return channel;
				})
				.map(channel -> ChannelFileWriter.create(executor, channel)
						.withOffset(offset)
						.withAcknowledgement(ack -> ack
								.thenEx(translateKnownErrors(name))));
	}

	@Override
	public Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long limit) {
		checkArgument(offset >= 0, "offset < 0");
		checkArgument(limit >= 0, "limit < 0");

		return resolveAsync(name)
				.then(path -> execute(() -> {
					if (!Files.exists(path)) {
						throw FILE_NOT_FOUND;
					}
					FileChannel channel = FileChannel.open(path, READ);
					if (channel.size() < offset) {
						throw ILLEGAL_OFFSET;
					}
					return channel;
				}))
				.map(channel -> ChannelFileReader.create(executor, channel)
						.withBufferSize(readerBufferSize)
						.withOffset(offset)
						.withLimit(limit)
						.withEndOfStream(eos -> eos
								.thenEx(translateKnownErrors(name))
								.whenComplete(downloadFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "downloadComplete", name, offset, limit))))
				.thenEx(translateKnownErrors(name))
				.whenComplete(toLogger(logger, TRACE, "download", name, offset, limit, this))
				.whenComplete(downloadBeginPromise.recordStats());
	}

	@Override
	public Promise<Map<String, FileMetadata>> list(@NotNull String glob) {
		return execute(() -> findMatching(glob).stream()
				.collect(Collector.of(
						(Supplier<Map<String, FileMetadata>>) HashMap::new,
						(map, path) -> {
							FileMetadata metadata = toFileMetadata(path);
							if (metadata != null) {
								map.put(toFileName(path), metadata);
							}
						},
						CollectorsEx.throwingMerger())
				))
				.whenComplete(toLogger(logger, TRACE, "list", glob, this))
				.whenComplete(listPromise.recordStats());
	}

	@Override
	public Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return execute(() -> doCopy(map(name, target)))
				.thenEx(translateKnownErrors())
				.whenComplete(toLogger(logger, TRACE, "copy", name, target, this))
				.whenComplete(copyPromise.recordStats());
	}

	@Override
	public Promise<Void> copyAll(Map<String, String> sourceToTarget) {
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return execute(() -> doCopy(sourceToTarget))
				.thenEx(translateKnownErrors())
				.whenComplete(toLogger(logger, TRACE, "copyAll", toLimitedString(sourceToTarget, 50), this))
				.whenComplete(copyAllPromise.recordStats());
	}

	@Override
	public Promise<Void> move(@NotNull String name, @NotNull String target) {
		return execute(() -> doMove(map(name, target)))
				.thenEx(translateKnownErrors())
				.whenComplete(toLogger(logger, TRACE, "move", name, target, this))
				.whenComplete(movePromise.recordStats());
	}

	@Override
	public Promise<Void> moveAll(Map<String, String> sourceToTarget) {
		if (sourceToTarget.isEmpty()) return Promise.complete();

		return execute(() -> doMove(sourceToTarget))
				.thenEx(translateKnownErrors())
				.whenComplete(toLogger(logger, TRACE, "moveAll", toLimitedString(sourceToTarget, 50), this))
				.whenComplete(moveAllPromise.recordStats());
	}

	@Override
	public Promise<Void> delete(@NotNull String name) {
		return execute(() -> doDelete(singleton(name)))
				.whenComplete(toLogger(logger, TRACE, "delete", name, this))
				.whenComplete(deletePromise.recordStats());
	}

	@Override
	public Promise<Void> deleteAll(Set<String> toDelete) {
		if (toDelete.isEmpty()) return Promise.complete();

		return execute(() -> doDelete(toDelete))
				.whenComplete(toLogger(logger, TRACE, "deleteAll", toDelete, this))
				.whenComplete(deleteAllPromise.recordStats());
	}

	@Override
	public Promise<Void> ping() {
		return Promise.complete(); // local fs is always available
	}

	@Override
	public Promise<@Nullable FileMetadata> info(@NotNull String name) {
		return execute(() -> toFileMetadata(resolve(name)))
				.whenComplete(toLogger(logger, TRACE, "info", name, this))
				.whenComplete(infoPromise.recordStats());
	}

	@Override
	public Promise<Map<String, @NotNull FileMetadata>> infoAll(@NotNull Set<String> names) {
		if (names.isEmpty()) return Promise.of(emptyMap());

		return execute(
				() -> {
					Map<String, FileMetadata> result = new HashMap<>();
					for (String name : names) {
						FileMetadata metadata = toFileMetadata(resolve(name));
						if (metadata != null) {
							result.put(name, metadata);
						}
					}
					return result;
				})
				.whenComplete(toLogger(logger, TRACE, "infoAll", names, this))
				.whenComplete(infoAllPromise.recordStats());
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

	@Override
	public String toString() {
		return "LocalActiveFs{storage=" + storage + '}';
	}

	private void deleteEmptyParents(Path parent) {
		while (!parent.equals(storage)) {
			try {
				Files.deleteIfExists(parent);
				parent = parent.getParent();
			} catch (IOException ignored) {
				// either directory is not empty or some other exception
				return;
			}
		}
	}

	private Path resolve(String name) throws StacklessException {
		Path path = storage.resolve(toLocalName.apply(name)).normalize();
		if (!path.startsWith(storage) || path.startsWith(systemDir)) {
			throw BAD_PATH;
		}
		return path;
	}

	private Path resolveTemp(String name) {
		return tempDir.resolve(toLocalName.apply(name)).normalize();
	}

	private Promise<Path> resolveAsync(String name) {
		try {
			return Promise.of(resolve(name));
		} catch (StacklessException e) {
			return Promise.ofException(e);
		}
	}

	private Promise<ChannelConsumer<ByteBuf>> doUpload(String name, ChannelConsumerTransformer<ByteBuf, ChannelConsumer<ByteBuf>> transformer) {
		Path tempPath = resolveTemp(name);
		return execute(
				() -> {
					Path path = resolve(name);
					FileChannel channel = ensureParent(tempPath, () -> FileChannel.open(tempPath, CREATE_NEW, WRITE));
					if (Files.exists(path)) {
						deleteEmptyParents(tempPath);
						throw Files.isDirectory(path) ? IS_DIRECTORY : FILE_EXISTS;
					}
					return channel;
				})
				.map(channel -> ChannelFileWriter.create(executor, channel)
						.transformWith(transformer)
						.withAcknowledgement(ack -> ack
								.then(() -> execute(() -> doMove(tempPath, resolve(name))))
								.thenEx(translateKnownErrors(name))
								.whenException(() -> execute(() -> deleteEmptyParents(tempPath)))
								.whenComplete(uploadFinishPromise.recordStats())
								.whenComplete(toLogger(logger, TRACE, "uploadComplete", name, this))))
				.thenEx(translateKnownErrors(name))
				.whenComplete(uploadBeginPromise.recordStats());
	}

	private void doCopy(Map<String, String> sourceToTargetMap) throws Exception {
		for (Map.Entry<String, String> entry : sourceToTargetMap.entrySet()) {
			Path path = resolve(entry.getKey());
			Path targetPath = resolve(entry.getValue());

			if (!Files.exists(path)) throw FILE_NOT_FOUND;
			if (Files.exists(targetPath)) throw Files.isDirectory(targetPath) ? IS_DIRECTORY : FILE_EXISTS;
			if (Files.isDirectory(path)) throw IS_DIRECTORY;

			if (path.equals(targetPath)) {
				touch(path);
				continue;
			}

			ensureParent(targetPath, () -> {
				if (hardlinkOnCopy) {
					try {
						Files.createLink(targetPath, path);
						touch(targetPath);
					} catch (UnsupportedOperationException | SecurityException | FileAlreadyExistsException e) {
						// if couldn't, then just actually copy it
						Files.copy(path, targetPath);
					}
				} else {
					Files.copy(path, targetPath);
				}
				return null;
			});
		}
	}

	private void doMove(Map<String, String> sourceToTargetMap) throws Exception {
		for (Map.Entry<String, String> entry : sourceToTargetMap.entrySet()) {
			Path path = resolve(entry.getKey());
			Path targetPath = resolve(entry.getValue());

			if (!Files.exists(path)) throw FILE_NOT_FOUND;
			if (Files.exists(targetPath)) throw Files.isDirectory(targetPath) ? IS_DIRECTORY : FILE_EXISTS;
			if (Files.isDirectory(path)) throw IS_DIRECTORY;

			if (path.equals(targetPath)) {
				Files.deleteIfExists(path);
				continue;
			}

			doMove(path, targetPath);
		}
	}

	private void doMove(Path path, Path targetPath) throws Exception {
		ensureParent(targetPath, () -> {
			try {
				Files.move(path, targetPath, ATOMIC_MOVE);
				touch(targetPath);
			} catch (AtomicMoveNotSupportedException e) {
				Files.move(path, targetPath);
			}
			return null;
		});

		deleteEmptyParents(path.getParent());
	}

	private void doDelete(Set<String> toDelete) throws StacklessException, IOException {
		for (String name : toDelete) {
			Path path = resolve(name);

			// cannot delete storage
			if (path.equals(storage)) continue;

			try {
				Files.deleteIfExists(path);
			} catch (DirectoryNotEmptyException e) {
				throw IS_DIRECTORY;
			}

			deleteEmptyParents(path.getParent());
		}
	}

	private <V> V ensureParent(Path path, BlockingCallable<V> afterCreation) throws Exception {
		Path parent = path.getParent();
		while (true) {
			Files.createDirectories(parent);
			try {
				return afterCreation.call();
			} catch (NoSuchFileException e) {
				if (path.toString().equals(e.getFile())) {
					continue;
				}
				throw e;
			}
		}
	}

	private void touch(Path path) throws IOException {
		Files.setLastModifiedTime(path, FileTime.fromMillis(now.currentTimeMillis()));
	}

	private Collection<Path> findMatching(String glob) throws IOException, StacklessException {
		// optimization for 'ping' empty list requests
		if (glob.isEmpty()) {
			return emptyList();
		}

		// get strict prefix folder from the glob
		StringBuilder sb = new StringBuilder();
		String[] split = glob.split(SEPARATOR);
		for (int i = 0; i < split.length - 1; i++) {
			String part = split[i];
			if (isWildcard(part)) {
				break;
			}
			sb.append(part).append(SEPARATOR);
		}
		String subglob = glob.substring(sb.length());
		Path subdirectory = resolve(sb.toString());

		// optimization for listing all files
		if ("**".equals(subglob)) {
			List<Path> list = new ArrayList<>();
			walkFiles(subdirectory, list::add);
			return list;
		}

		// optimization for single-file requests
		if (subglob.isEmpty()) {
			return Files.isRegularFile(subdirectory) ?
					singletonList(subdirectory) :
					emptyList();
		}

		// common route
		List<Path> list = new ArrayList<>();
		PathMatcher matcher = getPathMatcher(storage.getFileSystem(), subglob);

		walkFiles(subdirectory, subglob, path -> {
			if (matcher.matches(subdirectory.relativize(path))) {
				list.add(path);
			}
		});

		return list;
	}

	private String toFileName(Path path) {
		return toRemoteName.apply(storage.relativize(path).toString());
	}

	@Nullable
	private FileMetadata toFileMetadata(Path path) {
		try {
			if (!Files.isRegularFile(path)) return null;

			long timestamp = Files.getLastModifiedTime(path).toMillis();
			return FileMetadata.of(Files.size(path), timestamp);
		} catch (IOException e) {
			throw new UncheckedException(e);
		}
	}

	@FunctionalInterface
	private interface Walker {

		void accept(Path path) throws IOException;
	}

	private void walkFiles(Path dir, Walker walker) throws IOException, StacklessException {
		walkFiles(dir, null, walker);
	}

	private void walkFiles(Path dir, @Nullable String glob, Walker walker) throws IOException, StacklessException {
		if (!Files.isDirectory(dir) || dir.startsWith(systemDir)) {
			return;
		}
		String[] parts;
		if (glob == null || (parts = glob.split(SEPARATOR))[0].contains("**")) {
			Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {

				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
					if (dir.startsWith(systemDir)) {
						return SKIP_SUBTREE;
					}
					return CONTINUE;
				}

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
		matchers[0] = getPathMatcher(fs, parts[0]);

		for (int i = 1; i < parts.length; i++) {
			String part = parts[i];
			if (part.contains("**")) {
				break;
			}
			matchers[i] = getPathMatcher(fs, part);
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
				for (int i = 0; i < Math.min(relative.getNameCount(), matchers.length); i++) {
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

	private <T> BiFunction<T, @Nullable Throwable, Promise<? extends T>> translateKnownErrors() {
		return translateKnownErrors(null);
	}

	private <T> BiFunction<T, @Nullable Throwable, Promise<? extends T>> translateKnownErrors(@Nullable String name) {
		return (v, e) -> {
			if (!(e instanceof IOException)) {
				return Promise.of(v, e);
			} else if (e instanceof FileAlreadyExistsException) {
				return execute(() -> {
					if (name != null && Files.isDirectory(resolve(name))) throw IS_DIRECTORY;
					throw FILE_EXISTS;
				});
			} else if (e instanceof NoSuchFileException) {
				return Promise.ofException(FILE_NOT_FOUND);
			}
			// e is IOException
			return execute(() -> {
				if (name != null && Files.isDirectory(resolve(name))) throw IS_DIRECTORY;
				throw (IOException) e;
			});
		};
	}

	private PathMatcher getPathMatcher(FileSystem fileSystem, String glob) throws StacklessException {
		try {
			return fileSystem.getPathMatcher("glob:" + glob);
		} catch (PatternSyntaxException | UnsupportedOperationException e) {
			throw MALFORMED_GLOB;
		}
	}

	//region JMX
	@JmxAttribute
	public PromiseStats getUploadBeginPromise() {
		return uploadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getUploadFinishPromise() {
		return uploadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadBeginPromise() {
		return downloadBeginPromise;
	}

	@JmxAttribute
	public PromiseStats getDownloadFinishPromise() {
		return downloadFinishPromise;
	}

	@JmxAttribute
	public PromiseStats getListPromise() {
		return listPromise;
	}

	@JmxAttribute
	public PromiseStats getInfoPromise() {
		return infoPromise;
	}

	@JmxAttribute
	public PromiseStats getInfoAllPromise() {
		return infoAllPromise;
	}

	@JmxAttribute
	public PromiseStats getCopyPromise() {
		return copyPromise;
	}

	@JmxAttribute
	public PromiseStats getCopyAllPromise() {
		return copyAllPromise;
	}

	@JmxAttribute
	public PromiseStats getMovePromise() {
		return movePromise;
	}

	@JmxAttribute
	public PromiseStats getMoveAllPromise() {
		return moveAllPromise;
	}

	@JmxAttribute
	public PromiseStats getDeletePromise() {
		return deletePromise;
	}

	@JmxAttribute
	public PromiseStats getDeleteAllPromise() {
		return deleteAllPromise;
	}
	//endregion
}
