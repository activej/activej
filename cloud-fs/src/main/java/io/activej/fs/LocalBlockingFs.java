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

import io.activej.common.CollectorsEx;
import io.activej.common.exception.UncheckedException;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.fs.exception.scalar.ForbiddenPathException;
import io.activej.fs.util.ForwardingOutputStream;
import io.activej.fs.util.LimitedInputStream;
import io.activej.jmx.api.ConcurrentJmxBean;
import io.activej.service.BlockingService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.activej.fs.LocalFileUtils.*;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Collections.emptyMap;

public final class LocalBlockingFs implements BlockingFs, BlockingService, ConcurrentJmxBean {
	private static final Logger logger = LoggerFactory.getLogger(LocalBlockingFs.class);

	public static final String DEFAULT_TEMP_DIR = ".upload";

	private static final char SEPARATOR_CHAR = SEPARATOR.charAt(0);
	private static final Function<String, String> toLocalName = File.separatorChar == SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(SEPARATOR_CHAR, File.separatorChar);

	private static final Function<String, String> toRemoteName = File.separatorChar == SEPARATOR_CHAR ?
			Function.identity() :
			s -> s.replace(File.separatorChar, SEPARATOR_CHAR);

	private final Path storage;

	private boolean hardlinkOnCopy = false;
	private Path tempDir;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region creators
	private LocalBlockingFs(Path storage) {
		this.storage = storage;
		this.tempDir = storage.resolve(DEFAULT_TEMP_DIR);
	}

	public static LocalBlockingFs create(Path storageDir) {
		return new LocalBlockingFs(storageDir);
	}

	/**
	 * If set to {@code true}, an attempt to create a hard link will be made when copying files
	 */
	@SuppressWarnings("UnusedReturnValue")
	public LocalBlockingFs withHardLinkOnCopy(boolean hardLinkOnCopy) {
		this.hardlinkOnCopy = hardLinkOnCopy;
		return this;
	}

	/**
	 * Sets a temporary directory for files to be stored while uploading.
	 */
	public LocalBlockingFs withTempDir(Path tempDir) {
		this.tempDir = tempDir;
		return this;
	}
	// endregion

	@Override
	public OutputStream upload(@NotNull String name) throws IOException {
		Path tempPath = Files.createTempFile(tempDir, "", "");
		return new ForwardingOutputStream(new FileOutputStream(tempPath.toFile())) {
			@Override
			protected void onClose() throws IOException {
				doMove(tempPath, resolve(name));
			}
		};
	}

	@Override
	public OutputStream upload(@NotNull String name, long size) throws IOException {
		Path tempPath = Files.createTempFile(tempDir, "", "");
		return new ForwardingOutputStream(new FileOutputStream(tempPath.toFile())) {
			long totalSize;

			@Override
			protected void onBytes(int len) throws IOException {
				if ((totalSize += len) > size) throwOnSizeMismatch();
			}

			@Override
			protected void onClose() throws IOException {
				if (totalSize != size) throwOnSizeMismatch();
				doMove(tempPath, resolve(name));
			}

			private void throwOnSizeMismatch() throws IOException {
				peer.close();
				Files.deleteIfExists(tempPath);
				throw new IOException("Size mismatch");
			}
		};
	}

	@Override
	public OutputStream append(@NotNull String name, long offset) throws IOException {
		return new FileOutputStream(resolve(name).toFile(), true);
	}

	@Override
	public InputStream download(@NotNull String name, long offset, long limit) throws IOException {
		Path path = resolve(name);
		if (offset > Files.size(path)){
			throw new IOException("Offset exceeds file size");
		}
		FileInputStream fileInputStream = new FileInputStream(path.toFile());

		//noinspection ResultOfMethodCallIgnored
		fileInputStream.skip(offset);
		return new LimitedInputStream(fileInputStream, limit);
	}

	@Override
	public void delete(@NotNull String name) throws IOException {
		Path path = resolve(name);
		// cannot delete storage
		if (path.equals(storage)) return;

		Files.deleteIfExists(path);
	}

	@Override
	public void copy(@NotNull String name, @NotNull String target) throws IOException {
		Path path = resolve(name);
		if (!Files.isRegularFile(path)) {
			throw new FileNotFoundException("File '" + name + "' not found");
		}
		Path targetPath = resolve(target);

		if (path.equals(targetPath)) {
			touch(path, now);
			return;
		}

		ensureTarget(path, targetPath, () -> {
			if (hardlinkOnCopy) {
				LocalFileUtils.copyViaHardlink(path, targetPath, now);
			} else {
				Files.copy(path, targetPath, REPLACE_EXISTING);
			}
			return null;
		});

	}

	@Override
	public void move(@NotNull String name, @NotNull String target) throws IOException {
		Path path = resolve(name);
		if (!Files.isRegularFile(path)) {
			throw new FileNotFoundException("File '" + name + "' not found");
		}
		Path targetPath = resolve(target);
		if (path.equals(targetPath)) {
			touch(path, now);
			return;
		}
		doMove(path, targetPath);
	}

	@Override
	public Map<String, FileMetadata> list(@NotNull String glob) throws IOException {
		if (glob.isEmpty()) return emptyMap();

		String subdir = extractSubDir(glob);
		Path subdirectory = resolve(subdir);
		String subglob = glob.substring(subdir.length());

		return findMatching(tempDir, subglob, subdirectory).stream()
				.collect(Collector.of(
						(Supplier<Map<String, FileMetadata>>) HashMap::new,
						(map, path) -> {
							FileMetadata metadata = toFileMetadata(path);
							if (metadata != null) {
								String filename = toRemoteName.apply(storage.relativize(path).toString());
								map.put(filename, metadata);
							}
						},
						CollectorsEx.throwingMerger())
				);
	}

	@Override
	@Nullable
	public FileMetadata info(@NotNull String name) throws IOException {
		return toFileMetadata(resolve(name));
	}

	@Override
	public void ping() {
		// local fs is always available
	}

	@Override
	public void start() throws IOException {
		LocalFileUtils.init(storage, tempDir);
	}

	@Override
	public void stop() {
	}

	@Override
	public String toString() {
		return "LocalBlockingFs{storage=" + storage + '}';
	}

	private static FileMetadata toFileMetadata(Path path) {
		try {
			return LocalFileUtils.toFileMetadata(path);
		} catch (IOException e) {
			logger.warn("Failed to retrieve metadata for {}", path, e);
			throw new UncheckedException(e);
		}
	}

	private Path resolve(String name) throws IOException {
		try {
			return LocalFileUtils.resolve(storage, tempDir, toLocalName.apply(name));
		} catch (ForbiddenPathException e) {
			throw new FileSystemException(name, null, e.getMessage());
		}
	}

	private void doMove(Path path, Path targetPath) throws IOException {
		ensureTarget(path, targetPath, () -> {
			LocalFileUtils.moveViaHardlink(path, targetPath, now);
			return null;
		});
	}

}
