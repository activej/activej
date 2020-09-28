package io.activej.fs;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.isBijection;
import static io.activej.fs.util.RemoteFsUtils.escapeGlob;

@SuppressWarnings("unused")
public interface BlockingFs {
	String SEPARATOR = ActiveFs.SEPARATOR;

	OutputStream upload(@NotNull String name) throws IOException;

	OutputStream upload(@NotNull String name, long size) throws IOException;

	OutputStream append(@NotNull String name, long offset) throws IOException;

	InputStream download(@NotNull String name, long offset, long limit) throws IOException;

	default InputStream download(@NotNull String name) throws IOException {
		return download(name, 0, Long.MAX_VALUE);
	}

	void delete(@NotNull String name) throws IOException;

	default void deleteAll(Set<String> toDelete) throws IOException {
		for (String file : toDelete) {
			delete(file);
		}
	}

	/**
	 * Duplicates a file
	 *
	 * @param name   file to be copied
	 * @param target file name of copy
	 */
	default void copy(@NotNull String name, @NotNull String target) throws IOException {
		InputStream from = download(name);
		OutputStream to = upload(target);
		LocalFileUtils.copy(from, to);
	}

	default void copyAll(Map<String, String> sourceToTarget) throws IOException {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		for (Map.Entry<String, String> entry : sourceToTarget.entrySet()) {
			copy(entry.getKey(), entry.getValue());
		}
	}

	default void move(@NotNull String name, @NotNull String target) throws IOException {
		copy(name, target);
		delete(name);
	}

	default void moveAll(Map<String, String> sourceToTarget) throws IOException {
		checkArgument(isBijection(sourceToTarget), "Targets must be unique");
		for (Map.Entry<String, String> entry : sourceToTarget.entrySet()) {
			copy(entry.getKey(), entry.getValue());
		}
		deleteAll(sourceToTarget.keySet());
	}

	Map<String, FileMetadata> list(@NotNull String glob) throws IOException;

	@Nullable
	default FileMetadata info(@NotNull String name) throws IOException {
		return list(escapeGlob(name)).get(name);
	}

	default Map<String, @NotNull FileMetadata> infoAll(@NotNull Set<String> names) throws IOException {
		Map<String, @NotNull FileMetadata> result = new LinkedHashMap<>();
		for (String name : names) {
			FileMetadata info = info(name);
			if (info != null) {
				result.put(name, info);
			}
		}
		return result;
	}

	default void ping() throws IOException {
		list("");
	}

}
