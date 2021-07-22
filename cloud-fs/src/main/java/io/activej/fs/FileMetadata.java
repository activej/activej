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

import com.dslplatform.json.CompiledJson;
import io.activej.common.exception.InvalidSizeException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;

/**
 * This is a POJO for holding name, size, timestamp and revision of some file
 */
public final class FileMetadata {
	public static final Comparator<FileMetadata> COMPARATOR =
			Comparator.comparingLong(FileMetadata::getSize)
					.thenComparing(FileMetadata::getTimestamp);

	private final long size;
	private final long timestamp;

	private FileMetadata(long size, long timestamp) {
		this.size = size;
		this.timestamp = timestamp;
	}

	@CompiledJson
	public static FileMetadata of(long size, long timestamp) {
		checkArgument(size >= 0, "size >= 0");
		return new FileMetadata(size, timestamp);
	}

	public static FileMetadata decode(long size, long timestamp) throws InvalidSizeException {
		if (size < 0) {
			throw new InvalidSizeException("Size is less than zero");
		}
		return new FileMetadata(size, timestamp);
	}

	public long getSize() {
		return size;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "FileMetadata{size=" + size + ", timestamp=" + timestamp + '}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		FileMetadata that = (FileMetadata) o;

		return size == that.size && timestamp == that.timestamp;
	}

	@Override
	public int hashCode() {
		int result = (int) (size ^ (size >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Nullable
	public static FileMetadata getMoreCompleteFile(@Nullable FileMetadata first, @Nullable FileMetadata second) {
		return first == null ? second : second == null ? first : COMPARATOR.compare(first, second) > 0 ? first : second;
	}

	public static Map<String, FileMetadata> flatten(Stream<Map<String, @NotNull FileMetadata>> streamOfMaps) {
		Map<String, FileMetadata> result = new HashMap<>();
		streamOfMaps
				.flatMap(map -> map.entrySet().stream())
				.forEach(entry -> result.compute(entry.getKey(), ($, existing) -> getMoreCompleteFile(existing, entry.getValue())));
		return result;
	}
}
