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

import io.activej.common.exception.parse.ParseException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Stream;

import static io.activej.common.Preconditions.checkArgument;

/**
 * This is a POJO for holding name, size, timestamp and revision of some file
 */
public final class FileMetadata {
	public static final Comparator<FileMetadata> COMPARATOR =
			Comparator.comparingLong(FileMetadata::getSize)
					.thenComparing(FileMetadata::getTimestamp);

	private final String name;
	private final long size;
	private final long timestamp;

	private FileMetadata(String name, long size, long timestamp) {
		this.name = name;
		this.size = size;
		this.timestamp = timestamp;
	}

	public static FileMetadata of(@NotNull String name, long size, long timestamp) {
		checkArgument(size >= 0, "size >= 0");
		return new FileMetadata(name, size, timestamp);
	}

	public static FileMetadata parse(String name, long size, long timestamp) throws ParseException {
		if (name == null) {
			throw new ParseException(FileMetadata.class, "Name is null");
		}
		if (size < 0) {
			throw new ParseException(FileMetadata.class, "Size is less than zero");
		}
		return new FileMetadata(name, size, timestamp);
	}

	public FileMetadata withName(String name) {
		return new FileMetadata(name, size, timestamp);
	}

	public String getName() {
		return name;
	}

	public long getSize() {
		return size;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return name + "(size=" + size + ", timestamp=" + timestamp + ')';
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

		return size == that.size && timestamp == that.timestamp && name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return 29791 * name.hashCode()
				+ 961 * ((int) (size ^ (size >>> 32)))
				+ 31 * ((int) (timestamp ^ (timestamp >>> 32)));
	}

	@Nullable
	public static FileMetadata getMoreCompleteFile(@Nullable FileMetadata first, @Nullable FileMetadata second) {
		return first == null ? second : second == null ? first : COMPARATOR.compare(first, second) > 0 ? first : second;
	}

	public static List<FileMetadata> flatten(Stream<List<FileMetadata>> streamOfLists) {
		Map<String, FileMetadata> map = new HashMap<>();
		streamOfLists
				.flatMap(List::stream)
				.forEach(meta -> map.compute(meta.getName(), ($, existing) -> getMoreCompleteFile(existing, meta)));
		return new ArrayList<>(map.values());
	}
}
