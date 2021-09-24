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

package io.activej.common;

import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;

/**
 * Encapsulates a size in bytes as a convenient abstraction
 */
public final class MemSize implements Comparable<MemSize> {
	public static final MemSize ZERO = new MemSize(0);

	public static final long KB = 1024;
	public static final long MB = 1024 * KB;
	public static final long GB = 1024 * MB;
	public static final long TB = 1024 * GB;

	private final long bytes;

	private MemSize(long bytes) {
		this.bytes = bytes;
	}

	/**
	 * Creates a {@link MemSize} of n bytes
	 *
	 * @param bytes a number of bytes
	 * @return a new instance of {@link MemSize} of n bytes
	 */
	public static @NotNull MemSize of(long bytes) {
		checkArgument(bytes >= 0, "Cannot create MemSize of negative value");
		return new MemSize(bytes);
	}

	/**
	 * Creates a {@link MemSize} of n bytes
	 *
	 * @param bytes a number of bytes
	 * @return a new instance of {@link MemSize} of n bytes
	 * @see #of(long)
	 */
	public static @NotNull MemSize bytes(long bytes) {
		return MemSize.of(bytes);
	}

	/**
	 * Creates a {@link MemSize} of n kilobytes
	 *
	 * @param kilobytes a number of kilobytes
	 * @return a new instance of {@link MemSize} of n kilobytes
	 */
	public static @NotNull MemSize kilobytes(long kilobytes) {
		checkArgument(kilobytes <= Long.MAX_VALUE / KB, "Resulting number of bytes exceeds Long.MAX_VALUE");
		return of(kilobytes * KB);
	}

	/**
	 * Creates a {@link MemSize} of n megabytes
	 *
	 * @param megabytes a number of megabytes
	 * @return a new instance of {@link MemSize} of n megabytes
	 */
	public static @NotNull MemSize megabytes(long megabytes) {
		checkArgument(megabytes <= Long.MAX_VALUE / MB, "Resulting number of bytes exceeds Long.MAX_VALUE");
		return of(megabytes * MB);
	}

	/**
	 * Creates a {@link MemSize} of n gigabytes
	 *
	 * @param gigabytes a number of gigabytes
	 * @return a new instance of {@link MemSize} of n gigabytes
	 */
	public static @NotNull MemSize gigabytes(long gigabytes) {
		checkArgument(gigabytes <= Long.MAX_VALUE / GB, "Resulting number of bytes exceeds Long.MAX_VALUE");
		return of(gigabytes * GB);
	}

	/**
	 * Creates a {@link MemSize} of n terabytes
	 *
	 * @param terabytes a number of terabytes
	 * @return a new instance of {@link MemSize} of n terabytes
	 */
	public static @NotNull MemSize terabytes(long terabytes) {
		checkArgument(terabytes <= Long.MAX_VALUE / TB, "Resulting number of bytes exceeds Long.MAX_VALUE");
		return of(terabytes * TB);
	}

	/**
	 * @return a number of bytes is this {@link MemSize}
	 */
	public long toLong() {
		return bytes;
	}

	/**
	 * @return a number of bytes is this {@link MemSize} as {@code int}
	 * @throws IllegalStateException if number of bytes is greater than {@link Integer#MAX_VALUE}
	 */
	public int toInt() {
		if (bytes > Integer.MAX_VALUE) throw new IllegalStateException("MemSize exceeds Integer.MAX_VALUE: " + bytes);
		return (int) bytes;
	}

	/**
	 * Returns a new {@link MemSize} which has a number of bytes equal to the result of a
	 * mapping function applied to the number of bytes of this {@link MemSize}
	 *
	 * @param fn a mapping function that maps current size to a new one
	 * @return a new {@link MemSize} with mapped number of bytes
	 */
	public @NotNull MemSize map(@NotNull Function<Long, Long> fn) {
		return MemSize.of(fn.apply(bytes));
	}

	/**
	 * Creates a new {@link MemSize} out of a given {@link String}
	 *
	 * @param string a string to be parsed
	 * @return a parsed {@link MemSize}
	 * @throws IllegalArgumentException on malformed string
	 * @see StringFormatUtils#parseMemSize(String)
	 */
	public static @NotNull MemSize valueOf(@NotNull String string) {
		return StringFormatUtils.parseMemSize(string);
	}

	/**
	 * Formats this {@link MemSize} as a string
	 * <p>
	 * A string format is the same as {@link #valueOf(String)} method parses
	 *
	 * @return a string that represents a {@link MemSize}
	 * @see StringFormatUtils#formatMemSize(MemSize)
	 */
	public @NotNull String format() {
		return StringFormatUtils.formatMemSize(this);
	}

	@Override
	public int compareTo(@NotNull MemSize o) {
		return Long.compare(bytes, o.bytes);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		MemSize memSize = (MemSize) o;

		return bytes == memSize.bytes;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(bytes);
	}

	@Override
	public @NotNull String toString() {
		return "" + toLong();
	}
}
