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

package io.activej.serializer;

@SuppressWarnings("DeprecatedIsStillUsed")
public enum CompatibilityLevel {
	/**
	 * Provides basic version of serializer
	 */
	@Deprecated LEVEL_1(1, false),

	/**
	 * Provides string optimizations for ISO8859-1 and UTF8
	 */
	@Deprecated LEVEL_2(2, false),

	/**
	 * Includes previous optimizations and provides nullable optimization for enum, subclass, array, map and list
	 */
	LEVEL_3(3, false),

	/**
	 * Same as {@link #LEVEL_3} but provides little endian format for JVM intrinsics
	 *
	 * @see #LEVEL_3
	 */
	LEVEL_3_LE(3, true),

	/**
	 * Includes previous optimizations and provides nullable optimization for boolean
	 */
	LEVEL_4(4, false),

	/**
	 * Same as {@link #LEVEL_4} but provides little endian format for JVM intrinsics
	 *
	 * @see #LEVEL_4
	 */
	LEVEL_4_LE(4, true);

	private final int level;
	private final boolean littleEndian;

	CompatibilityLevel(int level, boolean littleEndian) {
		this.level = level;
		this.littleEndian = littleEndian;
	}

	public int getLevel() {
		return level;
	}

	public boolean isLittleEndian() {
		return littleEndian;
	}
}
