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

/**
 * Utility class that provides hashing functions based on the MurmurHash3 algorithm.
 * <p>
 * MurmurHash3 is a non-cryptographic hash function suitable for general hash-based lookup.
 * This class contains methods that allow hashing of long and int values.
 */
public final class HashUtils {

	/**
	 * Applies the MurmurHash3 hashing algorithm to a given long value.
	 *
	 * @param k the long value to be hashed
	 * @return the hashed value
	 */
	public static long murmur3hash(long k) {
		k ^= k >>> 33;
		k *= 0xff51afd7ed558ccdL;
		k ^= k >>> 33;
		k *= 0xc4ceb9fe1a85ec53L;
		k ^= k >>> 33;
		return k;
	}

	/**
	 * Applies the MurmurHash3 hashing algorithm to a given int value.
	 *
	 * @param k the int value to be hashed
	 * @return the hashed value as an int
	 */
	public static int murmur3hash(int k) {
		return (int) murmur3hash((long) k);
	}
}
