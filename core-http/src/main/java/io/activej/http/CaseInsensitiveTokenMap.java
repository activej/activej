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

package io.activej.http;

import io.activej.http.CaseInsensitiveTokenMap.Token;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;
import static io.activej.common.Checks.checkArgument;
import static io.activej.http.HttpUtils.hashCodeCI;

/**
 * This is a case-insensitive token registry used as permanent header value cache.
 * Its purpose is to map header values to references to Java objects with maximum efficiency and speed.
 */
public final class CaseInsensitiveTokenMap<T extends Token> {
	public abstract static class Token {
		protected final int hashCodeCI;
		protected final byte[] bytes;
		protected final int offset;
		protected final int length;
		protected final byte @Nullable [] lowerCase;

		protected Token(int hashCodeCI, byte[] bytes, int offset, int length, byte @Nullable [] lowerCase) {
			this.hashCodeCI = hashCodeCI;
			this.bytes = bytes;
			this.offset = offset;
			this.length = length;
			this.lowerCase = lowerCase;
		}
	}

	private final T[] tokens;
	private final int maxProbings;
	private final TokenFactory<T> factory;

	CaseInsensitiveTokenMap(int slotsNumber, int maxProbings, Class<T> elementsType, TokenFactory<T> factory) {
		checkArgument(Integer.bitCount(slotsNumber) == 1);
		this.maxProbings = maxProbings;
		this.factory = factory;
		//noinspection unchecked
		this.tokens = (T[]) Array.newInstance(elementsType, slotsNumber);
	}

	/**
	 * Creates a token with given value and places it in the registry, also returning it.
	 */
	public T register(String name) {
		T token = create(name);

		for (int p = 0; p < maxProbings; p++) {
			int slot = (token.hashCodeCI + p) & (tokens.length - 1);
			if (tokens[slot] == null) {
				tokens[slot] = token;
				return token;
			}
		}
		throw new IllegalArgumentException("CaseInsensitiveTokenMap hash collision, try to increase size");
	}

	/**
	 * Creates a token with given value but does not place it into the registry.
	 */
	public T create(String name) {
		byte[] bytes = encodeAscii(name);

		byte[] lowerCase = new byte[bytes.length];
		int hashCodeCI = 0;
		for (int i = 0; i < bytes.length; i++) {
			byte b = bytes[i];
			lowerCase[i] = (b >= 'A' && b <= 'Z') ? (byte) (b + 'a' - 'A') : b;
			hashCodeCI += (b | 0x20);
		}

		return factory.create(hashCodeCI, bytes, 0, bytes.length, lowerCase);
	}

	/**
	 * @see #getOrCreate(int, byte[], int, int)
	 */
	public T getOrCreate(byte[] bytes, int offset, int length) {
		int hashCodeCI = hashCodeCI(bytes, offset, length);
		return getOrCreate(hashCodeCI, bytes, offset, length);
	}

	/**
	 * Gets the cached token value for given input, or creates a new one without putting it in the registry.
	 */
	public T getOrCreate(int hashCodeCI, byte[] bytes, int offset, int length) {
		T t = get(hashCodeCI, bytes, offset, length);
		return t != null ? t : factory.create(hashCodeCI, bytes, offset, length, null);
	}

	/**
	 * Returns registered token value for given input, or null if no such value was registered.
	 */
	public T get(int hashCodeCI, byte[] array, int offset, int length) {
		int slot = hashCodeCI & (tokens.length - 1);
		T t = tokens[slot];
		if (t == null) return null;
		byte[] bytes = t.bytes;
		if (t.hashCodeCI == hashCodeCI && bytes.length == length) {
			int i = 0;
			for (; i < length; i++) {
				if (array[offset + i] != bytes[i]) {
					assert t.lowerCase != null;
					if (equalsLowerCase(i, t.lowerCase, array, offset, length)) return t;
					return probeNext(hashCodeCI, array, offset, length);
				}
			}
			return t;
		}
		return probeNext(hashCodeCI, array, offset, length);
	}

	private T probeNext(int hashCodeCI, byte[] array, int offset, int length) {
		probe:
		for (int p = 1; p < maxProbings; p++) {
			int slot = (hashCodeCI + p) & (tokens.length - 1);
			T t = tokens[slot];
			if (t == null) break;
			byte[] bytes = t.bytes;
			if (t.hashCodeCI == hashCodeCI && bytes.length == length) {
				int i = 0;
				for (; i < length; i++) {
					if (array[offset + i] != bytes[i]) {
						assert t.lowerCase != null;
						if (equalsLowerCase(i, t.lowerCase, array, offset, length)) return t;
						continue probe;
					}
				}
				return t;
			}
		}
		return null;
	}

	private static boolean equalsLowerCase(int i, byte[] lowerCase, byte[] array, int offset, int length) {
		assert lowerCase.length == length;
		for (; i < length; i++) {
			if (array[offset + i] != lowerCase[i]) {
				return equalsCaseInsensitive(i, lowerCase, array, offset, length);
			}
		}
		return true;
	}

	private static boolean equalsCaseInsensitive(int i, byte[] lowerCase, byte[] array, int offset, int length) {
		assert lowerCase.length == length;
		for (; i < length; i++) {
			byte p = lowerCase[i];
			byte b = array[offset + i];
			if (b == p) continue;
			if (b >= 'A' && b <= 'Z' && b + ('a' - 'A') == p) continue;
			return false;
		}
		return true;
	}

	@FunctionalInterface
	public interface TokenFactory<T> {
		T create(int hashCodeCI, byte[] bytes, int offset, int length, byte @Nullable [] lowerCaseBytes);
	}
}
