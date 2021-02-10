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
import static io.activej.http.HttpUtils.hashCodeLowerCaseAscii;

/**
 * This is a case-insensitive token registry used as permanent header value cache.
 * Its purpose is to map header values to references to Java objects with maximum efficiency and speed.
 */
public final class CaseInsensitiveTokenMap<T extends Token> {
	public abstract static class Token {
		protected final byte[] bytes;
		protected final int offset;
		protected final int length;

		protected final byte[] lowerCaseBytes;
		protected final int lowerCaseHashCode;

		protected Token(byte[] bytes, int offset, int length, @Nullable byte[] lowerCaseBytes, int lowerCaseHashCode) {
			this.bytes = bytes;
			this.offset = offset;
			this.length = length;
			this.lowerCaseBytes = lowerCaseBytes;
			this.lowerCaseHashCode = lowerCaseHashCode;
		}
	}

	protected final T[] TOKENS;
	protected final int maxProbings;
	private final TokenFactory<T> factory;

	protected CaseInsensitiveTokenMap(int slotsNumber, int maxProbings, Class<T> elementsType, TokenFactory<T> factory) {
		checkArgument(Integer.bitCount(slotsNumber) == 1);
		this.maxProbings = maxProbings;
		this.factory = factory;
		//noinspection unchecked
		TOKENS = (T[]) Array.newInstance(elementsType, slotsNumber);
	}

	/**
	 * Creates a token with given value and places it in the registry, also returning it.
	 */
	public final T register(String name) {
		T token = create(name);

		for (int p = 0; p < maxProbings; p++) {
			int slot = (token.lowerCaseHashCode + p) & (TOKENS.length - 1);
			if (TOKENS[slot] == null) {
				TOKENS[slot] = token;
				return token;
			}
		}
		throw new IllegalArgumentException("CaseInsensitiveTokenMap hash collision, try to increase size");
	}

	/**
	 * Creates a token with given value but does not place it into the registry.
	 */
	public final T create(String name) {
		byte[] bytes = encodeAscii(name);

		byte[] lowerCaseBytes = new byte[bytes.length];
		int lowerCaseHashCode = 1;
		for (int i = 0; i < bytes.length; i++) {
			byte b = bytes[i];
			b |= 0x20;
			lowerCaseBytes[i] = b;
			lowerCaseHashCode = lowerCaseHashCode + b;
		}

		return factory.create(bytes, 0, bytes.length, lowerCaseBytes, lowerCaseHashCode);
	}

	/**
	 * @see #getOrCreate(byte[], int, int, int)
	 */
	public final T getOrCreate(byte[] bytes, int offset, int length) {
		int lowerCaseHashCode = hashCodeLowerCaseAscii(bytes, offset, length);
		return getOrCreate(bytes, offset, length, lowerCaseHashCode);
	}

	/**
	 * Gets the cached token value for given input, or creates a new one without putting it in the registry.
	 */
	public final T getOrCreate(byte[] bytes, int offset, int length, int lowerCaseHashCode) {
		T t = get(bytes, offset, length, lowerCaseHashCode);
		return t != null ? t : factory.create(bytes, offset, length, null, lowerCaseHashCode);
	}

	/**
	 * Returns registered token value for given input, or null if no such value was registered.
	 * This method is very fast.
	 */
	public final T get(byte[] bytes, int offset, int length, int lowerCaseHashCode) {
		for (int p = 0; p < maxProbings; p++) {
			int slot = (lowerCaseHashCode + p) & (TOKENS.length - 1);
			T t = TOKENS[slot];
			if (t == null) {
				break;
			}
			if (t.lowerCaseHashCode != lowerCaseHashCode) continue;
			int i = 0;
			for (; i < length; i++) {
				if (bytes[offset + i] != t.bytes[i]) {
					break;
				}
			}
			if (i == length) return t;

			if (slowPathEquals(bytes, offset, length, t, i)) return t;
		}
		return null;
	}

	private boolean slowPathEquals(byte[] bytes, int offset, int length, T t, int i) {
		for (; i < length; i++) {
			if (bytes[offset + i] != t.lowerCaseBytes[i]) {
				return false;
			}
		}
		if (i == length) return true;
		return equalsLowerCaseAscii(t.lowerCaseBytes, i, bytes, offset, length);
	}

	private static boolean equalsLowerCaseAscii(byte[] lowerCasePattern, int patternOffset, byte[] array, int offset, int size) {
		if (lowerCasePattern.length != size)
			return false;
		for (int i = patternOffset; i < size; i++) {
			byte p = lowerCasePattern[i];
			byte a = array[offset + i];

			if (a != p) {
				// 32 =='a' - 'A'
				if (a < 'A' || a > 'Z' || p - a != 32) return false;
			}
		}
		return true;
	}

	@FunctionalInterface
	public interface TokenFactory<T> {

		T create(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode);
	}
}
