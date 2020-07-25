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

import static io.activej.bytebuf.ByteBufStrings.*;
import static io.activej.common.Checks.checkArgument;

/**
 * This is a case-insensitive token registry used as permanent header value cache.
 * Its purpose is to map header values to references to Java objects with maximum efficiency and speed.
 */
public final class CaseInsensitiveTokenMap<T extends Token> {
	public abstract static class Token {
		protected final byte[] lowerCaseBytes;
		protected final int lowerCaseHashCode;

		protected Token(@Nullable byte[] lowerCaseBytes, int lowerCaseHashCode) {
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
			if (b >= 'A' && b <= 'Z') {
				b += 'a' - 'A';
			}
			lowerCaseBytes[i] = b;
			lowerCaseHashCode = lowerCaseHashCode * 31 + b;
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
			if (t.lowerCaseHashCode == lowerCaseHashCode && equalsLowerCaseAscii(t.lowerCaseBytes, bytes, offset, length)) {
				return t;
			}
		}
		return null;
	}

	@FunctionalInterface
	public interface TokenFactory<T> {

		T create(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode);
	}
}
