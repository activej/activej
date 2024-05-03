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

import io.activej.common.ApplicationSettings;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Supplier;

import static java.lang.Integer.numberOfLeadingZeros;

final class HttpHeadersMultimap {
	static final int INITIAL_SIZE = ApplicationSettings.getInt(HttpHeadersMultimap.class, "initialSize", 4);

	Object[] kvPairs;

	public HttpHeadersMultimap(int initialSize) {
		this.kvPairs = new Object[1 << ((32 - numberOfLeadingZeros(initialSize - 1)) + 1)];
	}

	public HttpHeadersMultimap() {
		this(INITIAL_SIZE);
	}

	int size;

	@Contract(pure = true)
	public int size() {
		return size;
	}

	public void add(HttpHeader key, HttpHeaderValue value) {
		if (size++ > kvPairs.length / 4) {
			resize();
		}
		// those -2's below are ok - first -1 is to get the modulo mask
		// and second -1 is so that mask also floors the number to even
		// (because we have flat array of pairs)
		for (int i = key.hashCode() & (kvPairs.length - 2); ; i = (i + 2) & (kvPairs.length - 2)) {
			if (kvPairs[i] == null) {
				kvPairs[i] = key;
				kvPairs[i + 1] = value;
				return;
			}
		}
	}

	public void addIfAbsent(HttpHeader key, HttpHeaderValue value) {
		HttpHeaderValue existing = get(key);
		if (existing != null) return;

		add(key, value);
	}

	public void addIfAbsent(HttpHeader key, Supplier<HttpHeaderValue> valueSupplier) {
		HttpHeaderValue existing = get(key);
		if (existing != null) return;

		add(key, valueSupplier.get());
	}

	private void resize() {
		int beforeResize = this.size;

		Object[] oldKvPairs = this.kvPairs;
		this.kvPairs = new Object[this.kvPairs.length * 4];
		for (int i = 0; i != oldKvPairs.length; i += 2) {
			HttpHeader k = (HttpHeader) oldKvPairs[i];
			if (k != null) {
				HttpHeaderValue v = (HttpHeaderValue) oldKvPairs[i + 1];
				add(k, v);
			}
		}

		this.size = beforeResize;
	}

	@Contract(pure = true)
	public @Nullable HttpHeaderValue get(HttpHeader key) {
		Object[] kvPairs = this.kvPairs;
		for (int i = key.hashCode() & (kvPairs.length - 2); ; i = (i + 2) & (kvPairs.length - 2)) {
			HttpHeader k = (HttpHeader) kvPairs[i];
			if (k == null) {
				return null;
			}
			if (k.equals(key)) {
				return (HttpHeaderValue) kvPairs[i + 1];
			}
		}
	}

	public Collection<Map.Entry<HttpHeader, HttpHeaderValue>> getEntries() {
		return new AbstractCollection<>() {
			@Override
			public int size() {
				return size;
			}

			@Override
			public Iterator<Map.Entry<HttpHeader, HttpHeaderValue>> iterator() {
				return new Iterator<>() {
					int i = 0;
					@Nullable HttpHeader k;
					@Nullable HttpHeaderValue v;

					{advance();}

					private void advance() {
						for (; i < kvPairs.length; i += 2) {
							HttpHeader k = (HttpHeader) kvPairs[i];
							if (k != null) {
								this.k = k;
								this.v = (HttpHeaderValue) kvPairs[i + 1];
								i += 2;
								return;
							}
						}
						this.k = null;
						this.v = null;
					}

					@Override
					public boolean hasNext() {
						return this.k != null;
					}

					@Override
					public Map.Entry<HttpHeader, HttpHeaderValue> next() {
						if (k == null)
							throw new NoSuchElementException();
						Map.Entry<HttpHeader, HttpHeaderValue> entry = new AbstractMap.SimpleImmutableEntry<>(this.k, this.v);
						advance();
						return entry;
					}
				};
			}
		};
	}

	public void forEach(HttpHeaderValueBiPredicate predicate) {
		Object[] kvPairs = this.kvPairs;
		for (int i = 0; i < kvPairs.length - 1; i += 2) {
			HttpHeader k = (HttpHeader) kvPairs[i];
			if (k == null) {
				continue;
			}
			try {
				if (!predicate.test(k, (HttpHeaderValue) kvPairs[i + 1])) {
					break;
				}
			} catch (MalformedHttpException ignored) {
			}
		}
	}

	public void forEach(HttpHeader header, HttpHeaderValuePredicate predicate) {
		Object[] kvPairs = this.kvPairs;
		for (int i = header.hashCode() & (kvPairs.length - 2); ; i = (i + 2) & (kvPairs.length - 2)) {
			HttpHeader k = (HttpHeader) kvPairs[i];
			if (k == null) {
				break;
			}
			if (k.equals(header)) {
				try {
					if (!predicate.test((HttpHeaderValue) kvPairs[i + 1])) {
						break;
					}
				} catch (MalformedHttpException ignored) {
				}
			}
		}
	}

	@Override
	public String toString() {
		return Arrays.toString(kvPairs);
	}
}
