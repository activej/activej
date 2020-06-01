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

package io.activej.datastream.stats;

import io.activej.bytebuf.ByteBuf;

import java.util.Collection;

@FunctionalInterface
public interface StreamStatsSizeCounter<T> {
	int size(T item);

	static StreamStatsSizeCounter<ByteBuf> forByteBufs() {
		return ByteBuf::readRemaining;
	}

	static <T extends Collection<?>> StreamStatsSizeCounter<T> forCollections() {
		return Collection::size;
	}

	static <T> StreamStatsSizeCounter<T[]> forArrays() {
		return item -> item.length;
	}

	static StreamStatsSizeCounter<String> forStrings() {
		return String::length;
	}
}
