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

package io.activej.http.loader;

import io.activej.bytebuf.ByteBuf;
import io.activej.promise.Promise;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.activej.bytebuf.ByteBuf.wrapForReading;

class CacheStaticLoader implements AsyncStaticLoader {
	public static final byte[] NOT_FOUND = {};

	private final AsyncStaticLoader resourceLoader;
	private final Function<String, byte[]> get;
	private final BiConsumer<String, byte[]> put;

	public CacheStaticLoader(AsyncStaticLoader resourceLoader, Function<String, byte[]> get, BiConsumer<String, byte[]> put) {
		this.resourceLoader = resourceLoader;
		this.get = get;
		this.put = put;
	}

	@Override
	public Promise<ByteBuf> load(String path) {
		byte[] bytes = get.apply(path);
		if (bytes == NOT_FOUND) {
			return Promise.ofException(new ResourceNotFoundException("Could not find '" + path + '\''));
		} else if (bytes != null) {
			return Promise.of(wrapForReading(bytes));
		} else {
			return doLoad(path);
		}
	}

	private Promise<ByteBuf> doLoad(String path) {
		return resourceLoader.load(path)
				.whenResult(buf -> put.accept(path, buf.getArray()))
				.whenException(ResourceNotFoundException.class, e -> put.accept(path, NOT_FOUND));
	}
}
