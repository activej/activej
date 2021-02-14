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

import io.activej.bytebuf.ByteBuf;

import static io.activej.bytebuf.ByteBufStrings.encodeAscii;

/**
 * This enum represents an HTTP method.
 */
public enum HttpMethod {
	GET, PUT, POST, HEAD, DELETE, CONNECT, OPTIONS, TRACE, PATCH,
	SEARCH, COPY, MOVE, LOCK, UNLOCK, MKCOL, PROPFIND, PROPPATCH;

	final byte[] bytes;
	final int size;

	HttpMethod() {
		this.bytes = encodeAscii(this.name());
		this.size = this.bytes.length;
	}

	public byte[] bytes() {
		return bytes;
	}

	void write(ByteBuf buf) {
		buf.put(bytes);
	}

	boolean compareTo(byte[] array, int offset, int size) {
		if (bytes.length != size)
			return false;
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] != array[offset + i])
				return false;
		}
		return true;
	}
}
