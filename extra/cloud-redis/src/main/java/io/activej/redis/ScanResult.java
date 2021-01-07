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

package io.activej.redis;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public final class ScanResult {
	private final Charset charset;
	private final String cursor;
	private final List<byte[]> elements;

	ScanResult(Charset charset, String cursor, List<byte[]> elements) {
		this.charset = charset;
		this.cursor = cursor;
		this.elements = elements;
	}

	public String getCursor() {
		return cursor;
	}

	public int getCursorAsInt() {
		return Integer.parseInt(cursor);
	}

	public long getCursorAsLong() {
		return Long.parseLong(cursor);
	}

	public BigInteger getCursorAsBigInteger(){
		return new BigInteger(cursor);
	}

	public List<String> getElements() {
		List<String> result = new ArrayList<>(elements.size());
		for (byte[] element : elements) {
			result.add(new String(element, charset));
		}
		return result;
	}

	public List<byte[]> getElementsAsBinary() {
		return elements;
	}
}
