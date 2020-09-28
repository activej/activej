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

package io.activej.fs.util;

import java.io.IOException;
import java.io.InputStream;

public final class LimitedInputStream extends InputStream {
	private final InputStream peer;
	private long remaining;

	public LimitedInputStream(InputStream peer, long maxBytes) {
		this.peer = peer;
		remaining = Math.max(maxBytes, 0);
	}

	@Override
	public int available() throws IOException {
		return (int) Math.min(peer.available(), remaining);
	}

	@Override
	public void close() throws IOException {
		peer.close();
	}

	@Override
	public int read(byte[] buf, int off, int len) throws IOException {
		if (remaining > 0) {
			int nRead = peer.read(
					buf, off, (int) Math.min(len, remaining));

			remaining -= nRead;

			return nRead;
		} else {
			return -1;
		}
	}

	@Override
	public int read(byte[] buf) throws IOException {
		return this.read(buf, 0, buf.length);
	}

	@Override
	public long skip(long n) throws IOException {
		long skipped = peer.skip(Math.min(n, remaining));

		remaining -= skipped;

		return skipped;
	}

	@Override
	public int read() throws IOException {
		if (remaining > 0) {
			int c = peer.read();

			if (c >= 0) {
				remaining -= 1;
			}

			return c;
		} else {
			return -1;
		}
	}
}
