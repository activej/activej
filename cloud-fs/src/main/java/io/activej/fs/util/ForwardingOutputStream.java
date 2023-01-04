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
import java.io.OutputStream;

@SuppressWarnings("NullableProblems")
public class ForwardingOutputStream extends OutputStream{
	protected final OutputStream peer;

	public ForwardingOutputStream(OutputStream peer) {
		this.peer = peer;
	}

	@Override
	public void write(byte[] b) throws IOException {
		peer.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		peer.write(b, off, len);
	}

	@Override
	public void flush() throws IOException {
		peer.flush();
	}

	@Override
	public void write(int b) throws IOException {
		peer.write(b);
	}

	@Override
	public void close() throws IOException {
		peer.close();
	}
}
