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

import io.activej.fs.LocalFileUtils.IORunnable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class ForwardingOutputStream extends OutputStream {
	protected final OutputStream peer;

	public ForwardingOutputStream(OutputStream peer) {
		this.peer = peer;
	}

	@Override
	public final void write(int b) throws IOException {
		run(() -> {
			onBytes(1);
			peer.write(b);
		});
	}

	@Override
	public final void write(@NotNull byte[] b) throws IOException {
		run(() -> {
			onBytes(b.length);
			peer.write(b);
		});
	}

	@Override
	public final void write(@NotNull byte[] b, int off, int len) throws IOException {
		run(() -> {
			onBytes(len);
			peer.write(b, off, len);
		});
	}

	@Override
	public final void flush() throws IOException {
		run(peer::flush);
	}

	@Override
	public final void close() throws IOException {
		run(() -> {
			peer.close();
			onClose();
		});
	}

	protected void onBytes(int len) throws IOException {
	}

	protected void onInternalError(IOException e) throws IOException {
	}

	protected void onClose() throws IOException {
	}

	private void run(IORunnable runnable) throws IOException {
		try {
			runnable.run();
		} catch (IOException e) {
			try {
				onInternalError(e);
			} catch (IOException e2) {
				e2.addSuppressed(e);
				throw e2;
			}
			throw e;
		}
	}

}
