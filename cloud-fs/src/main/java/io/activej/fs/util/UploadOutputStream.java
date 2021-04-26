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

import io.activej.fs.LocalFileUtils.FileTransporter;
import io.activej.fs.LocalFileUtils.IORunnable;
import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.activej.fs.LocalFileUtils.tryFsync;

public class UploadOutputStream extends OutputStream {
	private final Path tempPath;
	private final Path targetPath;
	private final boolean synced;
	private final boolean syncedMetadata;
	private final FileTransporter transporter;
	private final OutputStream peer;

	private boolean closed;

	public UploadOutputStream(Path tempPath, Path targetPath, boolean synced, boolean syncedMetadata, FileTransporter transporter) throws FileNotFoundException {
		this.tempPath = tempPath;
		this.targetPath = targetPath;
		this.synced = synced;
		this.syncedMetadata = syncedMetadata;
		this.transporter = transporter;
		this.peer = new FileOutputStream(tempPath.toString());
	}

	@Override
	public final void write(int b) throws IOException {
		run(() -> {
			onBytes(1);
			peer.write(b);
		});
	}

	@Override
	public final void write(byte @NotNull [] b) throws IOException {
		run(() -> {
			onBytes(b.length);
			peer.write(b);
		});
	}

	@Override
	public final void write(byte @NotNull [] b, int off, int len) throws IOException {
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
			if (closed) return;
			closed = true;
			onClose();
			peer.close();
			if (synced) {
				tryFsync(tempPath);
			}
			transporter.transport(tempPath, targetPath);
			if (syncedMetadata) {
				tryFsync(targetPath.getParent());
			}
		});
	}

	protected void onBytes(int len) throws IOException {
	}

	protected void onClose() throws IOException {
	}

	private void run(IORunnable runnable) throws IOException {
		try {
			runnable.run();
		} catch (Exception e) {
			try {
				peer.close();
				Files.deleteIfExists(tempPath);
			} catch (Exception e2) {
				e2.addSuppressed(e);
				throw e2;
			}
			throw e;
		}
	}

}
