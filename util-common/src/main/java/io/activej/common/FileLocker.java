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

package io.activej.common;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;

import static io.activej.common.Checks.checkState;

@SuppressWarnings("unused")
public final class FileLocker implements Closeable {
	private static final Logger logger = LoggerFactory.getLogger(FileLocker.class);

	public static synchronized FileLocker obtainLockOrDie(String filename) {
		FileLocker fileLocker = new FileLocker(filename);
		if (!fileLocker.obtainLock()) {
			logger.error("Could not obtain lock for {}", filename);
			throw new RuntimeException("Could not obtain lock");
		}
		return fileLocker;
	}

	private final File lockFile;
	@Nullable
	private FileOutputStream lockStream;

	@Nullable
	private FileLock fileLock;

	public FileLocker(String lockFile) {
		this(new File(lockFile));
	}

	public FileLocker(File lockFile) {
		this.lockFile = lockFile.getAbsoluteFile();
	}

	public synchronized void obtainLockOrDie() {
		if (!obtainLock()) {
			logger.error("Could not obtain lock for {}", this);
			throw new RuntimeException("Could not obtain lock");
		}
	}

	@SuppressWarnings({"WeakerAccess", "ResultOfMethodCallIgnored"})
	public synchronized boolean obtainLock() {
		try {
			File parentDir = lockFile.getCanonicalFile().getParentFile();
			if (parentDir != null) {
				parentDir.mkdirs();
				checkState(parentDir.isDirectory(), "Cannot create directory %s", parentDir);
			}
			lockStream = new FileOutputStream(lockFile);
			lockStream.write(0);
			fileLock = lockStream.getChannel().tryLock();
			return fileLock != null;
		} catch (IOException e) {
			logger.error("IO Exception during locking {}", lockFile, e);
			return false;
		}
	}

	public synchronized void releaseLock() {
		try {
			releaseFileLock();
		} catch (IOException e) {
			logger.error("IO Exception during releasing FileLock on {}", lockFile, e);
		}
		try {
			closeLockStream();
		} catch (IOException e) {
			logger.error("IO Exception during closing FileOutputStream on {}", lockFile, e);
		}
	}

	private void releaseFileLock() throws IOException {
		if (fileLock != null) {
			fileLock.release();
			fileLock = null;
		}
	}

	private void closeLockStream() throws IOException {
		if (lockStream != null) {
			lockStream.close();
			lockStream = null;
		}
	}

	public boolean isLocked() {
		return fileLock != null;
	}

	@Override
	public void close() throws IOException {
		releaseFileLock();
		closeLockStream();
	}

	@Override
	public String toString() {
		return lockFile.toString();
	}

}
