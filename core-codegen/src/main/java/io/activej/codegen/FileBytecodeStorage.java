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

package io.activej.codegen;

import io.activej.codegen.util.WithInitializer;

import java.io.*;
import java.nio.file.Path;
import java.util.Optional;

/**
 * A bytecode storage that uses file system to save/load bytecode
 */
public final class FileBytecodeStorage extends AbstractBytecodeStorage implements WithInitializer<FileBytecodeStorage> {
	private static final String CLASS_FILE_EXTENSION = ".class";

	private final Path storageDir;

	private FileBytecodeStorage(Path storageDir) {
		this.storageDir = storageDir;
	}

	/**
	 * Creates a new {@link BytecodeStorage} that saves/loads bytecode to/from a given directory
	 *
	 * @param storageDir a directory to load/store bytecode
	 * @return a new instance of a {@code FileBytecodeStorage}
	 */
	public static FileBytecodeStorage create(Path storageDir) {
		return new FileBytecodeStorage(storageDir);
	}

	@Override
	protected Optional<InputStream> getInputStream(String className) {
		try {
			FileInputStream fileInputStream = new FileInputStream(storageDir.resolve(className + CLASS_FILE_EXTENSION).toFile());
			return Optional.of(fileInputStream);
		} catch (FileNotFoundException ignored) {
			return Optional.empty();
		}
	}

	@Override
	protected Optional<OutputStream> getOutputStream(String className) throws IOException {
		return Optional.of(new FileOutputStream(storageDir.resolve(className + CLASS_FILE_EXTENSION).toFile()));
	}
}
