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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

/**
 * An abstract {@link BytecodeStorage} that allows loading and saving bytecode
 * using {@link InputStream} and {@link OutputStream}
 */
public abstract class AbstractBytecodeStorage implements BytecodeStorage {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final int DEFAULT_BUFFER_SIZE = 8192;

	/**
	 * Returns an {@link Optional} of a {@link InputStream}
	 * that will be used to load a bytecode for a given class name.
	 * If optional is empty, that means there is no bytecode stored for a given class name
	 *
	 * @param className a class name for which bytecode will be loaded
	 * @return an optional of a bytecode input stream
	 * @throws IOException if an I/O error occurs
	 */
	protected abstract Optional<InputStream> getInputStream(String className) throws IOException;

	/**
	 * Returns an {@link Optional} of a {@link OutputStream}
	 * that will be used to save a bytecode for a given class name.
	 * If optional is empty, that means a bytecode for a given class name will not be saved
	 *
	 * @param className a class name for which bytecode will be saved
	 * @return an optional of a bytecode output stream
	 * @throws IOException if an I/O error occurs
	 */
	protected abstract Optional<OutputStream> getOutputStream(String className) throws IOException;

	@Override
	public final Optional<byte[]> loadBytecode(String className) {
		try {
			Optional<InputStream> maybeInputStream = getInputStream(className);
			if (!maybeInputStream.isPresent()) return Optional.empty();

			try (InputStream stream = maybeInputStream.get()) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
				int size;
				while ((size = stream.read(buffer)) != -1) {
					baos.write(buffer, 0, size);
				}
				return Optional.of(baos.toByteArray());
			}
		} catch (IOException e) {
			onLoadError(className, e);
			return Optional.empty();
		}
	}

	@Override
	public final void saveBytecode(String className, byte[] bytecode) {
		try {
			Optional<OutputStream> maybeOutputStream = getOutputStream(className);
			if (!maybeOutputStream.isPresent()) return;

			try (OutputStream outputStream = maybeOutputStream.get()) {
				outputStream.write(bytecode);
			}
		} catch (IOException e) {
			onSaveError(className, bytecode, e);
		}
	}

	/**
	 * This method will be called when I/O error occurs when loading a bytecode
	 *
	 * @param className a class name for which a bytecode was loaded
	 * @param e         I/O exception
	 */
	protected void onLoadError(String className, IOException e) {
		logger.warn("Could not load bytecode for class: {}", className, e);
	}

	/**
	 * This method will be called when I/O error occurs when saving a bytecode
	 *
	 * @param className a class name for which a bytecode was saved
	 * @param e         I/O exception
	 */
	protected void onSaveError(String className, byte[] bytecode, IOException e) {
		logger.warn("Could not save bytecode for class: {}", className, e);
	}

}
