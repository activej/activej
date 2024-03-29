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

/**
 * A class that encapsulates a class name and a generated bytecode of this class
 */
public class GeneratedBytecode implements AutoCloseable {
	private final String className;
	private final byte[] bytecode;

	public GeneratedBytecode(String className, byte[] bytecode) {
		this.className = className;
		this.bytecode = bytecode;
	}

	public final String getClassName() {
		return className;
	}

	public final byte[] getBytecode() {
		return bytecode;
	}

	public final Class<?> generateClass(DefiningClassLoader classLoader) {
		Class<?> generatedClass = classLoader.defineClass(className, bytecode);
		touchGeneratedClass(generatedClass);
		return generatedClass;
	}

	protected void touchGeneratedClass(Class<?> generatedClass) {
	}

	@Override
	public void close() {
	}
}
