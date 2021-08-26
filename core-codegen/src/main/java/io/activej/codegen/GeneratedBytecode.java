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

public class GeneratedBytecode {
	private final String className;
	private final byte[] bytecode;

	public GeneratedBytecode(String name, byte[] bytecode) {
		className = name;
		this.bytecode = bytecode;
	}

	public final String getClassName() {
		return className;
	}

	public final byte[] getBytecode() {
		return bytecode;
	}

	public final Class<?> defineClass(DefiningClassLoader classLoader) {
		try {
			Class<?> aClass = classLoader.defineClass(className, bytecode);
			onDefinedClass(aClass);
			return aClass;
		} catch (Exception e) {
			onError(e);
			throw e;
		}
	}

	protected void onDefinedClass(Class<?> clazz) {
	}

	protected void onError(Exception e) {
	}
}
