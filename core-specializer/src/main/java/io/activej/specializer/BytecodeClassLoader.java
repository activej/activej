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

package io.activej.specializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class BytecodeClassLoader extends ClassLoader {
	final AtomicInteger classN = new AtomicInteger();

	private final Map<String, byte[]> extraClassDefs = new HashMap<>();

	public BytecodeClassLoader() {
	}

	public BytecodeClassLoader(ClassLoader parent) {
		super(parent);
	}

	synchronized void register(String className, byte[] bytecode) {
		extraClassDefs.put(className, bytecode);
	}

	@Override
	protected synchronized Class<?> findClass(final String name) throws ClassNotFoundException {
		byte[] classBytes = this.extraClassDefs.remove(name);
		if (classBytes != null) {
			return defineClass(name, classBytes, 0, classBytes.length);
		}
		return super.findClass(name);
	}

}
