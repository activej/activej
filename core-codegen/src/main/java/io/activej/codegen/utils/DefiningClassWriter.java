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

package io.activej.codegen.utils;

import io.activej.codegen.DefiningClassLoader;
import org.objectweb.asm.ClassWriter;

public final class DefiningClassWriter extends ClassWriter {
	private final DefiningClassLoader classLoader;

	private DefiningClassWriter(DefiningClassLoader classLoader) {
		super(ClassWriter.COMPUTE_FRAMES);
		this.classLoader = classLoader;
	}

	public static DefiningClassWriter create(DefiningClassLoader classLoader) {
		return new DefiningClassWriter(classLoader);
	}

	@Override
	protected String getCommonSuperClass(String type1, String type2) {
		Class<?> c, d;
		try {
			c = Class.forName(type1.replace('/', '.'), false, classLoader);
			d = Class.forName(type2.replace('/', '.'), false, classLoader);
		} catch (Exception e) {
			//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - toString() is used
			throw new RuntimeException(e.toString());
		}
		if (c.isAssignableFrom(d)) {
			return type1;
		}
		if (d.isAssignableFrom(c)) {
			return type2;
		}
		if (c.isInterface() || d.isInterface()) {
			return "java/lang/Object";
		} else {
			do {
				c = c.getSuperclass();
			} while (!c.isAssignableFrom(d));
			return c.getName().replace('.', '/');
		}
	}
}
