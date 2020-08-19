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

import org.objectweb.asm.Type;

import java.lang.reflect.Array;
import java.util.function.Function;

import static java.lang.System.identityHashCode;

class Utils {

	static Class<?> getBoxedType(Class<?> type) {
		if (byte.class == type) return Byte.class;
		if (boolean.class == type) return Boolean.class;
		if (short.class == type) return Short.class;
		if (char.class == type) return Character.class;
		if (int.class == type) return Integer.class;
		if (float.class == type) return Float.class;
		if (long.class == type) return Long.class;
		if (double.class == type) return Double.class;
		throw new IllegalArgumentException();
	}

	static Class<?> loadClass(ClassLoader loader, Type stackOwnerType) {
		String name = stackOwnerType.getInternalName().replace('/', '.');
		return doLoadClass(loader, name);
	}

	private static Class<?> doLoadClass(ClassLoader loader, String name) {
		if (name.startsWith("[")) {
			Class<?> aClass = doLoadClass(loader, name.substring(1));
			return Array.newInstance(aClass, 0).getClass();
		}
		if (name.startsWith("L") && name.endsWith(";")) {
			return doLoadClass(loader, name.substring(1, name.length() - 1));
		}
		Class<?> stackOwnerClazz;
		try {
			stackOwnerClazz = loader.loadClass(name);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
		return stackOwnerClazz;
	}

	static final class IdentityKey<T> {
		private final T value;

		IdentityKey(T value) {this.value = value;}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			IdentityKey<?> ref = (IdentityKey<?>) o;
			return value == ref.value;
		}

		@Override
		public int hashCode() {
			return identityHashCode(value);
		}
	}

	static Class<?> normalizeClass(Class<?> clazz) {
		return clazz.isAnonymousClass() ?
				clazz.getSuperclass() != Object.class ?
						clazz.getSuperclass() :
						clazz.getInterfaces()[0] :
				clazz;
	}

	public static String internalizeClassName(String type) {
		return type.startsWith("[") ? type : "L" + type + ";";
	}

	@SuppressWarnings("unused") // A private class that should only be accessed via Reflection API
	private static class InjectorSpecializer implements Function<Object, Object> {
		private final Specializer specializer;

		public InjectorSpecializer() {
			try {
				Class<?> compiledBindingClass = Class.forName("io.activej.inject.impl.CompiledBinding");
				this.specializer = Specializer.create(Thread.currentThread().getContextClassLoader())
						.withPredicate(cls -> compiledBindingClass.isAssignableFrom(cls) &&
								!cls.getName().startsWith("io.activej.inject.binding.Multibinders$"));
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException("Can not access ActiveInject", e);
			}
		}

		@Override
		public Object apply(Object o) {
			return specializer.specialize(o);
		}
	}

}
