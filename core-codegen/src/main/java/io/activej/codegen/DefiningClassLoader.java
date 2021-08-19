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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.activej.codegen.util.Utils.getPathSetting;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

/**
 * Represents a loader for defining dynamically generated classes.
 * Also contains cache, that speeds up loading of classes, which have the same structure as the ones already loaded.
 */
@SuppressWarnings("WeakerAccess")
public final class DefiningClassLoader extends ClassLoader implements DefiningClassLoaderMBean {

	private final Map<String, Class<?>> definedClasses = new ConcurrentHashMap<>();
	private final Map<ClassKey<?>, Class<?>> cachedClasses = new ConcurrentHashMap<>();
	public static final Path DEFAULT_SAVE_DIR = getPathSetting(DefiningClassLoader.class, "saveDir", null);

	// region builders
	private DefiningClassLoader() {
	}

	private DefiningClassLoader(ClassLoader parent) {
		super(parent);
	}

	public static DefiningClassLoader create() {
		return new DefiningClassLoader();
	}

	public static DefiningClassLoader create(ClassLoader parent) {
		return new DefiningClassLoader(parent);
	}
	// endregion

	public DefiningClassLoader withBytecodeSaveDir(Path path) {
		return null;
	}

	public Class<?> defineClass(String className, byte[] bytecode) {
		Class<?> aClass = super.defineClass(className, bytecode, 0, bytecode.length);
		definedClasses.put(className, aClass);
		return aClass;
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <T> Class<T> ensureClass(String className, Supplier<ClassBuilder<T>> classBuilderSupplier) {
		synchronized (getClassLoadingLock(className)) {
			Class<?> aClass = findLoadedClass(className);
			if (aClass != null) return (Class<T>) aClass;
			ClassBuilder<T> builder = classBuilderSupplier.get();
			return builder.build(this, className);
		}
	}

	@NotNull
	public <T> Class<T> ensureClass(ClassKey<? super T> key, Supplier<ClassBuilder<T>> classBuilderSupplier) {
		//noinspection unchecked
		return (Class<T>) cachedClasses.computeIfAbsent(key, $ -> {
			ClassBuilder<T> builder = classBuilderSupplier.get();
			return builder.defineClass(this);
		});
	}

	@NotNull
	public <T> T ensureClassAndCreateInstance(ClassKey<? super T> key, Supplier<ClassBuilder<T>> classBuilderSupplier,
			Object... arguments) {
		try {
			return ensureClass(key, classBuilderSupplier)
					.getConstructor(Arrays.stream(arguments).map(Object::getClass).toArray(Class<?>[]::new))
					.newInstance(arguments);
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

	// JMX

	@Override
	public int getDefinedClassesCount() {
		return definedClasses.size();
	}

	@Override
	public Map<String, Long> getDefinedClassesCountByType() {
		return definedClasses.values().stream()
				.map(aClass -> aClass.getSuperclass() == Object.class && aClass.getInterfaces().length != 0 ?
						aClass.getInterfaces()[0] :
						aClass.getSuperclass())
				.map(Class::getName)
				.collect(groupingBy(identity(), counting()));
	}

	@Nullable
	public Class<?> getCachedClass(@NotNull ClassKey<?> key) {
		return cachedClasses.get(key);
	}

	@Override
	public int getCachedClassesCount() {
		return cachedClasses.size();
	}

	@Override
	public Map<String, Long> getCachedClassesCountByType() {
		return cachedClasses.keySet().stream()
				.map(key -> key.getKeyClass().getName())
				.collect(groupingBy(identity(), counting()));
	}

	@Override
	public String toString() {
		return "{classes=" + cachedClasses.size() + ", byType=" + getCachedClassesCountByType() + '}';
	}
}
