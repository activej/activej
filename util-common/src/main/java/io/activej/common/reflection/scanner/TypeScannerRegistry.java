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

package io.activej.common.reflection.scanner;

import io.activej.common.reflection.TypeT;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.activej.common.reflection.scanner.TypeUtils.isAssignable;
import static java.util.Collections.emptyList;

public final class TypeScannerRegistry<R> {

	public static final class Context<R> {
		private final Class<R> resultType;
		private final TypeScannerRegistry<R> self;
		private final AnnotatedType[] annotatedTypes;
		private final List<AnnotatedType[]> history;
		private final Object value;

		private Context(Class<R> resultType, TypeScannerRegistry<R> self, AnnotatedType[] annotatedTypes, List<AnnotatedType[]> history, Object value) {
			this.resultType = resultType;
			this.self = self;
			this.annotatedTypes = annotatedTypes;
			this.history = history;
			this.value = value;
		}

		public Context<R> withValue(Object value) {
			return new Context<>(resultType, self, annotatedTypes, history, value);
		}

		public AnnotatedType[] getTypes() {
			return annotatedTypes;
		}

		public AnnotatedType getType() {
			return annotatedTypes[annotatedTypes.length - 1];
		}

		public AnnotatedType[] getTypeArguments() {
			return TypeUtils.getTypeArguments(getType());
		}

		public AnnotatedType getTypeArgument(int n) {
			return TypeUtils.getTypeArguments(getType())[n];
		}

		public int getTypeArgumentsCount() {
			return getTypeArguments().length;
		}

		public boolean hasTypeArguments() {
			return getTypeArgumentsCount() != 0;
		}

		public List<AnnotatedType[]> getHistory() {
			return history;
		}

		public R[] scanTypeArguments() {
			//noinspection unchecked
			return Arrays.stream(getTypeArguments()).map(this::scanArgument).toArray(n -> (R[]) Array.newInstance(this.resultType, n));
		}

		public R scanTypeArgument(int n) {
			return scanArgument(getTypeArgument(n));
		}

		public R scan(AnnotatedType newAnnotatedType) {
			List<AnnotatedType[]> newStack = new ArrayList<>(history.size() + 1);
			newStack.addAll(history);
			newStack.add(annotatedTypes);
			return self.scan(newStack, new AnnotatedType[]{newAnnotatedType}, value);
		}

		private R scanArgument(AnnotatedType newAnnotatedType) {
			AnnotatedType[] newAnnotatedTypes = Arrays.copyOf(annotatedTypes, annotatedTypes.length + 1);
			newAnnotatedTypes[newAnnotatedTypes.length - 1] = newAnnotatedType;
			return self.scan(history, newAnnotatedTypes, value);
		}

		public <T> T getValue() {
			//noinspection unchecked
			return (T) value;
		}
	}

	public interface MappingFn<R> {
		R apply(Context<R> ctx);
	}

	private final Class<R> resultType;
	private final List<MappingEntry<R>> mappingFn = new ArrayList<>();

	private static final class MappingEntry<R> {
		final Type type;
		final MappingFn<R> fn;

		private MappingEntry(Type type, MappingFn<R> fn) {
			this.type = type;
			this.fn = fn;
		}
	}

	public TypeScannerRegistry(Class<R> resultType) {
		this.resultType = resultType;
	}

	public static <R> TypeScannerRegistry<R> create(Class<R> resultType) {
		return new TypeScannerRegistry<>(resultType);
	}

	public <T> TypeScannerRegistry<R> with(TypeT<?> typeT, MappingFn<R> fn) {
		mappingFn.add(new MappingEntry<>(typeT.getType(), fn));
		return this;
	}

	public <T> TypeScannerRegistry<R> with(Type type, MappingFn<R> fn) {
		mappingFn.add(new MappingEntry<>(type, fn));
		return this;
	}

	@NotNull
	private MappingFn<R> match(Type type) {
		MappingEntry<R> best = null;
		for (MappingEntry<R> found : mappingFn) {
			if (isAssignable(found.type, type)) {
				if (best == null || isAssignable(best.type, found.type)) {
					if (best != null && !best.type.equals(found.type) && isAssignable(found.type, best.type)) {
						throw new IllegalArgumentException("Conflicting types: " + type + " " + best.type);
					}
					best = found;
				}
			}
		}
		if (best == null) {
			throw new IllegalArgumentException("Not found: " + type);
		}
		return best.fn;
	}

	public TypeScanner<R> scanner() {
		return scanner(null);
	}

	public TypeScanner<R> scanner(Object contextValue) {
		return type -> scan(emptyList(), new AnnotatedType[]{type}, contextValue);
	}

	private R scan(List<AnnotatedType[]> history, AnnotatedType[] annotatedTypes, Object value) {
		AnnotatedType annotatedType = annotatedTypes[annotatedTypes.length - 1];
		MappingFn<R> fn = match(annotatedType.getType());
		return fn.apply(new Context<>(resultType, this, annotatedTypes, history, value));
	}

}
