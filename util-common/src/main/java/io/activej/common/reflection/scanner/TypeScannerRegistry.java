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
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.activej.common.reflection.scanner.TypeUtils.isAssignable;
import static java.util.Collections.emptyList;

public final class TypeScannerRegistry<R, C> {

	public static final class Context<R, C> {
		private final TypeScannerRegistry<R, C> self;
		private final AnnotatedType[] annotatedTypes;
		private final List<AnnotatedType[]> history;
		private final C value;

		private Context(TypeScannerRegistry<R, C> self, AnnotatedType[] annotatedTypes, List<AnnotatedType[]> history, C value) {
			this.self = self;
			this.annotatedTypes = annotatedTypes;
			this.history = history;
			this.value = value;
		}

		public Context<R, C> withValue(C value) {
			return new Context<>(self, annotatedTypes, history, value);
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

	public interface MappingFn<R, C> {
		R apply(Context<R, C> ctx);
	}

	private final List<MappingEntry<R, C>> mappingFn = new ArrayList<>();

	private static final class MappingEntry<R, C> {
		final Type type;
		final MappingFn<R, C> fn;

		private MappingEntry(Type type, MappingFn<R, C> fn) {
			this.type = type;
			this.fn = fn;
		}
	}

	private TypeScannerRegistry() {
	}

	public static <R, C> TypeScannerRegistry<R, C> create() {
		return new TypeScannerRegistry<>();
	}

	public TypeScannerRegistry<R, C> with(TypeT<?> typeT, MappingFn<R, C> fn) {
		mappingFn.add(new MappingEntry<>(typeT.getType(), fn));
		return this;
	}

	public TypeScannerRegistry<R, C> with(Type type, MappingFn<R, C> fn) {
		mappingFn.add(new MappingEntry<>(type, fn));
		return this;
	}

	@NotNull
	private MappingFn<R, C> match(Type type) {
		MappingEntry<R, C> best = null;
		for (MappingEntry<R, C> found : mappingFn) {
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

	public TypeScanner<R> scanner(C contextValue) {
		return type -> scan(emptyList(), new AnnotatedType[]{type}, contextValue);
	}

	private R scan(List<AnnotatedType[]> history, AnnotatedType[] annotatedTypes, C value) {
		AnnotatedType annotatedType = annotatedTypes[annotatedTypes.length - 1];
		MappingFn<R, C> fn = match(annotatedType.getType());
		return fn.apply(new Context<>(this, annotatedTypes, history, value));
	}

}
