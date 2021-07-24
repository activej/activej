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

package io.activej.types.scanner;

import io.activej.types.AnnotatedTypeUtils;
import io.activej.types.AnnotationUtils;
import io.activej.types.TypeT;
import io.activej.types.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.activej.types.AnnotatedTypeUtils.annotatedTypeOf;
import static io.activej.types.IsAssignableUtils.isAssignable;
import static java.util.Collections.emptyList;

public final class TypeScannerRegistry<R> {

	public interface Mapping<R> extends Function<Context<R>, R> {
		@Override
		R apply(Context<R> ctx);
	}

	public static final class Context<R> {
		private final Mapping<R> mappingFn;
		private final AnnotatedType[] stack;
		private final List<AnnotatedType[]> history;
		private final Object value;

		Context(Mapping<R> mappingFn, AnnotatedType[] stack, List<AnnotatedType[]> history, Object value) {
			this.mappingFn = mappingFn;
			this.stack = stack;
			this.history = history;
			this.value = value;
		}

		public Context<R> withValue(Object value) {
			return new Context<>(mappingFn, stack, history, value);
		}

		public Context<R> withMapping(Mapping<R> mappingFn) {
			return new Context<>(mappingFn, stack, history, value);
		}

		public Context<R> withMapping(BiFunction<Mapping<R>, Context<R>, R> mappingFn) {
			return withMapping(ctx -> mappingFn.apply(this.mappingFn, ctx));
		}

		public Type getType() {
			return getAnnotatedType().getType();
		}

		public Class<?> getRawClass() {
			return TypeUtils.getRawClass(getType());
		}

		public AnnotatedType getAnnotatedType() {
			return stack[stack.length - 1];
		}

		public Annotation[] getAnnotations() {
			return getAnnotatedType().getAnnotations();
		}

		public boolean hasAnnotation(Class<? extends Annotation> clazz) {
			return AnnotationUtils.hasAnnotation(clazz, this.getAnnotations());
		}

		public <A extends Annotation> A getAnnotation(Class<A> clazz) {
			return AnnotationUtils.getAnnotation(clazz, this.getAnnotations());
		}

		public <A extends Annotation, T> T getAnnotation(Class<A> clazz, Function<@Nullable A, T> fn) {
			return fn.apply(AnnotationUtils.getAnnotation(clazz, this.getAnnotations()));
		}

		public AnnotatedType[] getTypeArguments() {
			return AnnotatedTypeUtils.getTypeArguments(getAnnotatedType());
		}

		public AnnotatedType getTypeArgument(int n) {
			return AnnotatedTypeUtils.getTypeArguments(getAnnotatedType())[n];
		}

		public int getTypeArgumentsCount() {
			return getTypeArguments().length;
		}

		public boolean hasTypeArguments() {
			return getTypeArgumentsCount() != 0;
		}

		public R scanTypeArgument(int n) {
			return scanArgument(getTypeArgument(n));
		}

		public AnnotatedType[] getStack() {
			return stack;
		}

		public List<AnnotatedType[]> getHistory() {
			return history;
		}

		public R scan(Type type) {
			return scan(annotatedTypeOf(type));
		}

		public R scan(AnnotatedType annotatedType) {
			return mappingFn.apply(push(annotatedType));
		}

		private R scanArgument(AnnotatedType annotatedType) {
			return mappingFn.apply(pushArgument(annotatedType));
		}

		public Context<R> push(AnnotatedType annotatedType) {
			List<AnnotatedType[]> newHistory = new ArrayList<>(history.size() + 1);
			newHistory.addAll(history);
			newHistory.add(stack);
			return new Context<>(mappingFn, new AnnotatedType[]{annotatedType}, newHistory, value);
		}

		public Context<R> pushArgument(AnnotatedType annotatedType) {
			AnnotatedType[] newStack = Arrays.copyOf(stack, stack.length + 1);
			newStack[newStack.length - 1] = annotatedType;
			return new Context<>(mappingFn, newStack, history, value);
		}

		public Object value() {
			return value;
		}

		@Override
		public String toString() {
			return getType().toString();
		}
	}

	@Nullable
	private Mapping<R> mappingFn = null;
	private final List<MappingEntry<R>> entries = new ArrayList<>();

	private static final class MappingEntry<R> {
		final Type type;
		final Mapping<R> fn;

		private MappingEntry(Type type, Mapping<R> fn) {
			this.type = type;
			this.fn = fn;
		}
	}

	private TypeScannerRegistry() {
	}

	public static <R> TypeScannerRegistry<R> create() {
		return new TypeScannerRegistry<>();
	}

	public TypeScannerRegistry<R> with(TypeT<?> typeT, Mapping<R> fn) {
		entries.add(new MappingEntry<>(typeT.getType(), fn));
		return this;
	}

	public TypeScannerRegistry<R> with(Type type, Mapping<R> fn) {
		entries.add(new MappingEntry<>(type, fn));
		return this;
	}

	public TypeScannerRegistry<R> withMapping(Mapping<R> mappingFn) {
		this.mappingFn = mappingFn;
		return this;
	}

	public TypeScannerRegistry<R> withMapping(BiFunction<Mapping<R>, Context<R>, R> mappingFn) {
		this.mappingFn = ctx -> mappingFn.apply(this::scan, ctx);
		return this;
	}

	@NotNull
	private Mapping<R> match(Type type) {
		MappingEntry<R> best = null;
		for (MappingEntry<R> found : entries) {
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
		return type -> scan(
				new Context<>(
						mappingFn == null ?
								this::scan :
								mappingFn,
						new AnnotatedType[]{type},
						emptyList(),
						contextValue));
	}

	public R scan(Context<R> ctx) {
		AnnotatedType annotatedType = ctx.getAnnotatedType();
		Mapping<R> fn = match(annotatedType.getType());
		return fn.apply(ctx);
	}
}
