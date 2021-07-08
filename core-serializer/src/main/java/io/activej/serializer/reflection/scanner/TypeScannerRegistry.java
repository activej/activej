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

package io.activej.serializer.reflection.scanner;

import io.activej.serializer.reflection.TypeT;
import io.activej.serializer.util.Utils;
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

import static io.activej.serializer.reflection.scanner.TypeUtils.annotatedTypeOf;
import static io.activej.serializer.reflection.scanner.TypeUtils.isAssignable;
import static java.util.Collections.emptyList;

public final class TypeScannerRegistry<R, C> {

	public interface MappingFunction<R, C> extends Function<Context<R, C>, R> {
		@Override
		R apply(Context<R, C> ctx);
	}

	public static final class Context<R, C> {
		private final MappingFunction<R, C> mappingFn;
		private final AnnotatedType[] stack;
		private final List<AnnotatedType[]> history;
		private final C value;

		Context(MappingFunction<R, C> mappingFn, AnnotatedType[] stack, List<AnnotatedType[]> history, C value) {
			this.mappingFn = mappingFn;
			this.stack = stack;
			this.history = history;
			this.value = value;
		}

		public Context<R, C> withValue(C value) {
			return new Context<>(mappingFn, stack, history, value);
		}

		public Context<R, C> withMappingFunction(MappingFunction<R, C> mappingFn) {
			return new Context<>(mappingFn, stack, history, value);
		}

		public Context<R, C> withRemappingFunction(BiFunction<MappingFunction<R, C>, Context<R, C>, R> remappingFn) {
			return withMappingFunction(ctx -> remappingFn.apply(mappingFn, ctx));
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
			return Utils.hasAnnotation(clazz, this.getAnnotations());
		}

		public <A extends Annotation> A getAnnotation(Class<A> clazz) {
			return Utils.getAnnotation(clazz, this.getAnnotations());
		}

		public <A extends Annotation, T> T getAnnotation(Class<A> clazz, Function<@Nullable A, T> fn) {
			return Utils.getAnnotation(clazz, this.getAnnotations(), fn);
		}

		public AnnotatedType[] getTypeArguments() {
			return TypeUtils.getTypeArguments(getAnnotatedType());
		}

		public AnnotatedType getTypeArgument(int n) {
			return TypeUtils.getTypeArguments(getAnnotatedType())[n];
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

		public Context<R, C> push(AnnotatedType annotatedType) {
			List<AnnotatedType[]> newHistory = new ArrayList<>(history.size() + 1);
			newHistory.addAll(history);
			newHistory.add(stack);
			return new Context<>(mappingFn, new AnnotatedType[]{annotatedType}, newHistory, value);
		}

		public Context<R, C> pushArgument(AnnotatedType annotatedType) {
			AnnotatedType[] newStack = Arrays.copyOf(stack, stack.length + 1);
			newStack[newStack.length - 1] = annotatedType;
			return new Context<>(mappingFn, newStack, history, value);
		}

		public C value() {
			return value;
		}
	}

	private BiFunction<MappingFunction<R, C>, Context<R, C>, R> remappingFn = MappingFunction::apply;
	private final List<MappingEntry<R, C>> entries = new ArrayList<>();

	private static final class MappingEntry<R, C> {
		final Type type;
		final MappingFunction<R, C> fn;

		private MappingEntry(Type type, MappingFunction<R, C> fn) {
			this.type = type;
			this.fn = fn;
		}
	}

	private TypeScannerRegistry() {
	}

	public static <R, C> TypeScannerRegistry<R, C> create() {
		return new TypeScannerRegistry<>();
	}

	public TypeScannerRegistry<R, C> with(TypeT<?> typeT, MappingFunction<R, C> fn) {
		entries.add(new MappingEntry<>(typeT.getType(), fn));
		return this;
	}

	public TypeScannerRegistry<R, C> with(Type type, MappingFunction<R, C> fn) {
		entries.add(new MappingEntry<>(type, fn));
		return this;
	}

	public TypeScannerRegistry<R, C> withRemappingFunction(BiFunction<MappingFunction<R, C>, Context<R, C>, R> remappingFn) {
		this.remappingFn = remappingFn;
		return this;
	}

	@NotNull
	private TypeScannerRegistry.MappingFunction<R, C> match(Type type) {
		MappingEntry<R, C> best = null;
		for (MappingEntry<R, C> found : entries) {
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
		return type -> scan(new Context<>(remappingFn == null ? this::scan : ctx -> remappingFn.apply(this::scan, ctx),
				new AnnotatedType[]{type}, emptyList(), contextValue));
	}

	public R scan(Context<R, C> ctx) {
		AnnotatedType annotatedType = ctx.getAnnotatedType();
		MappingFunction<R, C> fn = match(annotatedType.getType());
		return fn.apply(ctx);
	}
}
