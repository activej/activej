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

import io.activej.types.AnnotatedTypes;
import io.activej.types.AnnotationUtils;
import io.activej.types.TypeT;
import io.activej.types.Types;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.activej.types.AnnotatedTypes.annotatedTypeOf;
import static io.activej.types.IsAssignableUtils.isAssignable;

/**
 * A registry of {@link Mapping}s by type
 * <p>
 * Allows traversing complex annotated types
 *
 * @param <R> a type of mapping result
 */
public final class TypeScannerRegistry<R> {

	/**
	 * An interface that describes how {@link R} value should be retrieved from a {@link Context<R>}
	 *
	 * @param <R> type of value to be retrieved from the context
	 */
	public interface Mapping<R> extends Function<Context<R>, R> {
		@Override
		R apply(Context<R> ctx);
	}

	/**
	 * A context that encapsulates a {@link Mapping<R>} and two stacks
	 * (a context stack and an argument stack).
	 * May hold an arbitrary context value
	 *
	 * @param <R> a type of context data
	 */
	public static final class Context<R> {
		private final Mapping<R> mappingFn;
		private final Context<R>[] stack;
		private final AnnotatedType[] argumentStack;
		private final Object value;

		Context(Mapping<R> mappingFn, Context<R>[] stack, AnnotatedType[] argumentStack, Object value) {
			this.mappingFn = mappingFn;
			this.argumentStack = argumentStack;
			this.stack = stack;
			this.value = value;
		}

		/**
		 * Sets a context value to this context
		 *
		 * @param value a context value
		 */
		public Context<R> withContextValue(Object value) {
			return new Context<>(mappingFn, stack, argumentStack, value);
		}

		/**
		 * Sets a mapping that defines how {@link R} value should be retrieved from this {@link Context<R>}
		 *
		 * @param mappingFn a mapping function
		 */
		public Context<R> withMapping(Mapping<R> mappingFn) {
			return new Context<>(mappingFn, stack, argumentStack, value);
		}

		/**
		 * Adds a mapping that defines how {@link R} value should be retrieved from a {@link Context<R>}
		 * and a previous mapping
		 *
		 * @param mappingFn a mapping function
		 */
		public Context<R> withMapping(BiFunction<Mapping<R>, Context<R>, R> mappingFn) {
			return withMapping(ctx -> mappingFn.apply(this.mappingFn, ctx));
		}

		/**
		 * Returns a type of {@code this} context
		 */
		public Type getType() {
			return getAnnotatedType().getType();
		}

		/**
		 * Returns a raw type of {@code this} context
		 */
		public Class<?> getRawType() {
			return Types.getRawType(getType());
		}

		/**
		 * Returns an annotated type of {@code this} context
		 */
		public AnnotatedType getAnnotatedType() {
			return argumentStack[argumentStack.length - 1];
		}

		/**
		 * Returns an array of annotations of {@code this} context annotated type
		 */
		public Annotation[] getAnnotations() {
			return getAnnotatedType().getAnnotations();
		}

		/**
		 * Checks whether {@code this} context annotations contain an annotation of a given class
		 */
		public boolean hasAnnotation(Class<? extends Annotation> clazz) {
			return AnnotationUtils.hasAnnotation(clazz, this.getAnnotations());
		}

		/**
		 * Returns an annotation of a given annotation type from {@code this} context annotations
		 * or {@code null} if none matches
		 */
		public <A extends Annotation> A getAnnotation(Class<A> clazz) {
			return AnnotationUtils.getAnnotation(clazz, this.getAnnotations());
		}

		/**
		 * Returns an array of annotated type arguments for {@code this} context
		 */
		public AnnotatedType[] getTypeArguments() {
			return AnnotatedTypes.getTypeArguments(getAnnotatedType());
		}

		/**
		 * Returns nth annotated type argument of {@code this} context
		 */
		public AnnotatedType getTypeArgument(int n) {
			return AnnotatedTypes.getTypeArguments(getAnnotatedType())[n];
		}

		/**
		 * Returns a number of annotated type arguments for {@code this} context
		 */
		public int getTypeArgumentsCount() {
			return getTypeArguments().length;
		}

		/**
		 * Checks whether {@code this} context has any type arguments
		 */
		public boolean hasTypeArguments() {
			return getTypeArgumentsCount() != 0;
		}

		/**
		 * Scans nth type argument and returns a resulting {@link R} object
		 *
		 * @param n an index of a type argument to be scanned
		 * @return a scan result which is a {@link R} object
		 */
		public R scanTypeArgument(int n) {
			return scanArgument(getTypeArgument(n));
		}

		/**
		 * Returns a context stack of {@code this} context
		 */
		public Context<R>[] getStack() {
			return stack;
		}

		/**
		 * Returns an argument stack of {@code this} context
		 */
		public AnnotatedType[] getArgumentStack() {
			return argumentStack;
		}

		/**
		 * Scans a given type and returns a resulting {@link R} object
		 *
		 * @param type a type to be scanned
		 * @return a scan result which is a {@link R} object
		 */
		public R scan(Type type) {
			return scan(annotatedTypeOf(type));
		}

		/**
		 * Scans a given annotated type and returns a resulting {@link R} object
		 *
		 * @param annotatedType an annotated type to be scanned
		 * @return a scan result which is a {@link R} object
		 */
		public R scan(AnnotatedType annotatedType) {
			return mappingFn.apply(push(annotatedType));
		}

		private R scanArgument(AnnotatedType annotatedType) {
			return mappingFn.apply(pushArgument(annotatedType));
		}

		/**
		 * Pushes {@code this} context onto a context stack and returns a new context
		 *
		 * @param annotatedType an annotated type to be pushed onto a context stack
		 * @return a context after an annotated type has been pushed onto a stack
		 */
		public Context<R> push(AnnotatedType annotatedType) {
			Context<R>[] stack = Arrays.copyOf(this.stack, this.stack.length + 1);
			stack[stack.length - 1] = this;
			return new Context<>(mappingFn, stack, new AnnotatedType[]{annotatedType}, value);
		}

		private Context<R> pushArgument(AnnotatedType annotatedType) {
			AnnotatedType[] argumentStack = Arrays.copyOf(this.argumentStack, this.argumentStack.length + 1);
			argumentStack[argumentStack.length - 1] = annotatedType;
			return new Context<>(mappingFn, stack, argumentStack, value);
		}

		/**
		 * Returns a context value if it is present, or {@code null} otherwise
		 */
		public Object getContextValue() {
			return value;
		}

		@Override
		public String toString() {
			return getType().toString();
		}
	}

	private @Nullable Mapping<R> mappingFn = null;
	private final List<MappingEntry<R>> entries = new ArrayList<>();

	private record MappingEntry<R>(Type type, Mapping<R> fn) {}

	private TypeScannerRegistry() {
	}

	/**
	 * Creates a new {@link TypeScannerRegistry}
	 *
	 * @param <R> type of values to be retrieved from the registry
	 * @return a new instance of {@link TypeScannerRegistry}
	 */
	public static <R> TypeScannerRegistry<R> create() {
		return new TypeScannerRegistry<>();
	}

	/**
	 * Adds a mapping for a given type
	 *
	 * @param typeT a type token
	 * @param fn    a mapping token to be added for a given type
	 */
	public TypeScannerRegistry<R> with(TypeT<?> typeT, Mapping<R> fn) {
		entries.add(new MappingEntry<>(typeT.getType(), fn));
		return this;
	}

	/**
	 * Adds a mapping for a given type
	 *
	 * @param type a type
	 * @param fn   a mapping to be added for a given type
	 */
	public TypeScannerRegistry<R> with(Type type, Mapping<R> fn) {
		entries.add(new MappingEntry<>(type, fn));
		return this;
	}

	/**
	 * Adds a mapping that defines how {@link R} value should be retrieved from a {@link Context<R>}
	 *
	 * @param mappingFn a mapping function
	 */
	public TypeScannerRegistry<R> withMapping(Mapping<R> mappingFn) {
		this.mappingFn = mappingFn;
		return this;
	}

	/**
	 * Adds a mapping that defines how {@link R} value should be retrieved from a {@link Context<R>}
	 * and a mapping that does {@link #scan(Context)}
	 *
	 * @param mappingFn a mapping function
	 */
	public TypeScannerRegistry<R> withMapping(BiFunction<Mapping<R>, Context<R>, R> mappingFn) {
		this.mappingFn = ctx -> mappingFn.apply(this::scan, ctx);
		return this;
	}

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

	/**
	 * Creates a new type scanner with no context value
	 *
	 * @return a new instance of {@link TypeScanner}
	 */
	public TypeScanner<R> scanner() {
		return scanner(null);
	}

	/**
	 * Creates a new type scanner with a context value
	 *
	 * @param contextValue a context value of this type scanner, possibly {@code null}
	 * @return a new instance of {@link TypeScanner}
	 */
	public TypeScanner<R> scanner(@Nullable Object contextValue) {
		//noinspection unchecked
		return type -> scan(
				new Context<>(
						mappingFn == null ?
								this::scan :
								mappingFn,
						(Context<R>[]) new Context[0],
						new AnnotatedType[]{type},
						contextValue));
	}

	/**
	 * Scans a context and retrieves {@link R} value as a result
	 *
	 * @param ctx a scan context
	 * @return {@link R} value retrieved as a result of scan
	 */
	public R scan(Context<R> ctx) {
		AnnotatedType annotatedType = ctx.getAnnotatedType();
		Mapping<R> fn = match(annotatedType.getType());
		return fn.apply(ctx);
	}
}
