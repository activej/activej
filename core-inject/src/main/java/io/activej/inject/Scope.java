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

package io.activej.inject;

import io.activej.inject.annotation.ScopeAnnotation;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

import static io.activej.common.Checks.checkArgument;
import static io.activej.inject.util.Utils.isMarker;

public final class Scope {
	public static final Scope[] UNSCOPED = new Scope[0];

	private final Class<? extends Annotation> annotationType;
	private final boolean threadsafe;

	private Scope(Class<? extends Annotation> annotationType, boolean threadsafe) {
		this.annotationType = annotationType;
		this.threadsafe = threadsafe;
	}

	/**
	 * Creates a Scope from a marker (or stateless) annotation, only identified by its class.
	 */
	public static Scope of(Class<? extends Annotation> annotationType) {
		checkArgument(isMarker(annotationType), "Scope by annotation type only accepts marker annotations with no arguments");
		ScopeAnnotation scopeAnnotation = annotationType.getAnnotation(ScopeAnnotation.class);
		checkArgument(scopeAnnotation != null, "Only annotations annotated with @ScopeAnnotation meta-annotation are allowed");
		return new Scope(annotationType, scopeAnnotation.threadsafe());
	}

	/**
	 * Creates a Scope from a real (or its custom surrogate impl) annotation instance.
	 */
	public static Scope of(Annotation annotation) {
		return of(annotation.annotationType());
	}

	public Class<? extends Annotation> getAnnotationType() {
		return annotationType;
	}

	public boolean isThreadsafe() {
		return threadsafe;
	}

	public String getDisplayString() {
		return annotationType.getSimpleName();
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Scope other = (Scope) o;
		return annotationType == other.annotationType;
	}

	@Override
	public int hashCode() {
		return 31 * annotationType.hashCode();
	}

	@Override
	public String toString() {
		return "@" + annotationType.getName() + "()";
	}
}
