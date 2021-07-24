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

import io.activej.types.TypeT;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;

import static io.activej.types.AnnotatedTypeUtils.annotatedTypeOf;

/**
 * This is an interface for something that can create or retrieve a codec for a given type.
 */
public interface TypeScanner<R> {
	R scan(AnnotatedType type);

	default R scan(Type type) {
		return scan(annotatedTypeOf(type));
	}

	default R scan(TypeT<?> type) {
		return scan(type.getAnnotatedType());
	}
}
