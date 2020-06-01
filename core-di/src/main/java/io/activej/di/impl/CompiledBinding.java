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

package io.activej.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * This is defined as an abstract class and not a functional interface so that
 * any anonymous class usage is compiled as a inner class and not a method with invokedynamic instruction.
 * This is needed for compiled bindings to be applicable for later specialization.
 */
@SuppressWarnings("rawtypes")
public interface CompiledBinding<R> {
	@SuppressWarnings("Convert2Lambda")
	CompiledBinding<?> MISSING_OPTIONAL_BINDING = new CompiledBinding<Object>() {
		@Override
		public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			return null;
		}
	};

	R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);

	@SuppressWarnings("unchecked")
	static <R> CompiledBinding<R> missingOptionalBinding() {
		return (CompiledBinding<R>) MISSING_OPTIONAL_BINDING;
	}
}
