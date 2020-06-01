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

@SuppressWarnings("rawtypes")
public abstract class AbstractCompiledBinding<R> implements CompiledBinding<R> {
	protected final int scope;

	protected final int index;

	protected AbstractCompiledBinding(int scope, int index) {
		this.scope = scope;
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		AtomicReferenceArray array = scopedInstances[scope];
		R instance = (R) array.get(index);
		if (instance != null) return instance;
		if (synchronizedScope == scope) {
			instance = doCreateInstance(scopedInstances, synchronizedScope);
			array.set(index, instance);
			return instance;
		}
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		synchronized (array) {
			instance = (R) array.get(index);
			if (instance != null) return instance;
			instance = doCreateInstance(scopedInstances, scope);
			array.set(index, instance);
			return instance;
		}
	}

	protected abstract R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);
}
