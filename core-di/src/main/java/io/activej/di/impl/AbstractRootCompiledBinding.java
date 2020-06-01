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
public abstract class AbstractRootCompiledBinding<R> implements CompiledBinding<R> {
	private volatile R instance;
	protected final int index;

	protected AbstractRootCompiledBinding(int index) {
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		R localInstance = instance;
		if (localInstance != null) return localInstance;
		synchronized (this) {
			localInstance = instance;
			if (localInstance != null) return localInstance;
			localInstance = (R) scopedInstances[0].get(index);
			if (localInstance != null) return instance = localInstance;
			instance = doCreateInstance(scopedInstances, synchronizedScope);
		}
		scopedInstances[0].lazySet(index, instance);
		return instance;
	}

	protected abstract R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);
}
