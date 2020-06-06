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

package io.activej.worker;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.binding.Binding;
import io.activej.inject.module.AbstractModule;
import io.activej.worker.annotation.Worker;
import io.activej.worker.annotation.WorkerId;

import java.lang.annotation.Annotation;

public final class WorkerPoolModule extends AbstractModule {
	private final Class<? extends Annotation>[] workerScopes;

	@SafeVarargs
	private WorkerPoolModule(Class<? extends Annotation>... workerScopes) {
		this.workerScopes = workerScopes;
	}

	public static WorkerPoolModule create() {
		return new WorkerPoolModule(Worker.class);
	}

	@SafeVarargs
	public static WorkerPoolModule create(Class<? extends Annotation>... workerScopes) {
		return new WorkerPoolModule(workerScopes);
	}

	@Override
	protected void configure() {
		bind(WorkerPools.class).to(WorkerPools::new, Injector.class);

		for (Class<? extends Annotation> scope : workerScopes) {
			bind(int.class).qualified(WorkerId.class).in(scope).to(() -> {
				throw new AssertionError("Worker ID constructor must never be called since it's instance is always put in the cache manually");
			});
		}

		generate(WorkerPool.Instances.class, (bindings, scope, key) -> {
			Key<Object> requestedKey = key.getTypeParameter(0);
			return Binding.to(wp -> wp.getInstances(requestedKey), Key.of(WorkerPool.class, key.getQualifier()));
		});
	}
}
