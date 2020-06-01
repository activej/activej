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

package io.activej.service;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public interface SupplierService<V> extends Supplier<V>, Service {

	static <V> SupplierService<V> of(Callable<? extends V> callable) {
		return of(Runnable::run, callable);
	}

	static <V> SupplierService<V> of(Executor executor, Callable<? extends V> callable) {
		return new AbstractSupplierService<V>(executor) {
			@NotNull
			@Override
			protected V compute() throws Exception {
				return callable.call();
			}
		};
	}

}
