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

package io.activej.async.callback;

import io.activej.common.function.RunnableEx;
import io.activej.common.function.SupplierEx;

import static io.activej.common.exception.FatalErrorHandlers.handleRuntimeException;

public interface AsyncComputation<T> {
	void run(Callback<? super T> callback);

	static AsyncComputation<Void> of(RunnableEx runnable) {
		return callback -> {
			try {
				runnable.run();
			} catch (Exception ex) {
				handleRuntimeException(ex, runnable);
				callback.accept(null, ex);
				return;
			}
			callback.accept(null, null);
		};
	}

	static <T> AsyncComputation<T> of(SupplierEx<? extends T> runnable) {
		return callback -> {
			T result;
			try {
				result = runnable.get();
			} catch (Exception ex) {
				handleRuntimeException(ex, runnable);
				callback.accept(null, ex);
				return;
			}
			callback.accept(result, null);
		};
	}

	static <T> AsyncComputation<T> ofDeferred(SupplierEx<? extends AsyncComputation<? extends T>> computationSupplier) {
		return callback -> {
			AsyncComputation<? extends T> computation;
			try {
				computation = computationSupplier.get();
			} catch (Exception ex) {
				handleRuntimeException(ex, computationSupplier);
				callback.accept(null, ex);
				return;
			}
			computation.run(callback);
		};
	}
}
