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

import static io.activej.common.exception.FatalErrorHandler.handleError;

public interface AsyncComputation<T> {
	void call(Callback<? super T> cb);

	static AsyncComputation<Void> of(RunnableEx runnable) {
		return cb -> {
			try {
				runnable.run();
			} catch (Exception ex) {
				handleError(ex, runnable);
				cb.accept(null, ex);
				return;
			}
			cb.accept(null, null);
		};
	}

	static <T> AsyncComputation<T> of(SupplierEx<? extends T> runnable) {
		return cb -> {
			T result;
			try {
				result = runnable.get();
			} catch (Exception ex) {
				handleError(ex, runnable);
				cb.accept(null, ex);
				return;
			}
			cb.accept(result, null);
		};
	}

	static <T> AsyncComputation<T> ofDeferred(SupplierEx<? extends AsyncComputation<? extends T>> computationSupplier) {
		return cb -> {
			AsyncComputation<? extends T> computation;
			try {
				computation = computationSupplier.get();
			} catch (Exception ex) {
				handleError(ex, computationSupplier);
				cb.accept(null, ex);
				return;
			}
			computation.call(cb);
		};
	}
}
