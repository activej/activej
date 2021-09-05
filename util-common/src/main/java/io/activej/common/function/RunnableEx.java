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

package io.activej.common.function;

import io.activej.common.exception.UncheckedException;

import static io.activej.common.exception.Utils.propagateRuntimeException;

@FunctionalInterface
public interface RunnableEx {
	void run() throws Exception;

	static RunnableEx of(Runnable uncheckedFn) {
		return () -> {
			try {
				uncheckedFn.run();
			} catch (UncheckedException ex) {
				throw ex.getCause();
			}
		};
	}

	static Runnable uncheckedOf(RunnableEx checkedFn) {
		return () -> {
			try {
				checkedFn.run();
			} catch (Exception ex) {
				propagateRuntimeException(ex);
				throw UncheckedException.of(ex);
			}
		};
	}
}
