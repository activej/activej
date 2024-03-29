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

package io.activej.promise;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Helps to create sequent chains of {@code Promise}s.
 */
@ApiStatus.Internal
public abstract class NextPromise<T, R> extends AbstractPromise<R> implements SettableCallback<R> {
	public abstract void acceptNext(T result, @Nullable Exception e);

	@Override
	public void set(R result, @Nullable Exception e) {
		complete(result, e);
	}

	@Override
	public void set(R result) {
		complete(result);
	}

	@Override
	public void setException(Exception e) {
		completeExceptionally(e);
	}

	@Override
	public boolean trySet(R result, @Nullable Exception e) {
		return tryComplete(result, e);
	}

	@Override
	public boolean trySet(R result) {
		return tryComplete(result);
	}

	@Override
	public boolean trySetException(Exception e) {
		return tryCompleteExceptionally(e);
	}
}
