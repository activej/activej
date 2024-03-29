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

import org.jetbrains.annotations.Async;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a {@link Promise} which can be completed or completedExceptionally
 * manually at once or later in the future.
 * <p>
 * Can be used as root {@code Promise} to start execution of chain of
 * {@code Promises} or when you want wrap your actions in {@code Promise}.
 *
 * @param <T> result type
 */
public final class SettablePromise<T> extends AbstractPromise<T> implements SettableCallback<T> {
	/**
	 * Accepts the provided values and performs this operation
	 * on them. If the {@code Exception e} is {@code null},
	 * provided {@code result} will be set to this
	 * {@code SettablePromise}.
	 * <p>
	 * Otherwise, {@code Exception e} will be set.
	 *
	 * @param result a value to be set to this
	 *               {@code SettablePromise} if
	 *               {@code e} is {@code null}
	 * @param e      a {@code Exception}, which will
	 *               be set to this {@code SettablePromise}
	 *               if not {@code null}
	 */
	@Override
	public void set(T result, @Nullable Exception e) {
		complete(result, e);
	}

	/**
	 * Sets the result of this {@code SettablePromise} and
	 * completes it. {@code AssertionError} is thrown when you
	 * try to set result for an already completed {@code Promise}.
	 */
	@Override
	@Async.Execute
	public void set(T result) {
		complete(result);
	}

	/**
	 * Sets exception and completes this {@code SettablePromise} exceptionally.
	 * {@code AssertionError} is thrown when you try to set exception for
	 * an already completed {@code Promise}.
	 *
	 * @param e exception
	 */
	@Override
	@Async.Execute
	public void setException(Exception e) {
		completeExceptionally(e);
	}

	/**
	 * Tries to set result or exception for this {@code SettablePromise}
	 * if it is not completed yet. Otherwise, does nothing.
	 */
	@Override
	@Async.Execute
	public boolean trySet(T result, @Nullable Exception e) {
		return tryComplete(result, e);
	}

	/**
	 * Tries to set provided {@code result} for this
	 * {@code SettablePromise} if it is not completed yet.
	 * Otherwise, does nothing.
	 */
	@Override
	@Async.Execute
	public boolean trySet(T result) {
		return tryComplete(result);
	}

	/**
	 * Tries to set provided {@code e} exception for this
	 * {@code SettablePromise} if it is not completed yet.
	 * Otherwise, does nothing.
	 */
	@Override
	@Async.Execute
	public boolean trySetException(Exception e) {
		return tryCompleteExceptionally(e);
	}

	@Override
	public String describe() {
		return "SettablePromise";
	}
}
