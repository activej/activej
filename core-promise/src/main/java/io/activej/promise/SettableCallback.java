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

import io.activej.async.callback.Callback;
import org.jetbrains.annotations.Nullable;

import static io.activej.reactor.Reactor.getCurrentReactor;

public interface SettableCallback<T> extends Callback<T> {
	default void set(T result, @Nullable Exception e) {
		accept(result, e);
	}

	default void set(T result) {
		set(result, null);
	}

	default void setException(Exception e) {
		set(null, e);
	}

	default boolean trySet(T result, @Nullable Exception e) {
		if (isComplete()) return false;
		set(result, e);
		return true;
	}

	default boolean trySet(T result) {
		return trySet(result, null);
	}

	default boolean trySetException(Exception e) {
		return trySet(null, e);
	}

	default void post(T result, @Nullable Exception e) {
		getCurrentReactor().post(() -> set(result, e));
	}

	default void post(T result) {
		getCurrentReactor().post(() -> set(result));
	}

	default void postException(Exception e) {
		getCurrentReactor().post(() -> setException(e));
	}

	default void tryPost(T result, @Nullable Exception e) {
		getCurrentReactor().post(() -> trySet(result, e));
	}

	default void tryPost(T result) {
		getCurrentReactor().post(() -> trySet(result));
	}

	default void tryPostException(Exception e) {
		getCurrentReactor().post(() -> trySetException(e));
	}

	boolean isComplete();
}
