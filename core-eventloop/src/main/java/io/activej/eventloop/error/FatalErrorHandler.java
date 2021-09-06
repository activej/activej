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

package io.activej.eventloop.error;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A callback for any fatal (unchecked) exceptions
 */
@FunctionalInterface
public interface FatalErrorHandler {
	/**
	 * Called when an unchecked exception is caught during execution of some task
	 *
	 * @param e       the caught exception
	 * @param context the context in which exception was caught in, possibly {@code null}
	 */
	void handle(@NotNull Throwable e, @Nullable Object context);

	/**
	 * @see #handle(Throwable, Object)
	 */
	default void handle(@NotNull Throwable e) {
		handle(e, null);
	}
}
