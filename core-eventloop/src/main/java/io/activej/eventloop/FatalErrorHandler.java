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

package io.activej.eventloop;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link Eventloop}-global callback for any fatal (unchecked) exceptions,
 * that bubbled up to the task call from the main loop.
 *
 * @see Eventloop#withFatalErrorHandler
 */
@FunctionalInterface
public interface FatalErrorHandler {
	/**
	 * Called when an unchecked exception is catched during some task is called from {@link Eventloop}.
	 *
	 * @param e       the catched exception
	 * @param context the context in was catched in, commonly a {@link Runnable} or some callback
	 */
	void handle(@NotNull Throwable e, @Nullable Object context);
}
