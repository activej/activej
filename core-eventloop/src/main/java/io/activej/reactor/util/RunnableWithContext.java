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

package io.activej.reactor.util;

import io.activej.common.ApplicationSettings;

public record RunnableWithContext(Object context, Runnable runnable) implements Runnable {
	public static final boolean WITH_CONTEXT = ApplicationSettings.getBoolean(RunnableWithContext.class, "withContext", false);

	public static Runnable runnableOf(Object context, Runnable runnable) {
		return WITH_CONTEXT ? new RunnableWithContext(context, runnable) : runnable;
	}

	@Override
	public void run() {
		runnable.run();
	}
}
