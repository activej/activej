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

package io.activej.inject.binding;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import org.jetbrains.annotations.Nullable;

import static io.activej.inject.util.Utils.getScopeDisplayString;

/**
 * A runtime exception that is thrown on startup when some static conditions fail
 * (missing or cyclic dependencies, incorrect annotations etc.) or in runtime when
 * you ask an {@link Injector} for an instance it does not have a {@link Binding binding} for.
 */
public final class DIException extends RuntimeException {
	public static DIException cannotConstruct(Key<?> key, @Nullable Binding<?> binding) {
		return new DIException(
			(binding != null ?
				"Binding refused to" :
				"No binding to") +
			" construct an instance for key " + key.getDisplayString() +
			(binding != null && binding.getLocation() != null ?
				("\n\t at" + binding.getLocation()) :
				""));
	}

	public static DIException noCachedBinding(Key<?> key, Scope[] scope) {
		throw new DIException(
			"No cached binding was bound for key " + key.getDisplayString() +
			" in scope " + getScopeDisplayString(scope) +
			". Either bind it or check if a binding for such key exists with hasBinding() call.");
	}

	public DIException(String message) {
		super(message);
	}

	public DIException(String message, Throwable cause) {
		super(message, cause);
	}
}
