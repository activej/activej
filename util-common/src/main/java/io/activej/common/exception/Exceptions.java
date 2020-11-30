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

package io.activej.common.exception;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public final class Exceptions {

	public static Exception concat(@NotNull Class<?> component, String message, List<? extends Throwable> errors) {
		StacklessException res = new StacklessException(component, message);
		errors.forEach(res::addSuppressed);
		return res;
	}

	public static Exception concat(String message, List<? extends Throwable> errors) {
		return concat(Exceptions.class, message, errors);
	}
}
