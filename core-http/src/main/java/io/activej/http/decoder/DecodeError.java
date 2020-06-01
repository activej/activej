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

package io.activej.http.decoder;

public final class DecodeError {
	final String message;
	final Object[] args;

	private DecodeError(String message, Object[] args) {
		this.message = message;
		this.args = args;
	}

	public static DecodeError of(String message, Object... args) {
		return new DecodeError(message, args);
	}

	public String getMessage() {
		return message;
	}

	public Object[] getArgs() {
		return args;
	}

	@Override
	public String toString() {
		return String.format(message, args);
	}
}
