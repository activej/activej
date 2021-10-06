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

package io.activej.common.time;

import java.time.Instant;

/**
 * Gives access to current time in milliseconds
 */
public interface CurrentTimeProvider {
	/**
	 * Returns current time in milliseconds
	 */
	long currentTimeMillis();

	/**
	 * Returns current time as an {@link Instant}
	 */
	default Instant currentInstant() {
		return Instant.ofEpochMilli(currentTimeMillis());
	}

	/**
	 * Returns a provider of current time that uses {@link System#currentTimeMillis()}
	 */
	static CurrentTimeProvider ofSystem() {
		return System::currentTimeMillis;
	}

}
