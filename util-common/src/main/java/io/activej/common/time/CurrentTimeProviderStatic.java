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

/**
 * Singleton which gives access to  current time in program in milliseconds
 */
public final class CurrentTimeProviderStatic implements CurrentTimeProvider {
	/**
	 * Creates a new instance of provider
	 */
	private final static CurrentTimeProviderStatic INSTANCE = new CurrentTimeProviderStatic();

	/**
	 * @return instance of provider
	 */
	public static CurrentTimeProviderStatic instance() {
		return INSTANCE;
	}

	private CurrentTimeProviderStatic() {
	}

	/**
	 * Current time in program
	 */
	private volatile long time;

	/**
	 * @return current time in program
	 */
	@Override
	public long currentTimeMillis() {
		return time;
	}

	/**
	 * Refreshes the time in program, assigns it current system time.
	 */
	public void refresh() {
		time = System.currentTimeMillis();
	}

}
