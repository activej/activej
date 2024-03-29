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

package io.activej.redis;

/**
 * Local marker exception for requesting more data.
 * <p>
 * As this exception is stackless and is used purely for signaling that parser needs more data,
 * you can throw constant {@link #NEED_MORE_DATA} rather than creating new instance of this class.
 */
public final class NeedMoreDataException extends RuntimeException {
	public static final NeedMoreDataException NEED_MORE_DATA = new NeedMoreDataException();

	private NeedMoreDataException() {
		super();
	}

	@Override
	public Exception fillInStackTrace() {
		return this;
	}
}
