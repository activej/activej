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

package io.activej.csp.process.frame;

import io.activej.common.exception.TruncatedDataException;

/**
 * Exception indicates a stream that either is empty or ends with a complete Data Block (with no trailing data),
 * missing End-Of-Stream Block
 */
public final class MissingEndOfStreamBlockException extends TruncatedDataException {
	public MissingEndOfStreamBlockException(Throwable cause) {
		super("Stream ends with data block, not end-of-stream block", cause);
	}
}
