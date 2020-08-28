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

package io.activej.http;

import io.activej.common.annotation.Beta;
import io.activej.common.exception.StacklessException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.activej.common.Checks.checkArgument;
import static io.activej.http.HttpUtils.isReservedCloseCode;

/**
 * A generic web socket exception that corresponds to a close frame that has been sent or received.
 * If a web socket is closed with this exception it will be translated to a close frame with corresponding
 * close code and close reason.
 * <p>
 *
 * Note that some codes are forbidden to be sent on a wire, exceptions with such codes will be translated to a more generic
 * exceptions.
 */
@Beta
public final class WebSocketException extends StacklessException {
	@Nullable
	private final Integer code;

	/**
	 * An empty exception with no close code and no close reason.
	 * Peer that receives close frame with no close code will interpret it as a close code {@code 1005}
	 */
	public WebSocketException(Class<?> component) {
		super(component, "");
		this.code = null;
	}

	/**
	 * An empty exception with a close code but no close reason.
	 */
	public WebSocketException(Class<?> component, @NotNull Integer code) {
		super(component, "");
		this.code = code;
	}

	/**
	 * An exception with a close code and a close reason. Length of the reason string should not exceed 123 characters.
	 */
	public WebSocketException(Class<?> component, @NotNull Integer code, @NotNull String reason) {
		super(component, checkArgument(reason, r -> r.length() <= 123, "Reason too long"));
		this.code = code;
	}

	@Nullable
	public Integer getCode() {
		return code;
	}

	public String getReason() {
		return super.getMessage();
	}

	boolean canBeEchoed() {
		return code == null || !isReservedCloseCode(code);
	}

	@Override
	public String getMessage() {
		return code == null ? "" : ("[" + code + ']' + super.getMessage());
	}
}
