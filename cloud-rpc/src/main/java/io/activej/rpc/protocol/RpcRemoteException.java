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

package io.activej.rpc.protocol;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import org.jetbrains.annotations.Nullable;

public final class RpcRemoteException extends RpcException implements RpcMandatoryData {
	private static final long serialVersionUID = 769022174067373741L;
	private final @Nullable String causeMessage;
	private final @Nullable String causeClassName;

	public RpcRemoteException(Throwable cause) {
		super(cause.toString(), cause);
		this.causeClassName = cause.getClass().getName();
		this.causeMessage = cause.getMessage();
	}

	@SuppressWarnings("unused")
	public RpcRemoteException(String message) {
		super(message);
		this.causeClassName = null;
		this.causeMessage = null;
	}

	@SuppressWarnings("unused")
	public RpcRemoteException(@Deserialize("message") String message,
			@Nullable @Deserialize("causeClassName") String causeClassName,
			@Nullable @Deserialize("causeMessage") String causeMessage) {
		super(message);
		this.causeClassName = causeClassName;
		this.causeMessage = causeMessage;
	}

	@Serialize(order = 1)
	@SerializeNullable
	public @Nullable String getCauseClassName() {
		return causeClassName;
	}

	@Serialize(order = 2)
	@SerializeNullable
	public @Nullable String getCauseMessage() {
		return causeMessage;
	}

	@Override
	@Serialize(order = 3)
	@SerializeNullable
	public String getMessage() {
		return super.getMessage();
	}
}
