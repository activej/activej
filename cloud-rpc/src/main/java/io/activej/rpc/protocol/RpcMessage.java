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
import io.activej.serializer.annotations.SerializeClass;
import io.activej.serializer.annotations.SerializeNullable;

public final class RpcMessage {
	public static final String SUBCLASSES_ID = "data";

	private final int index;
	private final Object message;

	public RpcMessage(@Deserialize("index") int index, @Deserialize("message") Object message) {
		this.index = index;
		this.message = message;
	}

	public RpcMessage(RpcControlMessage controlMessage) {
		this(0, controlMessage);
	}

	@Serialize(order = 1)
	public int getIndex() {
		return index;
	}

	@Serialize(order = 2)
	@SerializeClass(
		subclassesIdx = -1, subclasses = {RpcControlMessage.class, RpcRemoteException.class},
		subclassesId = SUBCLASSES_ID
	)
	@SerializeNullable
	public Object getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "RpcMessage{" +
			"index=" + index +
			", message=" + message +
			'}';
	}
}
