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

package io.activej.memcache.protocol;

import io.activej.rpc.protocol.RpcMandatoryData;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.ToIntFunction;

public class MemcacheRpcMessage {
	public static final ToIntFunction<Object> HASH_FUNCTION =
			item -> {
				if (item instanceof GetRequest request) {
					return Arrays.hashCode(request.getKey());
				} else if (item instanceof PutRequest request) {
					return Arrays.hashCode(request.getKey());
				}
				throw new IllegalArgumentException("Unknown request type " + item);
			};

	public static final List<Class<?>> MESSAGE_TYPES = List.of(GetRequest.class, GetResponse.class, PutRequest.class, PutResponse.class);

	public static final class GetRequest implements RpcMandatoryData {
		private final byte[] key;

		public GetRequest(@Deserialize("key") byte[] key) {
			this.key = key;
		}

		@Serialize(order = 1)
		public byte[] getKey() {
			return key;
		}
	}

	public static final class GetResponse {
		private final Slice data;

		public GetResponse(@Deserialize("data") Slice data) {
			this.data = data;
		}

		@Serialize(order = 1)
		@SerializeNullable
		public Slice getData() {
			return data;
		}
	}

	public static final class PutRequest {
		private final byte[] key;
		private final Slice data;

		public PutRequest(@Deserialize("key") byte[] key, @Deserialize("data") Slice data) {
			this.key = key;
			this.data = data;
		}

		@Serialize(order = 1)
		public byte[] getKey() {
			return key;
		}

		@Serialize(order = 2)
		@SerializeNullable
		public Slice getData() {
			return data;
		}
	}

	@SuppressWarnings("InstantiationOfUtilityClass")
	public static final class PutResponse {
		public static final PutResponse INSTANCE = new PutResponse();
	}

	public static final class Slice {
		private final byte[] array;
		private final int offset;
		private final int length;

		public Slice(byte[] array) {
			this.array = array;
			this.offset = 0;
			this.length = array.length;
		}

		public Slice(byte[] array, int offset, int length) {
			this.array = array;
			this.offset = offset;
			this.length = length;
		}

		public byte[] array() {
			return array;
		}

		public int offset() {
			return offset;
		}

		public int length() {
			return length;
		}
	}
}
