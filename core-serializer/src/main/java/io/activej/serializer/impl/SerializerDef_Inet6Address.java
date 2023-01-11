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

package io.activej.serializer.impl;

import io.activej.serializer.*;

import java.net.Inet6Address;
import java.net.UnknownHostException;

public final class SerializerDef_Inet6Address extends SimpleSerializerDef<Inet6Address> {
	@Override
	protected BinarySerializer<Inet6Address> createSerializer(int version, CompatibilityLevel compatibilityLevel) {
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, Inet6Address address) {
				out.write(address.getAddress());
			}

			@Override
			public Inet6Address decode(BinaryInput in) throws CorruptedDataException {
				byte[] addressBytes = new byte[16];
				in.read(addressBytes);

				try {
					return Inet6Address.getByAddress(null, addressBytes, null);
				} catch (UnknownHostException e) {
					throw new CorruptedDataException(e.getMessage());
				}
			}
		};
	}
}
