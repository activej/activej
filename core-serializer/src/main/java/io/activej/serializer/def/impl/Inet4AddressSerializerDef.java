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

package io.activej.serializer.def.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.serializer.*;
import io.activej.serializer.def.SimpleSerializerDef;

import java.net.Inet4Address;
import java.net.UnknownHostException;

@ExposedInternals
public final class Inet4AddressSerializerDef extends SimpleSerializerDef<Inet4Address> {
	@Override
	protected BinarySerializer<Inet4Address> createSerializer(int version, CompatibilityLevel compatibilityLevel) {
		return new BinarySerializer<>() {
			@Override
			public void encode(BinaryOutput out, Inet4Address address) {
				out.write(address.getAddress());
			}

			@Override
			public Inet4Address decode(BinaryInput in) throws CorruptedDataException {
				byte[] addressBytes = new byte[4];
				in.read(addressBytes);

				try {
					return (Inet4Address) Inet4Address.getByAddress(addressBytes);
				} catch (UnknownHostException e) {
					throw new CorruptedDataException(e.getMessage());
				}
			}
		};
	}
}
