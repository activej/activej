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

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.memcache.protocol.MemcacheRpcMessage.Slice;
import io.activej.serializer.AbstractSerializerDef;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.SerializerDefNullable;
import io.activej.serializer.impl.SerializerDefWithNullable;
import io.activej.serializer.util.BinaryOutputUtils;

import static io.activej.codegen.expression.Expressions.*;

@SuppressWarnings("unused")
public class SerializerDefSlice extends AbstractSerializerDef implements SerializerDefWithNullable {
	private final boolean nullable;

	public SerializerDefSlice() {
		this.nullable = false;
	}

	SerializerDefSlice(boolean nullable) {
		this.nullable = nullable;
	}

	@Override
	public Class<?> getEncodeType() {
		return Slice.class;
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return set(pos,
				staticCall(SerializerDefSlice.class,
						"write" + (nullable ? "Nullable" : ""),
						buf, pos, cast(value, Slice.class)));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return staticCall(SerializerDefSlice.class,
				"read" + (nullable ? "Nullable" : ""),
				in);
	}

	public static int write(byte[] output, int offset, Slice slice) {
		offset = BinaryOutputUtils.writeVarInt(output, offset, slice.length());
		offset = BinaryOutputUtils.write(output, offset, slice.array(), slice.offset(), slice.length());
		return offset;
	}

	public static int writeNullable(byte[] output, int offset, Slice slice) {
		if (slice == null) {
			output[offset] = 0;
			return offset + 1;
		} else {
			offset = BinaryOutputUtils.writeVarInt(output, offset, slice.length() + 1);
			offset = BinaryOutputUtils.write(output, offset, slice.array(), slice.offset(), slice.length());
			return offset;
		}
	}

	public static Slice read(BinaryInput in) {
		int length = in.readVarInt();
		Slice result = new Slice(in.array(), in.pos(), length);
		in.pos(in.pos() + length);
		return result;
	}

	public static Slice readNullable(BinaryInput in) {
		int length = in.readVarInt();
		if (length == 0) return null;
		length--;
		Slice result = new Slice(in.array(), in.pos(), length);
		in.pos(in.pos() + length);
		return result;
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.compareTo(CompatibilityLevel.LEVEL_3) < 0) {
			return new SerializerDefNullable(this);
		}
		return new SerializerDefSlice(true);
	}
}
