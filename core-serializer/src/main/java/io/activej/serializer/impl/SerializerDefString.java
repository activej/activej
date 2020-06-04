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

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.StringFormat;
import io.activej.serializer.util.BinaryOutputUtils;

import java.util.Set;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Utils.of;
import static io.activej.serializer.CompatibilityLevel.LEVEL_3_LE;
import static io.activej.serializer.StringFormat.UTF8;
import static java.util.Collections.emptySet;

public final class SerializerDefString implements SerializerDefWithNullable {
	private final StringFormat format;
	private final boolean nullable;

	private SerializerDefString(StringFormat format, boolean nullable) {
		this.format = format;
		this.nullable = nullable;
	}

	public SerializerDefString() {
		this(UTF8, false);
	}

	public SerializerDefString(StringFormat format) {
		this(format, false);
	}

	@Override
	public SerializerDefString ensureNullable() {
		return new SerializerDefString(format, true);
	}

	public SerializerDefString encoding(StringFormat format) {
		return new SerializerDefString(format, nullable);
	}

	@Override
	public void accept(Visitor visitor) {
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public Class<?> getEncodeType() {
		return String.class;
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return set(pos, of(() -> {
			Expression string = cast(value, String.class);
			switch (format) {
				case ISO_8859_1:
					return nullable ?
							staticCall(BinaryOutputUtils.class, "writeIso88591Nullable", buf, pos, string) :
							staticCall(BinaryOutputUtils.class, "writeIso88591", buf, pos, string);
				case UTF8:
					return nullable ?
							staticCall(BinaryOutputUtils.class, "writeUTF8Nullable", buf, pos, string) :
							staticCall(BinaryOutputUtils.class, "writeUTF8", buf, pos, string);
				case UTF16:
					String LE = compatibilityLevel.compareTo(LEVEL_3_LE) < 0 ? "" : "LE";
					return nullable ?
							staticCall(BinaryOutputUtils.class, "writeUTF16Nullable" + LE, buf, pos, string) :
							staticCall(BinaryOutputUtils.class, "writeUTF16" + LE, buf, pos, string);
				case UTF8_MB3:
					return nullable ?
							staticCall(BinaryOutputUtils.class, "writeUTF8mb3Nullable", buf, pos, string) :
							staticCall(BinaryOutputUtils.class, "writeUTF8mb3", buf, pos, string);
				default:
					throw new AssertionError();
			}
		}));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		switch (format) {
			case ISO_8859_1:
				return nullable ?
						call(in, "readIso88591Nullable") :
						call(in, "readIso88591");
			case UTF8:
				return nullable ?
						call(in, "readUTF8Nullable") :
						call(in, "readUTF8");
			case UTF16:
				String LE = compatibilityLevel.compareTo(LEVEL_3_LE) < 0 ? "" : "LE";
				return nullable ?
						call(in, "readUTF16Nullable" + LE) :
						call(in, "readUTF16" + LE);
			case UTF8_MB3:
				return nullable ?
						call(in, "readUTF8mb3Nullable") :
						call(in, "readUTF8mb3");
			default:
				throw new AssertionError();
		}
	}
}
