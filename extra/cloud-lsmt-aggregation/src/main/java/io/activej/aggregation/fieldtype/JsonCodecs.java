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

package io.activej.aggregation.fieldtype;

import com.dslplatform.json.BoolConverter;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.NumberConverter;
import io.activej.aggregation.util.JsonCodec;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Set;

@SuppressWarnings("ConstantConditions")
class JsonCodecs {
	static final JsonCodec<String> STRING_CODEC = JsonCodec.of(JsonReader::readString, JsonWriter::writeString);
	static final JsonCodec<Short> SHORT_CODEC = JsonCodec.of(NumberConverter::deserializeShort, (writer, value) -> NumberConverter.serialize(value, writer));
	static final JsonCodec<Integer> INTEGER_CODEC = JsonCodec.of(NumberConverter::deserializeInt, (writer, value) -> NumberConverter.serialize(value, writer));
	static final JsonCodec<Long> LONG_CODEC = JsonCodec.of(NumberConverter::deserializeLong, (writer, value) -> NumberConverter.serialize(value, writer));
	static final JsonCodec<Float> FLOAT_CODEC = JsonCodec.of(NumberConverter::deserializeFloat, (writer, value) -> NumberConverter.serialize(value, writer));
	static final JsonCodec<Double> DOUBLE_CODEC = JsonCodec.of(NumberConverter::deserializeDouble, (writer, value) -> NumberConverter.serialize(value, writer));
	static final JsonCodec<Boolean> BOOLEAN_CODEC = JsonCodec.of(BoolConverter::deserialize, (writer, value) -> BoolConverter.serialize(value, writer));

	static final JsonCodec<Byte> BYTE_CODEC = JsonCodec.of(
			reader -> {
				int result = NumberConverter.deserializeInt(reader);
				if (result >= 0 && result <= 255) {
					return (byte) result;
				}
				throw reader.newParseError("Read an int not in range [0, 255] while trying to read a byte");
			},
			(writer, value) -> NumberConverter.serialize(value & 0xFF, writer));

	static final JsonCodec<Character> CHARACTER_CODEC = JsonCodec.of(
			reader -> {
				String string = reader.readString();
				if (string.length() == 1) {
					return string.charAt(0);
				}
				throw reader.newParseError("Read a string with length != 1 while trying to read a character");

			},
			(writer, value) -> writer.writeString(value.toString()));

	static final JsonCodec<LocalDate> LOCAL_DATE_CODEC = JsonCodec.of(
			reader -> {
				try {
					return LocalDate.parse(reader.readString());
				} catch (DateTimeParseException e) {
					throw reader.newParseError(e.getMessage());
				}
			}, (writer, value) -> writer.writeString(value.toString())
	);

	static <T> JsonCodec<Set<T>> ofSet(JsonCodec<T> codec) {
		return new JsonCodec<Set<T>>() {
			@Override
			public Set<T> read(@NotNull JsonReader reader) throws IOException {
				return ((JsonReader<?>) reader).readSet(codec);
			}

			@Override
			public void write(@NotNull JsonWriter writer, Set<T> value) {
				writer.serialize(value, codec);
			}
		};
	}

	static <E extends Enum<E>> JsonCodec<E> ofEnum(Class<E> enumClass) {
		return JsonCodec.of(reader -> Enum.valueOf(enumClass, reader.readString()), (writer, value) -> writer.writeString(value.name()));
	}
}
