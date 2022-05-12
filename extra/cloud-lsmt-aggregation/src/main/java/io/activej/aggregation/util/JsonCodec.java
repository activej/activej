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

package io.activej.aggregation.util;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.JsonWriter.WriteObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

public interface JsonCodec<T> extends ReadObject<T>, WriteObject<T> {
	static <T> JsonCodec<T> of(ReadObject<T> readObject, WriteObject<T> writeObject) {
		return new JsonCodec<>() {
			@Override
			public T read(@NotNull JsonReader reader) throws IOException {
				return readObject.read(reader);
			}

			@Override
			public void write(@NotNull JsonWriter writer, T value) {
				writeObject.write(writer, value);
			}
		};
	}

	default JsonCodec<@Nullable T> nullable() {
		return JsonCodec.of(
				reader -> {
					if (reader.wasNull()) return null;
					return JsonCodec.this.read(reader);
				},
				(writer, value) -> {
					if (value == null) writer.writeNull();
					else JsonCodec.this.write(writer, value);
				});
	}
}
