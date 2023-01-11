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

package io.activej.aggregation;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.aggregation.util.JsonCodec;
import io.activej.common.initializer.WithInitializer;

import java.io.IOException;

import static com.dslplatform.json.JsonWriter.*;

public class JsonCodec_PrimaryKey implements JsonCodec<PrimaryKey>, WithInitializer<JsonCodec_PrimaryKey> {
	private final JsonCodec<Object>[] codecs;

	private JsonCodec_PrimaryKey(JsonCodec<Object>[] codecs) {
		this.codecs = codecs;
	}

	@SuppressWarnings("unchecked")
	public static JsonCodec_PrimaryKey create(AggregationStructure structure) {
		JsonCodec<Object>[] codecs = new JsonCodec[structure.getKeys().size()];
		for (int i = 0; i < structure.getKeys().size(); i++) {
			String key = structure.getKeys().get(i);
			FieldType<?> keyType = structure.getKeyTypes().get(key);
			codecs[i] = (JsonCodec<Object>) keyType.getInternalCodec();
		}
		return new JsonCodec_PrimaryKey(codecs);
	}

	@Override
	public PrimaryKey read(JsonReader reader) throws IOException {
		if (reader.last() != ARRAY_START) throw reader.newParseError("Expected '['");
		Object[] array = new Object[codecs.length];
		for (int i = 0; i < codecs.length; i++) {
			reader.getNextToken();
			array[i] = codecs[i].read(reader);
			if (i != codecs.length - 1) {
				reader.comma();
			}
		}
		reader.endArray();
		return PrimaryKey.ofArray(array);
	}

	@Override
	public void write(JsonWriter writer, PrimaryKey value) {
		assert value != null;
		Object[] array = value.getArray();
		writer.writeByte(ARRAY_START);
		for (int i = 0; i < codecs.length; i++) {
			codecs[i].write(writer, array[i]);
			if (i != codecs.length - 1) {
				writer.writeByte(COMMA);
			}
		}
		writer.writeByte(ARRAY_END);
	}
}
