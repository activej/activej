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

package io.activej.etl;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.NumberConverter;
import io.activej.common.initializer.WithInitializer;
import io.activej.multilog.LogFile;
import io.activej.multilog.LogPosition;
import io.activej.ot.repository.JsonIndentUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;

import static com.dslplatform.json.JsonWriter.*;
import static com.dslplatform.json.NumberConverter.serialize;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class LogDiffCodec<D> implements ReadObject<LogDiff<D>>, WriteObject<LogDiff<D>>, WithInitializer<LogDiffCodec<D>> {
	public static final String POSITIONS = "positions";
	public static final String LOG = "log";
	public static final String FROM = "from";
	public static final String TO = "to";
	public static final String OPS = "ops";

	public static final LogPositionFormat LOG_POSITION_FORMAT = new LogPositionFormat();

	private final ReadObject<List<D>> diffsDecoder;
	private final WriteObject<D> diffEncoder;

	private LogDiffCodec(ReadObject<List<D>> diffsDecoder, WriteObject<D> diffEncoder) {
		this.diffsDecoder = diffsDecoder;
		this.diffEncoder = diffEncoder;
	}

	public static <D> LogDiffCodec<D> create(ReadObject<D> diffDecoder, WriteObject<D> diffEncoder) {
		return new LogDiffCodec<>(reader -> reader.readCollection(diffDecoder), diffEncoder);
	}

	public static <D, F extends ReadObject<D> & WriteObject<D>> LogDiffCodec<D> create(F format) {
		return create(format, format);
	}

	@Override
	public LogDiff<D> read(@NotNull JsonReader reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		Map<String, LogPositionDiff> positions = readValue(reader, POSITIONS, $ -> {
			if (reader.last() != ARRAY_START) throw reader.newParseError("Expected '['");
			if (reader.getNextToken() == ARRAY_END) {
				return Map.of();
			}
			Map<String, LogPositionDiff> map = new LinkedHashMap<>();
			Iterator<Map.Entry<String, LogPositionDiff>> iterator = reader.iterateOver((ReadObject) r -> {
				if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
				String log = readValue(reader, LOG, JsonReader::readString);
				reader.comma();
				LogPosition from = readValue(reader, FROM, LOG_POSITION_FORMAT);
				reader.comma();
				LogPosition to = readValue(reader, TO, LOG_POSITION_FORMAT);
				reader.endObject();
				return Map.entry(log, new LogPositionDiff(from, to));
			});
			while (iterator.hasNext()) {
				Map.Entry<String, LogPositionDiff> entry = iterator.next();
				map.put(entry.getKey(), entry.getValue());
			}
			return map;
		});

		reader.comma();
		List<D> diffs = readValue(reader, OPS, diffsDecoder);
		reader.endObject();
		return LogDiff.of(positions, diffs);
	}

	@Override
	public void write(@NotNull JsonWriter writer, LogDiff<D> value) {
		if (value == null) {
			writer.writeNull();
			return;
		}
		writer.writeByte(OBJECT_START);
		writer.writeString(POSITIONS);
		writer.writeByte(SEMI);
		Set<Map.Entry<String, LogPositionDiff>> collection = value.getPositions().entrySet();
		writer.serialize(collection, ($, entry) -> {
			assert entry != null;
			writer.writeByte(OBJECT_START);

			writer.writeString(LOG);
			writer.writeByte(SEMI);
			writer.writeString(entry.getKey());
			writer.writeByte(COMMA);

			writer.writeString(FROM);
			writer.writeByte(SEMI);
			LOG_POSITION_FORMAT.write(writer, entry.getValue().from());
			writer.writeByte(COMMA);

			writer.writeString(TO);
			writer.writeByte(SEMI);
			LOG_POSITION_FORMAT.write(writer, entry.getValue().to());

			writer.writeByte(OBJECT_END);
		});

		writer.writeByte(COMMA);
		writer.writeString(OPS);
		writer.writeByte(SEMI);

		writer.serialize(value.getDiffs(), diffEncoder);

		writer.writeByte(OBJECT_END);
	}

	public static final class LogPositionFormat implements ReadObject<LogPosition>, WriteObject<LogPosition> {
		@SuppressWarnings("ConstantConditions")
		private final WriteObject<LogPosition> onelined = JsonIndentUtils.oneline((writer, value) -> {
			writer.writeByte(ARRAY_START);
			writer.writeString(value.getLogFile().getName());
			writer.writeByte(COMMA);
			serialize(value.getLogFile().getRemainder(), writer);
			writer.writeByte(COMMA);
			serialize(value.getPosition(), writer);
			writer.writeByte(ARRAY_END);
		});

		@Override
		public void write(@NotNull JsonWriter writer, LogPosition value) {
			onelined.write(writer, value);
		}

		@Override
		@SuppressWarnings("ConstantConditions")
		public LogPosition read(@NotNull JsonReader reader) throws IOException {
			if (reader.last() != ARRAY_START) throw reader.newParseError("Expected '['");
			reader.getNextToken();
			String name = reader.readString();
			reader.comma();
			int n = (int) reader.next(NumberConverter::deserializeInt);
			reader.comma();
			long position = (long) reader.next(NumberConverter::deserializeLong);
			reader.endArray();

			return LogPosition.create(new LogFile(name, n), position);
		}
	}

	private static <T> T readValue(JsonReader<?> reader, String key, ReadObject<T> readObject) throws IOException {
		reader.getNextToken();
		String readKey = reader.readKey();
		if (!readKey.equals(key)) throw reader.newParseError("Expected key '" + key + '\'');
		T value = readObject.read(reader);
		if (value == null) {
			throw reader.newParseError("Value of '" + key + "' cannot be null");
		}
		return value;
	}

}
