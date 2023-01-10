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

package io.activej.cube.http;

import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.NumberConverter;
import com.dslplatform.json.StringConverter;
import io.activej.aggregation.util.JsonCodec;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.initializer.WithInitializer;
import io.activej.cube.QueryResult;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.types.Types;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static com.dslplatform.json.JsonWriter.*;
import static io.activej.common.Utils.nonNullElse;
import static io.activej.common.Utils.nonNullElseEmpty;
import static io.activej.cube.ReportType.*;
import static io.activej.cube.Utils.getJsonCodec;

final class QueryResultCodec implements JsonCodec<QueryResult>, WithInitializer<QueryResultCodec> {
	private static final String MEASURES_FIELD = "measures";
	private static final String ATTRIBUTES_FIELD = "attributes";
	private static final String FILTER_ATTRIBUTES_FIELD = "filterAttributes";
	private static final String RECORDS_FIELD = "records";
	private static final String TOTALS_FIELD = "totals";
	private static final String COUNT_FIELD = "count";
	private static final String SORTED_BY_FIELD = "sortedBy";
	private static final String METADATA_FIELD = "metadata";

	private final Map<String, JsonCodec<Object>> attributeCodecs;
	private final Map<String, JsonCodec<Object>> measureCodecs;

	private final Map<String, Class<?>> attributeTypes;
	private final Map<String, Class<?>> measureTypes;

	private final DefiningClassLoader classLoader;

	public QueryResultCodec(DefiningClassLoader classLoader,
			Map<String, JsonCodec<Object>> attributeCodecs, Map<String, JsonCodec<Object>> measureCodecs, Map<String, Class<?>> attributeTypes, Map<String, Class<?>> measureTypes) {
		this.classLoader = classLoader;
		this.attributeCodecs = attributeCodecs;
		this.measureCodecs = measureCodecs;
		this.attributeTypes = attributeTypes;
		this.measureTypes = measureTypes;
	}

	public static QueryResultCodec create(DefiningClassLoader classLoader,
			Map<String, Type> attributeTypes, Map<String, Type> measureTypes) {
		Map<String, JsonCodec<Object>> attributeCodecs = new LinkedHashMap<>();
		Map<String, JsonCodec<Object>> measureCodecs = new LinkedHashMap<>();
		Map<String, Class<?>> attributeRawTypes = new LinkedHashMap<>();
		Map<String, Class<?>> measureRawTypes = new LinkedHashMap<>();
		for (Map.Entry<String, Type> entry : attributeTypes.entrySet()) {
			attributeCodecs.put(entry.getKey(), getJsonCodec(entry.getValue()).nullable());
			attributeRawTypes.put(entry.getKey(), Types.getRawType(entry.getValue()));
		}
		for (Map.Entry<String, Type> entry : measureTypes.entrySet()) {
			measureCodecs.put(entry.getKey(), getJsonCodec(entry.getValue()));
			measureRawTypes.put(entry.getKey(), Types.getRawType(entry.getValue()));
		}
		return new QueryResultCodec(classLoader, attributeCodecs, measureCodecs, attributeRawTypes, measureRawTypes);
	}

	@Override
	public QueryResult read(JsonReader reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");

		RecordScheme recordScheme = null;
		List<String> attributes = new ArrayList<>();
		List<String> measures = new ArrayList<>();
		List<String> sortedBy = null;
		List<Record> records = null;
		Record totals = null;
		int totalCount = 0;
		Map<String, Object> filterAttributes = null;

		while (true) {
			reader.getNextToken();
			String field = reader.readKey();
			switch (field) {
				case METADATA_FIELD -> {
					if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
					reader.getNextToken();
					String attrFieldsKey = reader.readKey();
					if (!attrFieldsKey.equals(ATTRIBUTES_FIELD)) {
						throw reader.newParseError("Key " + ATTRIBUTES_FIELD + " is expected");
					}
					attributes.addAll(readStrings(reader));
					reader.comma();
					reader.getNextToken();
					String measuresField = reader.readKey();
					if (!measuresField.equals(MEASURES_FIELD)) {
						throw reader.newParseError("Key " + MEASURES_FIELD + " is expected");
					}
					measures.addAll(readStrings(reader));
					reader.endObject();
					recordScheme = recordScheme(attributes, measures);
				}
				case SORTED_BY_FIELD -> sortedBy = readStrings(reader);
				case RECORDS_FIELD -> {
					if (recordScheme == null) {
						throw reader.newParseError('\'' + METADATA_FIELD + "' field should go before '" + RECORDS_FIELD + "' field");
					}
					records = readRecords(reader, recordScheme);
				}
				case COUNT_FIELD -> totalCount = NumberConverter.deserializeInt(reader);
				case FILTER_ATTRIBUTES_FIELD -> filterAttributes = readFilterAttributes(reader);
				case TOTALS_FIELD -> {
					if (recordScheme == null) {
						throw reader.newParseError('\'' + METADATA_FIELD + "' field should go before '" + TOTALS_FIELD + "' field");
					}
					totals = readTotals(reader, recordScheme);
				}
				default -> throw reader.newParseError("Unknown field: " + field);
			}
			byte nextToken = reader.getNextToken();
			if (nextToken == OBJECT_END) {
				break;
			} else if (nextToken != COMMA) {
				throw reader.newParseError("Unknown symbol");
			}
		}

		if (recordScheme == null) {
			throw reader.newParseError("Missing '" + METADATA_FIELD + "' field");
		}

		return QueryResult.create(recordScheme, attributes, measures,
				nonNullElseEmpty(sortedBy),
				nonNullElseEmpty(records),
				nonNullElse(totals, recordScheme.record()),
				totalCount,
				nonNullElseEmpty(filterAttributes),
				totals != null ?
						DATA_WITH_TOTALS :
						records != null ?
								DATA :
								METADATA);

	}

	private List<Record> readRecords(JsonReader<?> reader, RecordScheme recordScheme) throws IOException {
		JsonCodec<?>[] fieldJsonCodecs = getJsonCodecs(recordScheme);
		int size = fieldJsonCodecs.length;
		ReadObject<Record> recordDecoder = in -> {
			if (reader.last() != ARRAY_START) throw reader.newParseError("Expected '{'");
			reader.getNextToken();
			Record record = recordScheme.record();
			for (int i = 0; i < size; i++) {
				Object fieldValue = fieldJsonCodecs[i].read(reader);
				reader.getNextToken();
				record.set(i, fieldValue);
				if (i != size - 1) {
					if (reader.last() != COMMA) throw reader.newParseError("Expected ','");
					reader.getNextToken();
				}
			}
			reader.checkArrayEnd();
			return record;
		};
		return reader.readCollection(recordDecoder);
	}

	private Record readTotals(JsonReader<?> reader, RecordScheme recordScheme) throws IOException {
		if (reader.last() != ARRAY_START) throw reader.newParseError("Expected '['");

		boolean first = true;
		Record totals = recordScheme.record();
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			ReadObject<Object> codec = measureCodecs.get(field);
			if (codec == null)
				continue;
			if (!first) {
				reader.comma();
			}
			first = false;
			reader.getNextToken();
			Object fieldValue = codec.read(reader);
			totals.set(i, fieldValue);
		}
		reader.endArray();
		return totals;
	}

	private Map<String, Object> readFilterAttributes(JsonReader<?> reader) throws IOException {
		if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
		if (reader.getNextToken() == OBJECT_END) return Map.of();
		Map<String, Object> result = new LinkedHashMap<>();
		String key = reader.readKey();
		Object value = attributeCodecs.get(key).read(reader);
		result.put(key, value);
		while (reader.getNextToken() == ',') {
			reader.getNextToken();
			key = reader.readKey();
			value = attributeCodecs.get(key).read(reader);
			result.put(key, value);
		}
		reader.checkObjectEnd();
		return result;
	}

	private static List<String> readStrings(JsonReader<?> reader) throws IOException {
		List<String> strings = reader.readCollection(JsonReader::readString);
		if (strings == null) {
			throw reader.newParseError("List cannot be null");
		}
		return strings;
	}

	@SuppressWarnings("NullableProblems")
	@Override
	public void write(JsonWriter writer, QueryResult result) {
		if (result == null) {
			writer.writeNull();
			return;
		}
		writer.writeByte(OBJECT_START);

		writer.writeString(METADATA_FIELD);
		writer.writeByte(SEMI);

		writer.writeByte(OBJECT_START);
		writer.writeString(ATTRIBUTES_FIELD);
		writer.writeByte(SEMI);
		StringConverter.serialize(result.getAttributes(), writer);
		writer.writeByte(COMMA);

		writer.writeString(MEASURES_FIELD);
		writer.writeByte(SEMI);
		StringConverter.serialize(result.getMeasures(), writer);
		writer.writeByte(OBJECT_END);

		if (result.getReportType() == DATA || result.getReportType() == DATA_WITH_TOTALS) {
			writer.writeByte(COMMA);
			writer.writeString(SORTED_BY_FIELD);
			writer.writeByte(SEMI);
			StringConverter.serialize(result.getSortedBy(), writer);
		}

		if (result.getReportType() == DATA || result.getReportType() == DATA_WITH_TOTALS) {
			writer.writeByte(COMMA);
			writer.writeString(RECORDS_FIELD);
			writer.writeByte(SEMI);
			writeRecords(writer, result.getRecordScheme(), result.getRecords());

			writer.writeByte(COMMA);
			writer.writeString(COUNT_FIELD);
			writer.writeByte(SEMI);
			NumberConverter.serialize(result.getTotalCount(), writer);

			writer.writeByte(COMMA);
			writer.writeString(FILTER_ATTRIBUTES_FIELD);
			writer.writeByte(SEMI);
			writeFilterAttributes(writer, result.getFilterAttributes());
		}

		if (result.getReportType() == DATA_WITH_TOTALS) {
			writer.writeByte(COMMA);
			writer.writeString(TOTALS_FIELD);
			writer.writeByte(SEMI);
			writeTotals(writer, result.getRecordScheme(), result.getTotals());
		}

		writer.writeByte(OBJECT_END);
	}

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	private void writeRecords(JsonWriter writer, RecordScheme recordScheme, List<Record> records) {
		JsonCodec<Object>[] fieldJsonCodecs = (JsonCodec<Object>[]) getJsonCodecs(recordScheme);

		writer.serialize(records, (out, record) -> {
			writer.writeByte(ARRAY_START);
			for (int i = 0; i < fieldJsonCodecs.length; i++) {
				fieldJsonCodecs[i].write(writer, record.get(i));
				if (i != fieldJsonCodecs.length - 1) {
					writer.writeByte(COMMA);
				}
			}
			writer.writeByte(ARRAY_END);
		});
	}

	private void writeTotals(JsonWriter writer, RecordScheme recordScheme, Record totals) {
		writer.writeByte(ARRAY_START);
		int size = recordScheme.getFields().size();
		boolean first = true;
		for (int i = 0; i < size; i++) {
			String field = recordScheme.getField(i);
			JsonCodec<Object> fieldCodec = measureCodecs.get(field);
			if (fieldCodec == null)
				continue;
			if (!first) {
				writer.writeByte(COMMA);
			}
			first = false;
			fieldCodec.write(writer, totals.get(i));
		}
		writer.writeByte(ARRAY_END);
	}

	private void writeFilterAttributes(JsonWriter writer, Map<String, Object> filterAttributes) {
		writer.writeByte(OBJECT_START);
		if (filterAttributes.isEmpty()) {
			writer.writeByte(OBJECT_END);
			return;
		}

		Iterator<Map.Entry<String, Object>> iterator = filterAttributes.entrySet().iterator();
		while (true) {
			Map.Entry<String, Object> entry = iterator.next();
			writer.writeString(entry.getKey());
			writer.writeByte(SEMI);
			attributeCodecs.get(entry.getKey()).write(writer, entry.getValue());
			if (iterator.hasNext()) {
				writer.writeByte(COMMA);
			} else {
				writer.writeByte(OBJECT_END);
				return;
			}
		}
	}

	public RecordScheme recordScheme(List<String> attributes, List<String> measures) {
		RecordScheme recordScheme = RecordScheme.create(classLoader);
		for (String attribute : attributes) {
			recordScheme.withField(attribute, attributeTypes.get(attribute));
		}
		for (String measure : measures) {
			recordScheme.withField(measure, measureTypes.get(measure));
		}
		recordScheme.build();
		return recordScheme;
	}

	private JsonCodec<?>[] getJsonCodecs(RecordScheme recordScheme) {
		JsonCodec<?>[] fieldStructuredCodecs = new JsonCodec<?>[recordScheme.getFields().size()];
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			fieldStructuredCodecs[i] = nonNullElse(attributeCodecs.get(field), measureCodecs.get(field));
		}
		return fieldStructuredCodecs;
	}
}
