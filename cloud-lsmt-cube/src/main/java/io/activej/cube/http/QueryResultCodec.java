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

import io.activej.codec.*;
import io.activej.codec.registry.CodecFactory;
import io.activej.common.exception.parse.ParseException;
import io.activej.common.record.Record;
import io.activej.common.record.RecordScheme;
import io.activej.common.reflection.RecursiveType;
import io.activej.cube.QueryResult;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Utils.*;
import static io.activej.cube.ReportType.*;

final class QueryResultCodec implements StructuredCodec<QueryResult> {
	private static final String MEASURES_FIELD = "measures";
	private static final String ATTRIBUTES_FIELD = "attributes";
	private static final String FILTER_ATTRIBUTES_FIELD = "filterAttributes";
	private static final String RECORDS_FIELD = "records";
	private static final String TOTALS_FIELD = "totals";
	private static final String COUNT_FIELD = "count";
	private static final String SORTED_BY_FIELD = "sortedBy";
	private static final String METADATA_FIELD = "metadata";

	private final Map<String, StructuredCodec<?>> attributeCodecs;
	private final Map<String, StructuredCodec<?>> measureCodecs;

	private final Map<String, Class<?>> attributeTypes;
	private final Map<String, Class<?>> measureTypes;

	private static final StructuredCodec<List<String>> STRING_CODEC = StructuredCodecs.STRING_CODEC.ofList();

	public QueryResultCodec(Map<String, StructuredCodec<?>> attributeCodecs, Map<String, StructuredCodec<?>> measureCodecs, Map<String, Class<?>> attributeTypes, Map<String, Class<?>> measureTypes) {
		this.attributeCodecs = attributeCodecs;
		this.measureCodecs = measureCodecs;
		this.attributeTypes = attributeTypes;
		this.measureTypes = measureTypes;
	}

	public static QueryResultCodec create(CodecFactory mapping, Map<String, Type> attributeTypes, Map<String, Type> measureTypes) {
		Map<String, StructuredCodec<?>> attributeCodecs = new LinkedHashMap<>();
		Map<String, StructuredCodec<?>> measureCodecs = new LinkedHashMap<>();
		Map<String, Class<?>> attributeRawTypes = new LinkedHashMap<>();
		Map<String, Class<?>> measureRawTypes = new LinkedHashMap<>();
		for (Map.Entry<String, Type> entry : attributeTypes.entrySet()) {
			RecursiveType token = RecursiveType.of(entry.getValue());
			attributeCodecs.put(entry.getKey(), mapping.get(token.getType()).nullable());
			attributeRawTypes.put(entry.getKey(), token.getRawType());
		}
		for (Map.Entry<String, Type> entry : measureTypes.entrySet()) {
			RecursiveType token = RecursiveType.of(entry.getValue());
			measureCodecs.put(entry.getKey(), mapping.get(token.getType()));
			measureRawTypes.put(entry.getKey(), token.getRawType());
		}
		return new QueryResultCodec(attributeCodecs, measureCodecs, attributeRawTypes, measureRawTypes);
	}

	@Override
	public QueryResult decode(StructuredInput reader) throws ParseException {
		return reader.readObject($1 -> {
			RecordScheme recordScheme = null;
			List<String> attributes = new ArrayList<>();
			List<String> measures = new ArrayList<>();
			List<String> sortedBy = null;
			List<Record> records = null;
			Record totals = null;
			int totalCount = 0;
			Map<String, Object> filterAttributes = null;

			while (reader.hasNext()) {
				String field = reader.readKey();
				switch (field) {
					case METADATA_FIELD:
						reader.readObject(() -> {
							reader.readKey(ATTRIBUTES_FIELD);
							attributes.addAll(STRING_CODEC.decode(reader));

							reader.readKey(MEASURES_FIELD);
							measures.addAll(STRING_CODEC.decode(reader));
						});
						recordScheme = recordScheme(attributes, measures);
						break;
					case SORTED_BY_FIELD:
						sortedBy = STRING_CODEC.decode(reader);
						break;
					case RECORDS_FIELD:
						records = readRecords(reader, recordScheme);
						break;
					case COUNT_FIELD:
						totalCount = reader.readInt();
						break;
					case FILTER_ATTRIBUTES_FIELD:
						filterAttributes = readFilterAttributes(reader);
						break;
					case TOTALS_FIELD:
						totals = readTotals(reader, recordScheme);
						break;
					default:
						throw new ParseException("Unknown field: " + field);
				}
			}

			return QueryResult.create(recordScheme, attributes, measures,
					nullToEmpty(sortedBy),
					nullToEmpty(records),
					nullToDefault(totals, Record.create(recordScheme)),
					totalCount,
					nullToEmpty(filterAttributes),
					totals != null ?
							DATA_WITH_TOTALS :
							records != null ?
									DATA :
									METADATA);
		});
	}

	private List<Record> readRecords(StructuredInput reader, RecordScheme recordScheme) throws ParseException {
		StructuredCodec<?>[] fieldStructuredCodecs = getStructuredCodecs(recordScheme);
		StructuredDecoder<Record> recordDecoder = in -> {
			Record record = Record.create(recordScheme);
			for (int i = 0; i < fieldStructuredCodecs.length; i++) {
				Object fieldValue = fieldStructuredCodecs[i].decode(in);
				record.put(i, fieldValue);
			}
			return record;
		};
		return reader.readTuple($1 -> {
			List<Record> records = new ArrayList<>();
			while (reader.hasNext()) {
				records.add(reader.readTuple(recordDecoder));
			}
			return records;
		});
	}

	private Record readTotals(StructuredInput reader, RecordScheme recordScheme) throws ParseException {
		return reader.readTuple($ -> {
			Record totals = Record.create(recordScheme);
			for (int i = 0; i < recordScheme.getFields().size(); i++) {
				String field = recordScheme.getField(i);
				StructuredCodec<?> fieldStructuredCodec = measureCodecs.get(field);
				if (fieldStructuredCodec == null)
					continue;
				Object fieldValue = fieldStructuredCodec.decode(reader);
				totals.put(i, fieldValue);
			}
			return totals;
		});
	}

	private Map<String, Object> readFilterAttributes(StructuredInput reader) throws ParseException {
		return reader.readObject($ -> {
			Map<String, Object> result = new LinkedHashMap<>();
			while (reader.hasNext()) {
				String attribute = reader.readKey();
				Object value = attributeCodecs.get(attribute).decode(reader);
				result.put(attribute, value);
			}
			return result;
		});
	}

	@Override
	public void encode(StructuredOutput writer, QueryResult result) {
		writer.writeObject(() -> {
			writer.writeKey(METADATA_FIELD);
			writer.writeObject(() -> {
				writer.writeKey(ATTRIBUTES_FIELD);
				STRING_CODEC.encode(writer, result.getAttributes());

				writer.writeKey(MEASURES_FIELD);
				STRING_CODEC.encode(writer, result.getMeasures());
			});

			if (result.getReportType() == DATA || result.getReportType() == DATA_WITH_TOTALS) {
				writer.writeKey(SORTED_BY_FIELD);
				STRING_CODEC.encode(writer, result.getSortedBy());
			}

			if (result.getReportType() == DATA || result.getReportType() == DATA_WITH_TOTALS) {
				writer.writeKey(RECORDS_FIELD);
				writeRecords(writer, result.getRecordScheme(), result.getRecords());

				writer.writeKey(COUNT_FIELD);
				writer.writeInt(result.getTotalCount());

				writer.writeKey(FILTER_ATTRIBUTES_FIELD);
				writeFilterAttributes(writer, result.getFilterAttributes());
			}

			if (result.getReportType() == DATA_WITH_TOTALS) {
				writer.writeKey(TOTALS_FIELD);
				writeTotals(writer, result.getRecordScheme(), result.getTotals());
			}
		});
	}

	@SuppressWarnings("unchecked")
	private void writeRecords(StructuredOutput writer, RecordScheme recordScheme, List<Record> records) {
		StructuredCodec<Object>[] fieldStructuredCodecs = (StructuredCodec<Object>[]) getStructuredCodecs(recordScheme);
		StructuredEncoder<Record> recordEncoder = (out, record) -> {
			for (int i = 0; i < recordScheme.getFields().size(); i++) {
				fieldStructuredCodecs[i].encode(writer, record.get(i));
			}
		};
		writer.writeTuple(() -> {
			for (Record record : records) {
				writer.writeTuple(recordEncoder, record);
			}
		});
	}

	@SuppressWarnings("unchecked")
	private void writeTotals(StructuredOutput writer, RecordScheme recordScheme, Record totals) {
		writer.writeTuple(() -> {
			for (int i = 0; i < recordScheme.getFields().size(); i++) {
				String field = recordScheme.getField(i);
				StructuredCodec<Object> fieldStructuredCodec = (StructuredCodec<Object>) measureCodecs.get(field);
				if (fieldStructuredCodec == null)
					continue;
				fieldStructuredCodec.encode(writer, totals.get(i));
			}
		});
	}

	@SuppressWarnings("unchecked")
	private void writeFilterAttributes(StructuredOutput writer, Map<String, Object> filterAttributes) {
		writer.writeObject(() -> {
			for (Map.Entry<String, Object> entry : filterAttributes.entrySet()) {
				writer.writeKey(entry.getKey());
				StructuredCodec<Object> codec = (StructuredCodec<Object>) attributeCodecs.get(entry.getKey());
				codec.encode(writer, entry.getValue());
			}
		});
	}

	public RecordScheme recordScheme(List<String> attributes, List<String> measures) {
		RecordScheme recordScheme = RecordScheme.create();
		for (String attribute : attributes) {
			recordScheme.withField(attribute, attributeTypes.get(attribute));
		}
		for (String measure : measures) {
			recordScheme.withField(measure, measureTypes.get(measure));
		}
		return recordScheme;
	}

	private StructuredCodec<?>[] getStructuredCodecs(RecordScheme recordScheme) {
		StructuredCodec<?>[] fieldStructuredCodecs = new StructuredCodec<?>[recordScheme.getFields().size()];
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			fieldStructuredCodecs[i] = firstNonNull(attributeCodecs.get(field), measureCodecs.get(field));
		}
		return fieldStructuredCodecs;
	}
}
