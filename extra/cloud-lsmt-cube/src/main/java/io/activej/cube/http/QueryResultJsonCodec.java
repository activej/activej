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

import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.QueryResult;
import io.activej.cube.ReportType;
import io.activej.json.*;
import io.activej.json.ObjectJsonCodec.JsonCodecProvider;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.record.RecordSetter;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.activej.common.Utils.entriesToHashMap;
import static io.activej.common.Utils.nonNullElse;
import static io.activej.cube.ReportType.*;
import static io.activej.json.JsonCodecs.*;
import static io.activej.json.JsonValidationUtils.validateArgument;
import static io.activej.json.JsonValidationUtils.validateNotNull;

public final class QueryResultJsonCodec {

	static final String ATTRIBUTES_FIELD = "attributes";
	static final String MEASURES_FIELD = "measures";
	static final String FILTER_ATTRIBUTES_FIELD = "filterAttributes";
	static final String RECORDS_FIELD = "records";
	static final String TOTALS_FIELD = "totals";
	static final String COUNT_FIELD = "count";
	static final String SORTED_BY_FIELD = "sortedBy";
	static final String METADATA_FIELD = "metadata";

	record Metadata(List<String> attributes, List<String> measures) {
		static Metadata create(List<String> attributes, List<String> measures) throws JsonValidationException {
			return new Metadata(validateNotNull(attributes), validateNotNull(measures));
		}
	}

	@SuppressWarnings({"unchecked", "NullableProblems"})
	static final class QueryResultBuilder {
		final DefiningClassLoader classLoader;
		final Map<String, Type> attributeTypes;
		final Map<String, Type> measureTypes;

		@Nullable RecordScheme recordScheme;
		@Nullable List<String> attributes;
		@Nullable List<String> measures;
		@Nullable List<String> sortedBy;

		@Nullable List<Record> records;
		@Nullable Record totals;
		@Nullable Integer totalCount;

		@Nullable Map<String, Object> filterAttributes;

		QueryResultBuilder(DefiningClassLoader classLoader,
			Map<String, Type> attributeTypes, Map<String, Type> measureTypes
		) {
			this.attributeTypes = attributeTypes;
			this.measureTypes = measureTypes;
			this.classLoader = classLoader;
		}

		public RecordScheme ensureRecordScheme() throws JsonValidationException {
			if (recordScheme == null) {
				if (attributes == null || measures == null) throw new JsonValidationException();
				RecordScheme.Builder recordSchemeBuilder = RecordScheme.builder(classLoader);
				for (String attribute : attributes) {
					recordSchemeBuilder.withField(attribute, attributeTypes.get(attribute));
				}
				for (String measure : measures) {
					recordSchemeBuilder.withField(measure, measureTypes.get(measure));
				}
				recordScheme = recordSchemeBuilder.build();
			}
			return recordScheme;
		}

		void setMetadata(Metadata metadata) {
			this.attributes = metadata.attributes;
			this.measures = metadata.measures;
		}

		public void setFilterAttributes(Map<String, ?> filterAttributes) {
			this.filterAttributes = (Map<String, Object>) filterAttributes;
		}

		public void setSortedBy(List<String> sortedBy) {
			this.sortedBy = sortedBy;
		}

		public void setTotalCount(Integer totalCount) {
			this.totalCount = totalCount;
		}

		public void setRecords(List<Record> records) {
			this.records = records;
		}

		public void setTotals(Record totals) {
			this.totals = totals;
		}

		public QueryResult toQueryResult() throws JsonValidationException {
			ReportType reportType = totals != null || totalCount != null ? DATA_WITH_TOTALS : records != null ? DATA : METADATA;
			return switch (reportType) {
				case METADATA -> QueryResult.createForMetadata(ensureRecordScheme(),
					validateNotNull(attributes), validateNotNull(measures));
				case DATA -> QueryResult.createForData(ensureRecordScheme(),
					validateNotNull(attributes), validateNotNull(measures), validateNotNull(sortedBy), validateNotNull(filterAttributes), validateNotNull(records));
				case DATA_WITH_TOTALS -> QueryResult.createForDataWithTotals(ensureRecordScheme(),
					validateNotNull(attributes), validateNotNull(measures), validateNotNull(sortedBy), validateNotNull(filterAttributes), validateNotNull(records), validateNotNull(totals), validateNotNull(totalCount));
			};
		}
	}

	public static JsonCodec<QueryResult> create(
		DefiningClassLoader classLoader,
		JsonCodecFactory factory,
		Map<String, Type> attributeTypes,
		Map<String, Type> measureTypes
	) {
		Map<String, JsonCodec<Object>> attributeCodecs = attributeTypes.entrySet().stream()
			.collect(entriesToHashMap(value -> factory.resolve(value).nullable()));
		Map<String, JsonCodec<Object>> measureCodecs = measureTypes.entrySet().stream()
			.collect(entriesToHashMap(factory::resolve));

		return ObjectJsonCodec.builder(
				() -> new QueryResultBuilder(classLoader, attributeTypes, measureTypes),
				QueryResultBuilder::toQueryResult)
			.with(METADATA_FIELD, QueryResultJsonCodec::getMetadata, QueryResultBuilder::setMetadata,
				ofObject(Metadata::create,
					ATTRIBUTES_FIELD, Metadata::attributes, ofList(ofString()),
					MEASURES_FIELD, Metadata::measures, ofList(ofString())
				))
			.with(FILTER_ATTRIBUTES_FIELD, QueryResultJsonCodec::getFilterAttributes, QueryResultBuilder::setFilterAttributes,
				skipNulls(
					ofMap(attributeCodecs::get)))
			.with(SORTED_BY_FIELD, QueryResultJsonCodec::getSortedBy, QueryResultBuilder::setSortedBy,
				skipNulls(
					ofList(ofString())))
			.with(COUNT_FIELD, QueryResultJsonCodec::getTotalCount, QueryResultBuilder::setTotalCount,
				skipNulls(
					ofInteger()))
			.with(RECORDS_FIELD, QueryResultJsonCodec::getRecords, QueryResultBuilder::setRecords,
				skipNulls(recordScheme ->
					ofList(createRecordJsonCodec(recordScheme, field -> nonNullElse(attributeCodecs.get(field), measureCodecs.get(field))))))
			.with(TOTALS_FIELD, QueryResultJsonCodec::getTotals, QueryResultBuilder::setTotals,
				skipNulls(recordScheme ->
					createRecordJsonCodec(recordScheme, measureCodecs::get)))
			.build();
	}

	static <V> JsonCodecProvider<QueryResult, QueryResultBuilder, V> skipNulls(JsonCodec<V> codec) {
		return new JsonCodecProvider<>() {
			@Override
			public @Nullable JsonCodec<V> encoder(String key, int index, QueryResult item, V value) {
				if (value == null) return null;
				return codec;
			}

			@Override
			public @Nullable JsonCodec<V> decoder(String key, int index, QueryResultBuilder accumulator) {
				return codec;
			}
		};
	}

	static <V> JsonCodecProvider<QueryResult, QueryResultBuilder, V> skipNulls(Function<RecordScheme, JsonCodec<V>> codecFn) {
		return new JsonCodecProvider<>() {
			@Override
			public @Nullable JsonCodec<V> encoder(String key, int index, QueryResult item, V value) {
				if (value == null) return null;
				return codecFn.apply(item.getRecordScheme());
			}

			@Override
			public @Nullable JsonCodec<V> decoder(String key, int index, QueryResultBuilder accumulator) throws JsonValidationException {
				return codecFn.apply(accumulator.ensureRecordScheme());
			}
		};
	}

	static Metadata getMetadata(QueryResult queryResult) {
		return new Metadata(queryResult.getAttributes(), queryResult.getMeasures());
	}

	static Map<String, Object> getFilterAttributes(QueryResult queryResult) {
		return queryResult.getFilterAttributes();
	}

	static List<String> getSortedBy(QueryResult queryResult) {
		return queryResult.getSortedBy();
	}

	static Integer getTotalCount(QueryResult queryResult) {
		return queryResult.getTotals() == null ? null : queryResult.getTotalCount();
	}

	static List<Record> getRecords(QueryResult queryResult) {
		return queryResult.getRecords();
	}

	static Record getTotals(QueryResult queryResult) {
		return queryResult.getTotals();
	}

	@SuppressWarnings("unchecked")
	static AbstractArrayJsonCodec<Record, Record, Object> createRecordJsonCodec(RecordScheme recordScheme,
		Function<String, @Nullable JsonCodec<?>> codecFn
	) {
		List<@Nullable JsonCodec<?>> codecs = recordScheme.getFields().stream().map(codecFn).toList();
		@Nullable JsonEncoder<?>[] encoders = codecs.toArray(JsonCodec[]::new);
		JsonDecoder<?>[] decoders = codecs.stream().filter(Objects::nonNull).toList().toArray(JsonCodec[]::new);
		RecordSetter<?>[] setters = IntStream.range(0, encoders.length).filter(i -> encoders[i] != null).mapToObj(recordScheme::setter).toArray(RecordSetter[]::new);
		return new AbstractArrayJsonCodec<>() {
			@Override
			protected Iterator<Object> iterate(Record item) {
				return item.iterate();
			}

			@Override
			protected @Nullable JsonEncoder<Object> encoder(int index, Record item, Object value) {
				return (JsonEncoder<Object>) encoders[index];
			}

			@Override
			protected JsonDecoder<Object> decoder(int index, Record accumulator) throws JsonValidationException {
				validateArgument(index < decoders.length);
				return (JsonDecoder<Object>) decoders[index];
			}

			@Override
			protected Record accumulator() {
				return recordScheme.record();
			}

			@Override
			protected void accumulate(Record accumulator, int index, Object value) throws JsonValidationException {
				validateArgument(index < setters.length);
				((RecordSetter<Object>) setters[index]).set(accumulator, value);
			}

			@Override
			protected Record result(Record accumulator, int count) throws JsonValidationException {
				validateArgument(count == decoders.length);
				return accumulator;
			}
		};
	}

}
