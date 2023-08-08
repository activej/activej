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
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Utils.entriesToHashMap;
import static io.activej.common.Utils.nonNullElse;
import static io.activej.cube.ReportType.*;
import static io.activej.json.JsonCodecs.*;

public final class QueryResultJsonCodec {

	static final String ATTRIBUTES_FIELD = "attributes";
	static final String MEASURES_FIELD = "measures";
	static final String FILTER_ATTRIBUTES_FIELD = "filterAttributes";
	static final String RECORDS_FIELD = "records";
	static final String TOTALS_FIELD = "totals";
	static final String COUNT_FIELD = "count";
	static final String SORTED_BY_FIELD = "sortedBy";
	static final String METADATA_FIELD = "metadata";

	record Metadata(List<String> attributes, List<String> measures) {}

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
		int totalCount;

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

		void setMetadata(Metadata metadata) throws JsonValidationException {
			this.attributes = metadata.attributes;
			this.measures = metadata.measures;
		}

		public void setFilterAttributes(Map<String, ?> filterAttributes) throws JsonValidationException {
			this.filterAttributes = (Map<String, Object>) filterAttributes;
		}

		public void setSortedBy(List<String> sortedBy) throws JsonValidationException {
			this.sortedBy = sortedBy;
		}

		public void setTotalCount(Integer totalCount) throws JsonValidationException {
			this.totalCount = totalCount;
		}

		public void setRecords(List<Record> records) throws JsonValidationException {
			this.records = records;
		}

		public void setTotals(Record totals) throws JsonValidationException {
			this.totals = totals;
		}

		public QueryResult toQueryResult() throws JsonValidationException {
			ReportType reportType = totals != null ? DATA_WITH_TOTALS : records != null ? DATA : METADATA;
			return QueryResult.create(ensureRecordScheme(),
				attributes, measures, sortedBy, records, totals, totalCount, filterAttributes, reportType);
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
				ofObject(Metadata::new,
					ATTRIBUTES_FIELD, Metadata::attributes, ofList(ofString()),
					MEASURES_FIELD, Metadata::measures, ofList(ofString())
				))
			.with(FILTER_ATTRIBUTES_FIELD, QueryResult::getFilterAttributes, QueryResultBuilder::setFilterAttributes,
				skipNulls(
					ofMap(attributeCodecs::get)))
			.with(SORTED_BY_FIELD, QueryResult::getSortedBy, QueryResultBuilder::setSortedBy,
				skipNulls(
					ofList(ofString())))
			.with(COUNT_FIELD, QueryResultJsonCodec::getTotalCountNullable, QueryResultBuilder::setTotalCount,
				skipNulls(
					ofInteger()))
			.with(RECORDS_FIELD, QueryResult::getRecords, QueryResultBuilder::setRecords,
				codecOf(recordScheme ->
					ofList(createRecordJsonCodec(recordScheme, field -> nonNullElse(attributeCodecs.get(field), measureCodecs.get(field))))))
			.with(TOTALS_FIELD, QueryResult::getTotals, QueryResultBuilder::setTotals,
				codecOf(recordScheme ->
					createRecordJsonCodec(recordScheme, field -> nonNullElse(attributeCodecs.get(field), measureCodecs.get(field)))))
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
			public @Nullable JsonCodec<V> decoder(String key, int index, QueryResultBuilder accumulator) throws JsonValidationException {
				return codec;
			}
		};
	}

	static <V> JsonCodecProvider<QueryResult, QueryResultBuilder, V> codecOf(Function<RecordScheme, JsonCodec<V>> codecFn) {
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

	static Integer getTotalCountNullable(QueryResult queryResult) {
		return queryResult.getTotals() == null ? null : queryResult.getTotalCount();
	}

	static Metadata getMetadata(QueryResult queryResult) {
		return new Metadata(queryResult.getAttributes(), queryResult.getMeasures());
	}

	static AbstractArrayJsonCodec<Record, Record> createRecordJsonCodec(RecordScheme recordScheme,
		Function<String, @Nullable JsonCodec<?>> codecFn
	) {
		List<@Nullable JsonCodec<?>> codecs = recordScheme.getFields().stream().map(codecFn).toList();
		@Nullable JsonEncoder<?>[] encoders = codecs.toArray(JsonCodec[]::new);
		JsonDecoder<?>[] decoders = codecs.stream().filter(Objects::nonNull).toList().toArray(JsonCodec[]::new);
		return new AbstractArrayJsonCodec<>() {
			@Override
			protected Iterator<?> iterate(Record item) {
				return item.iterate();
			}

			@Override
			protected @Nullable JsonEncoder<?> encoder(int index, Record item, Object value) {
				return encoders[index];
			}

			@Override
			protected JsonDecoder<?> decoder(int index, Record accumulator) throws JsonValidationException {
				if (index >= decoders.length) throw new JsonValidationException();
				return checkNotNull(decoders[index]);
			}

			@Override
			protected Record accumulator() {
				return recordScheme.record();
			}

			@Override
			protected void accumulate(Record accumulator, int index, Object value) throws JsonValidationException {
				accumulator.set(index, value);
			}

			@Override
			protected Record result(Record accumulator, int count) throws JsonValidationException {
				if (count != decoders.length) throw new JsonValidationException();
				return accumulator;
			}
		};
	}

}
