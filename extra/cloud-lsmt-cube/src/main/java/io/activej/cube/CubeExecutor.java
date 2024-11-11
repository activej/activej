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

package io.activej.cube;

import io.activej.async.AsyncAccumulator;
import io.activej.codegen.ClassGenerator;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Variable;
import io.activej.codegen.expression.impl.Compare;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.ref.Ref;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.csp.process.frame.FrameFormats;
import io.activej.cube.CubeQuery.Ordering;
import io.activej.cube.CubeState.CompatibleAggregations;
import io.activej.cube.CubeStructure.AttributeResolverContainer;
import io.activej.cube.CubeStructure.PreprocessedQuery;
import io.activej.cube.aggregation.AggregationStats;
import io.activej.cube.aggregation.IAggregationChunkStorage;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.ot.ProtoAggregationDiff;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.aggregation.predicate.AggregationPredicate.ValueResolver;
import io.activej.cube.exception.QueryException;
import io.activej.cube.function.MeasuresFunction;
import io.activej.cube.function.RecordFunction;
import io.activej.cube.function.TotalsFunction;
import io.activej.cube.ot.ProtoCubeDiff;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumerWithResult;
import io.activej.datastream.processor.StreamSplitter;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.reducer.StreamReducer;
import io.activej.datastream.processor.transformer.StreamTransformers;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.etl.ILogDataConsumer;
import io.activej.fs.exception.FileNotFoundException;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.ValueStats;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.iterate;
import static io.activej.common.Utils.not;
import static io.activej.common.Utils.*;
import static io.activej.cube.Utils.createResultClass;
import static io.activej.cube.Utils.filterEntryKeys;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.alwaysFalse;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.alwaysTrue;
import static io.activej.cube.aggregation.util.Utils.*;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class CubeExecutor extends AbstractReactive
	implements ReactiveJmxBeanWithStats {

	private static final Logger logger = LoggerFactory.getLogger(CubeExecutor.class);

	public static final FrameFormat DEFAULT_SORT_FRAME_FORMAT = FrameFormats.lz4();

	private final Executor executor;
	private final DefiningClassLoader classLoader;
	private final IAggregationChunkStorage aggregationChunkStorage;

	private FrameFormat sortFrameFormat = DEFAULT_SORT_FRAME_FORMAT;
	private Path temporarySortDir;

	private final CubeStructure structure;

	// state
	private final Map<String, AggregationExecutor> aggregationExecutors = new LinkedHashMap<>();

	private CubeClassLoaderCache classLoaderCache;

	private int aggregationsChunkSize = AggregationExecutor.DEFAULT_CHUNK_SIZE;
	private int aggregationsReducerBufferSize = AggregationExecutor.DEFAULT_REDUCER_BUFFER_SIZE;
	private int aggregationsSorterItemsInMemory = AggregationExecutor.DEFAULT_SORTER_ITEMS_IN_MEMORY;
	private int aggregationsMaxChunksToConsolidate = AggregationExecutor.DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE;

	// JMX
	private final AggregationStats aggregationStats = new AggregationStats();
	private final ValueStats queryTimes = ValueStats.create(Duration.ofMinutes(10));
	private long queryErrors;
	private Exception queryLastError;

	private CubeExecutor(
		Reactor reactor, CubeStructure structure, Executor executor, DefiningClassLoader classLoader,
		IAggregationChunkStorage aggregationChunkStorage
	) {
		super(reactor);
		this.structure = structure;
		this.executor = executor;
		this.classLoader = classLoader;
		this.aggregationChunkStorage = aggregationChunkStorage;
	}

	public static final class AggregationConfig implements WithInitializer<AggregationConfig> {
		private final String id;
		private int chunkSize;
		private int reducerBufferSize;
		private int sorterItemsInMemory;
		private int maxChunksToConsolidate;

		public AggregationConfig(String id) {
			this.id = id;
		}

		public static AggregationConfig id(String id) {
			return new AggregationConfig(id);
		}

		public AggregationConfig withChunkSize(int chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}

		public AggregationConfig withReducerBufferSize(int reducerBufferSize) {
			this.reducerBufferSize = reducerBufferSize;
			return this;
		}

		public AggregationConfig withSorterItemsInMemory(int sorterItemsInMemory) {
			this.sorterItemsInMemory = sorterItemsInMemory;
			return this;
		}

		public AggregationConfig withMaxChunksToConsolidate(int maxChunksToConsolidate) {
			this.maxChunksToConsolidate = maxChunksToConsolidate;
			return this;
		}
	}

	public static CubeExecutor create(
		Reactor reactor, CubeStructure structure, Executor executor, DefiningClassLoader classLoader,
		IAggregationChunkStorage aggregationChunkStorage
	) {
		return builder(reactor, structure, executor, classLoader, aggregationChunkStorage).build();
	}

	public static Builder builder(
		Reactor reactor, CubeStructure structure, Executor executor, DefiningClassLoader classLoader,
		IAggregationChunkStorage aggregationChunkStorage
	) {
		return new CubeExecutor(reactor, structure, executor, classLoader, aggregationChunkStorage).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, CubeExecutor> {
		private final Map<String, AggregationConfig> aggregationConfigs = new HashMap<>();

		private Builder() {}

		public Builder withAggregationConfig(AggregationConfig aggregationConfig) {
			checkNotBuilt(this);
			checkArgument(!aggregationConfigs.containsKey(aggregationConfig.id), "Aggregation config '%s' is already defined", aggregationConfig.id);
			aggregationConfigs.put(aggregationConfig.id, aggregationConfig);
			return this;
		}

		public Builder withClassLoaderCache(CubeClassLoaderCache classLoaderCache) {
			checkNotBuilt(this);
			checkArgument(iterate(classLoaderCache.getRootClassLoader(), Objects::nonNull, ClassLoader::getParent).anyMatch(isEqual(classLoader)),
				"Unrelated cache ClassLoader");
			CubeExecutor.this.classLoaderCache = classLoaderCache;
			return this;
		}

		public Builder withAggregationsChunkSize(int aggregationsChunkSize) {
			checkNotBuilt(this);
			CubeExecutor.this.aggregationsChunkSize = aggregationsChunkSize;
			return this;
		}

		public Builder withAggregationsReducerBufferSize(int aggregationsReducerBufferSize) {
			checkNotBuilt(this);
			CubeExecutor.this.aggregationsReducerBufferSize = aggregationsReducerBufferSize;
			return this;
		}

		public Builder withAggregationsSorterItemsInMemory(int aggregationsSorterItemsInMemory) {
			checkNotBuilt(this);
			CubeExecutor.this.aggregationsSorterItemsInMemory = aggregationsSorterItemsInMemory;
			return this;
		}

		public Builder withAggregationsMaxChunksToConsolidate(int aggregationsMaxChunksToConsolidate) {
			checkNotBuilt(this);
			checkArgument(aggregationsMaxChunksToConsolidate > 0, "Nothing to consolidate");
			CubeExecutor.this.aggregationsMaxChunksToConsolidate = aggregationsMaxChunksToConsolidate;
			return this;
		}

		public Builder withTemporarySortDir(Path temporarySortDir) {
			checkNotBuilt(this);
			CubeExecutor.this.temporarySortDir = temporarySortDir;
			return this;
		}

		public Builder withSortFrameFormat(FrameFormat sortFrameFormat) {
			checkNotBuilt(this);
			CubeExecutor.this.sortFrameFormat = sortFrameFormat;
			return this;
		}

		@Override
		protected CubeExecutor doBuild() {
			Set<String> difference = difference(aggregationConfigs.keySet(), structure.getAggregationIds());
			checkArgument(difference.isEmpty(), "Found configs for unknown aggregations: " + difference);

			for (Entry<String, AggregationStructure> entry : structure.getAggregationStructures().entrySet()) {
				addAggregation(entry.getKey(), entry.getValue());
			}
			return CubeExecutor.this;
		}

		private void addAggregation(String id, AggregationStructure structure) {
			AggregationConfig config = aggregationConfigs.get(id);

			AggregationExecutor aggregationExecutor = new AggregationExecutor(reactor, executor, classLoader, aggregationChunkStorage, sortFrameFormat, structure);

			aggregationExecutor.setTemporarySortDir(temporarySortDir);
			aggregationExecutor.setChunkSize(config != null && config.chunkSize != 0 ?
				config.chunkSize :
				aggregationsChunkSize);
			aggregationExecutor.setReducerBufferSize(config != null && config.reducerBufferSize != 0 ?
				config.reducerBufferSize :
				aggregationsReducerBufferSize);
			aggregationExecutor.setSorterItemsInMemory(config != null && config.sorterItemsInMemory != 0 ?
				config.sorterItemsInMemory :
				aggregationsSorterItemsInMemory);
			aggregationExecutor.setMaxChunksToConsolidate(config != null && config.maxChunksToConsolidate != 0 ?
				config.maxChunksToConsolidate :
				aggregationsMaxChunksToConsolidate);
			aggregationExecutor.setStats(aggregationStats);

			aggregationExecutors.put(id, aggregationExecutor);
			logger.info("Added aggregation executor {} for id '{}'", aggregationExecutor, id);
		}
	}

	public <T> ILogDataConsumer<T, ProtoCubeDiff> logStreamConsumer(Class<T> inputClass) {
		return logStreamConsumer(inputClass, alwaysTrue());
	}

	public <T> ILogDataConsumer<T, ProtoCubeDiff> logStreamConsumer(Class<T> inputClass, AggregationPredicate predicate) {
		return logStreamConsumer(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass), predicate);
	}

	public <T> ILogDataConsumer<T, ProtoCubeDiff> logStreamConsumer(
		Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields
	) {
		return logStreamConsumer(inputClass, dimensionFields, measureFields, alwaysTrue());
	}

	public <T> ILogDataConsumer<T, ProtoCubeDiff> logStreamConsumer(
		Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields,
		AggregationPredicate predicate
	) {
		return () -> consume(inputClass, dimensionFields, measureFields, predicate)
			.transformResult(result -> result.map(cubeDiff -> List.of(cubeDiff)));
	}

	public <T> StreamConsumerWithResult<T, ProtoCubeDiff> consume(Class<T> inputClass) {
		return consume(inputClass, alwaysTrue());
	}

	public <T> StreamConsumerWithResult<T, ProtoCubeDiff> consume(Class<T> inputClass, AggregationPredicate predicate) {
		return consume(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass), predicate);
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this cube.
	 * The returned {@link StreamConsumer} writes to chosen {@link AggregationExecutor}s using the specified dimensions, measures and input class.
	 *
	 * @param inputClass class of input records
	 * @param <T>        data records type
	 * @return consumer for streaming data to cube
	 */
	public <T> StreamConsumerWithResult<T, ProtoCubeDiff> consume(
		Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields,
		AggregationPredicate dataPredicate
	) {
		checkInReactorThread(this);
		logger.info("Started consuming data. Dimensions: {}. Measures: {}", dimensionFields.keySet(), measureFields.keySet());

		StreamSplitter<T, T> streamSplitter = StreamSplitter.create((item, acceptors) -> {
			for (StreamDataAcceptor<T> acceptor : acceptors) {
				acceptor.accept(item);
			}
		});

		AsyncAccumulator<Map<String, ProtoAggregationDiff>> diffsAccumulator = AsyncAccumulator.create(new HashMap<>());
		Map<String, AggregationPredicate> compatibleAggregations = structure.getCompatibleAggregationsForDataInput(dimensionFields, measureFields, dataPredicate);
		if (compatibleAggregations.isEmpty()) {
			throw new IllegalArgumentException(format(
				"No compatible aggregation for dimensions fields: %s, measureFields: %s",
				dimensionFields, measureFields));
		}

		for (Entry<String, AggregationPredicate> entry : compatibleAggregations.entrySet()) {
			String aggregationId = entry.getKey();
			AggregationExecutor aggregationExecutor = aggregationExecutors.get(aggregationId);

			AggregationStructure aggregationStructure = aggregationExecutor.getStructure();
			List<String> keys = aggregationStructure.getKeys();
			Map<String, String> aggregationKeyFields = filterEntryKeys(dimensionFields.entrySet().stream(), keys::contains)
				.collect(entriesToLinkedHashMap());
			Map<String, String> aggregationMeasureFields = filterEntryKeys(measureFields.entrySet().stream(), aggregationStructure.getMeasures()::contains)
				.collect(entriesToLinkedHashMap());

			AggregationPredicate dataInputFilterPredicate = entry.getValue();
			StreamSupplier<T> output = streamSplitter.newOutput();

			if (!dataInputFilterPredicate.equals(alwaysTrue()) ||
				!aggregationStructure.getPrecondition().equals(alwaysTrue())
			) {
				Predicate<T> filterPredicate = createPredicateWithPrecondition(
					inputClass,
					dataInputFilterPredicate,
					aggregationStructure.getPrecondition(),
					structure.getFieldTypes(),
					classLoader,
					structure.getValidityPredicates()::get);
				output = output.transformWith(StreamTransformers.filter(filterPredicate));
			}

			Promise<ProtoAggregationDiff> consume = output.streamTo(aggregationExecutor.consume(inputClass, aggregationKeyFields, aggregationMeasureFields));
			diffsAccumulator.addPromise(consume, (accumulator, diff) -> accumulator.put(aggregationId, diff));
		}
		return StreamConsumerWithResult.of(streamSplitter.getInput(), diffsAccumulator.run().map(ProtoCubeDiff::new));
	}

	public <T, K extends Comparable, S, A> StreamSupplier<T> queryRawStream(
		List<CompatibleAggregations> compatibleAggregations, List<String> dimensions, List<String> storedMeasures, AggregationPredicate where, Class<T> resultClass,
		DefiningClassLoader queryClassLoader
	) {
		checkInReactorThread(this);

		Class<K> resultKeyClass = createKeyClass(
			dimensions.stream()
				.collect(toLinkedHashMap(structure.getDimensionTypes()::get)),
			queryClassLoader);

		StreamReducer<K, T, A> streamReducer = StreamReducer.create();
		StreamSupplier<T> queryResultSupplier = streamReducer.getOutput();

		for (CompatibleAggregations compatibleAggregation : compatibleAggregations) {
			List<String> compatibleMeasures = compatibleAggregation.measures();
			Class<S> aggregationClass = createRecordClass(
				dimensions.stream()
					.collect(toLinkedHashMap(structure.getDimensionTypes()::get)),
				compatibleMeasures.stream()
					.collect(toLinkedHashMap(m -> structure.getMeasures().get(m).getFieldType())),
				queryClassLoader);

			AggregationExecutor aggregation = aggregationExecutors.get(compatibleAggregation.id());

			AggregationQuery aggregationQuery = new AggregationQuery();
			aggregationQuery.addKeys(dimensions);
			aggregationQuery.addMeasures(compatibleMeasures);
			aggregationQuery.setPredicate(where);
			aggregationQuery.setPrecondition(aggregation.getStructure().getPrecondition());

			StreamSupplier<S> aggregationSupplier = aggregation.query(
				compatibleAggregation.chunks(),
				aggregationQuery,
				aggregationClass,
				queryClassLoader
			);

			if (compatibleAggregations.size() == 1) {
				/*
				If query is fulfilled from the single aggregation,
				just use mapper instead of reducer to copy requested fields.
				 */
				Function<S, T> mapper = createMapper(aggregationClass, resultClass, dimensions,
					compatibleMeasures, queryClassLoader);
				queryResultSupplier = aggregationSupplier
					.transformWith(StreamTransformers.mapper(mapper));
				break;
			}

			Function<S, K> keyFunction = createKeyFunction(aggregationClass, resultKeyClass, dimensions, queryClassLoader);

			Map<String, Measure> extraFields = storedMeasures.stream()
				.filter(not(compatibleMeasures::contains))
				.collect(toLinkedHashMap(structure.getMeasures()::get));
			Reducer<K, S, T, A> reducer = aggregationReducer(aggregation.getStructure(), aggregationClass, resultClass,
				dimensions, compatibleMeasures, extraFields, queryClassLoader);

			StreamConsumer<S> streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			aggregationSupplier.streamTo(streamReducerInput);
		}

		return queryResultSupplier;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	// region temp query() method
	public Promise<QueryResult> query(List<CompatibleAggregations> compatibleAggregations, PreprocessedQuery query) throws QueryException {
		checkInReactorThread(this);
		CubeQuery cubeQuery = query.query();
		DefiningClassLoader queryClassLoader = getQueryClassLoader(new CubeClassLoaderCache.Key(
			new LinkedHashSet<>(cubeQuery.getAttributes()),
			new LinkedHashSet<>(cubeQuery.getMeasures()),
			cubeQuery.getWhere().getDimensions()));
		long queryStarted = reactor.currentTimeMillis();
		return new RequestContext<>().execute(compatibleAggregations, queryClassLoader, query)
			.whenResult(() -> queryTimes.recordValue(reactor.currentTimeMillis() - queryStarted))
			.whenException(e -> {
				queryErrors++;
				queryLastError = e;

				if (e instanceof FileNotFoundException) {
					logger.warn("Query failed because of FileNotFoundException. {}", cubeQuery, e);
				}
			});
	}
	// endregion

	private DefiningClassLoader getQueryClassLoader(CubeClassLoaderCache.Key key) {
		if (classLoaderCache == null)
			return classLoader;
		return classLoaderCache.getOrCreate(key);
	}

	public IAggregationChunkStorage getAggregationChunkStorage() {
		return aggregationChunkStorage;
	}

	public class RequestContext<R> {
		DefiningClassLoader queryClassLoader;
		PreprocessedQuery query;

		AggregationPredicate queryPredicate;
		AggregationPredicate queryHaving;

		Map<String, Object> fullySpecifiedDimensions;

		Class<R> resultClass;
		Predicate<R> havingPredicate;
		final List<String> resultOrderings = new ArrayList<>();
		Comparator<R> comparator;
		MeasuresFunction<R> measuresFunction;
		TotalsFunction<R, R> totalsFunction;

		RecordScheme recordScheme;
		RecordFunction recordFunction;

		Promise<QueryResult> execute(List<CompatibleAggregations> compatibleAggregations, DefiningClassLoader queryClassLoader, PreprocessedQuery query) {
			this.queryClassLoader = queryClassLoader;
			this.query = query;

			CubeQuery cubeQuery = this.query.query();

			queryPredicate = cubeQuery.getWhere().simplify();
			queryHaving = cubeQuery.getHaving().simplify();
			fullySpecifiedDimensions = queryPredicate.getFullySpecifiedDimensions();

			resultClass = createResultClass(query.resultAttributes(), query.resultMeasures(), structure, queryClassLoader);
			recordScheme = createRecordScheme();
			if (cubeQuery.getReportType() == ReportType.METADATA) {
				return Promise.of(QueryResult.createForMetadata(recordScheme, query.recordAttributes(), query.recordMeasures()));
			}
			measuresFunction = createMeasuresFunction();
			totalsFunction = createTotalsFunction();
			comparator = createComparator();
			havingPredicate = createHavingPredicate();
			recordFunction = createRecordFunction();

			return queryRawStream(compatibleAggregations,
				new ArrayList<>(query.resultDimensions()),
				new ArrayList<>(query.resultStoredMeasures()),
				queryPredicate, resultClass, queryClassLoader)
				.toList()
				.then(this::processResults);
		}

		RecordScheme createRecordScheme() {
			RecordScheme.Builder recordSchemeBuilder = RecordScheme.builder(classLoader);
			for (String attribute : query.recordAttributes()) {
				recordSchemeBuilder.withField(attribute, structure.getAttributeType(attribute));
			}
			recordSchemeBuilder.withHashCodeEqualsFields(query.recordAttributes());
			for (String measure : query.recordMeasures()) {
				recordSchemeBuilder.withField(measure, structure.getMeasureType(measure));
			}
			return recordSchemeBuilder.build();
		}

		RecordFunction createRecordFunction() {
			return queryClassLoader.ensureClassAndCreateInstance(
				ClassKey.of(RecordFunction.class, resultClass, recordScheme.getFields()),
				() -> ClassGenerator.builder(RecordFunction.class)
					.withMethod("copyAttributes",
						let(cast(arg(0), resultClass), result ->
							sequence(seq -> {
								for (String field : recordScheme.getFields()) {
									int fieldIndex = recordScheme.getFieldIndex(field);
									if (structure.getDimensionTypes().containsKey(field)) {
										seq.add(call(arg(1), "set", value(fieldIndex),
											cast(structure.getDimensionTypes().get(field).toValue(
												property(result, field)), Object.class)));
									} else if (!structure.getMeasures().containsKey(field) && !structure.getComputedMeasures().containsKey(field)) {
										seq.add(call(arg(1), "set", value(fieldIndex),
											cast(property(result, field.replace('.', '$')), Object.class)));
									}
								}
							})))
					.withMethod("copyMeasures",
						let(cast(arg(0), resultClass), result ->
							sequence(seq -> {
								for (String field : recordScheme.getFields()) {
									int fieldIndex = recordScheme.getFieldIndex(field);
									if (structure.getMeasures().containsKey(field)) {
										Variable fieldValue = property(result, field);
										seq.add(call(arg(1), "set", value(fieldIndex),
											cast(structure.getMeasures().get(field).getFieldType().toValue(
												structure.getMeasures().get(field).valueOfAccumulator(fieldValue)), Object.class)));
									} else if (structure.getComputedMeasures().containsKey(field)) {
										Variable fieldValue = property(result, field);
										seq.add(call(arg(1), "set", value(fieldIndex),
											cast(fieldValue, Object.class)));
									}
								}
							})))
					.build()
			);
		}

		MeasuresFunction<R> createMeasuresFunction() {
			return queryClassLoader.ensureClassAndCreateInstance(
				ClassKey.of(MeasuresFunction.class, resultClass, query.resultComputedMeasures()),
				() -> ClassGenerator.builder(MeasuresFunction.class)
					.initialize(b ->
						query.resultComputedMeasures().forEach(computedMeasure ->
							b.withField(computedMeasure, structure.getComputedMeasures().get(computedMeasure).getType(structure.getMeasures()))))
					.withMethod("computeMeasures", let(cast(arg(0), resultClass), record ->
						sequence(seq -> {
							for (String computedMeasure : query.resultComputedMeasures()) {
								seq.add(set(property(record, computedMeasure),
									structure.getComputedMeasures().get(computedMeasure).getExpression(record, structure.getMeasures())));
							}
						})))
					.build()
			);
		}

		private Predicate<R> createHavingPredicate() {
			if (queryHaving == alwaysTrue()) return o -> true;
			if (queryHaving == alwaysFalse()) return o -> false;

			return queryClassLoader.ensureClassAndCreateInstance(
				ClassKey.of(Predicate.class, resultClass, queryHaving),
				() -> {
					ValueResolver valueResolver = createValueResolverOfMeasures(
						structure.getFieldTypes(),
						structure.getMeasures()
					);
					return ClassGenerator.builder(Predicate.class)
						.withMethod("test",
							let(cast(arg(0), resultClass), value -> queryHaving.createPredicate(value, valueResolver)))
						.build();
				}
			);
		}

		@SuppressWarnings("unchecked")
		Comparator<R> createComparator() {
			if (query.query().getOrderings().isEmpty())
				return (o1, o2) -> 0;

			for (Ordering ordering : query.query().getOrderings()) {
				String field = ordering.getField();
				if (query.resultMeasures().contains(field) || query.resultAttributes().contains(field)) {
					resultOrderings.add(field);
				}
			}

			return queryClassLoader.ensureClassAndCreateInstance(
				ClassKey.of(Comparator.class, resultClass, query.query().getOrderings()),
				() -> ClassGenerator.builder(Comparator.class)
					.withMethod("compare", () -> {
						Compare.Builder compareBuilder = Compare.builder();
						for (Ordering ordering : query.query().getOrderings()) {
							String field = ordering.getField();
							if (query.resultMeasures().contains(field) || query.resultAttributes().contains(field)) {
								String property = field.replace('.', '$');
								compareBuilder.with(
									ordering.isAsc() ? leftProperty(resultClass, property) : rightProperty(resultClass, property),
									ordering.isAsc() ? rightProperty(resultClass, property) : leftProperty(resultClass, property),
									true);
							}
						}
						return compareBuilder.build();
					})
					.build()
			);
		}

		Promise<QueryResult> processResults(List<R> results) {
			R totals;
			try {
				totals = resultClass.getDeclaredConstructor().newInstance();
			} catch (
				InstantiationException |
				IllegalAccessException |
				NoSuchMethodException |
				InvocationTargetException e
			) {
				throw new RuntimeException(e);
			}

			if (results.isEmpty()) {
				totalsFunction.zero(totals);
			} else {
				Iterator<R> iterator = results.iterator();
				R first = iterator.next();
				measuresFunction.computeMeasures(first);
				totalsFunction.init(totals, first);
				while (iterator.hasNext()) {
					R next = iterator.next();
					measuresFunction.computeMeasures(next);
					totalsFunction.accumulate(totals, next);
				}
				totalsFunction.computeMeasures(totals);
			}

			Record totalRecord = recordScheme.record();
			recordFunction.copyMeasures(totals, totalRecord);

			List<Promise<Void>> tasks = new ArrayList<>();
			Map<String, Object> filterAttributes = new LinkedHashMap<>();
			for (AttributeResolverContainer resolverContainer : structure.getAttributeResolvers()) {
				List<String> attributes = new ArrayList<>(resolverContainer.attributes);
				attributes.retainAll(query.resultAttributes());
				if (!attributes.isEmpty()) {
					tasks.add(Utils.resolveAttributes(results, resolverContainer.resolver,
						resolverContainer.dimensions, attributes,
						fullySpecifiedDimensions, resultClass, structure, queryClassLoader));
				}
			}

			for (AttributeResolverContainer resolverContainer : structure.getAttributeResolvers()) {
				if (fullySpecifiedDimensions.keySet().containsAll(resolverContainer.dimensions)) {
					tasks.add(resolveSpecifiedDimensions(resolverContainer, filterAttributes));
				}
			}
			return Promises.all(tasks)
				.map($ -> processResults2(results, totals, filterAttributes));
		}

		QueryResult processResults2(List<R> results, R totals, Map<String, Object> filterAttributes) {
			results = (hasAllResultDimensions() ? results.stream() : remergeRecords(results))
				.filter(havingPredicate)
				.collect(toList());

			int totalCount = results.size();

			results = applyLimitAndOffset(results);

			List<Record> resultRecords = new ArrayList<>(results.size());
			for (R result : results) {
				Record record = recordScheme.record();
				recordFunction.copyAttributes(result, record);
				recordFunction.copyMeasures(result, record);
				resultRecords.add(record);
			}

			if (query.query().getReportType() == ReportType.DATA) {
				return QueryResult.createForData(recordScheme,
					query.recordAttributes(), query.recordMeasures(),
					resultOrderings, filterAttributes,
					resultRecords
				);
			}

			if (query.query().getReportType() == ReportType.DATA_WITH_TOTALS) {
				Record totalRecord = recordScheme.record();
				recordFunction.copyMeasures(totals, totalRecord);
				return QueryResult.createForDataWithTotals(recordScheme,
					query.recordAttributes(), query.recordMeasures(),
					resultOrderings, filterAttributes,
					resultRecords, totalRecord, totalCount
				);
			}

			throw new AssertionError();
		}

		private boolean hasAllResultDimensions() {
			return new HashSet<>(query.recordAttributes()).containsAll(query.resultDimensions());
		}

		private <K extends Comparable> Stream<R> remergeRecords(List<R> results) {
			Class<K> keyClass = createKeyClass(queryClassLoader,
				query.recordAttributes().stream()
					.collect(toLinkedHashMap(
						attribute -> attribute.replace(".", "$"),
						structure::getAttributeInternalType)
					)
			);
			Function<R, K> keyFunction = createKeyFunction(resultClass, keyClass,
				query.recordAttributes().stream().map(attribute -> attribute.replace(".", "$")).toList(),
				queryClassLoader);

			LinkedHashMap<K, R> map = new LinkedHashMap<>(results.size());
			results.forEach(r -> {
				K key = keyFunction.apply(r);
				map.compute(key, (k, v) -> {
					if (v == null) return r;
					totalsFunction.accumulate(v, r);
					return v;
				});
			});
			return map.values().stream().peek(r -> totalsFunction.computeMeasures(r));
		}

		private Promise<Void> resolveSpecifiedDimensions(
			AttributeResolverContainer resolverContainer, Map<String, Object> result
		) {
			Object[] key = new Object[resolverContainer.dimensions.size()];
			for (int i = 0; i < resolverContainer.dimensions.size(); i++) {
				String dimension = resolverContainer.dimensions.get(i);
				FieldType fieldType = structure.getDimensionTypes().get(dimension);
				key[i] = fieldType.toInternalValue(fullySpecifiedDimensions.get(dimension));
			}

			Ref<Object> attributesRef = new Ref<>();
			return resolverContainer.resolver.resolveAttributes(List.of((Object) key),
					result1 -> (Object[]) result1,
					(result12, attributes) -> attributesRef.value = attributes)
				.whenResult(() -> {
					for (int i = 0; i < resolverContainer.attributes.size(); i++) {
						String attribute = resolverContainer.attributes.get(i);
						result.put(attribute, attributesRef.value != null ? ((Object[]) attributesRef.value)[i] : null);
					}
				});
		}

		List<R> applyLimitAndOffset(List<R> results) {
			Integer offset = query.query().getOffset();
			Integer limit = query.query().getLimit();
			int start;
			int end;

			if (offset == null) {
				start = 0;
				offset = 0;
			} else if (offset >= results.size()) {
				return new ArrayList<>();
			} else {
				start = offset;
			}

			if (limit == null) {
				end = results.size();
				limit = results.size();
			} else {
				end = min(start + limit, results.size());
			}

			if (comparator != null) {
				return results.stream()
					.sorted(comparator)
					.skip(offset)
					.limit(limit)
					.collect(toList());
			}

			return results.subList(start, end);
		}

		TotalsFunction<R, R> createTotalsFunction() {
			return queryClassLoader.ensureClassAndCreateInstance(
				ClassKey.of(TotalsFunction.class, resultClass, query.resultStoredMeasures(), query.resultComputedMeasures()),
				() -> ClassGenerator.builder(TotalsFunction.class)
					.withMethod("zero",
						let(cast(arg(0), resultClass), totalRecord ->
							sequence(seq -> {
								for (String field : query.resultStoredMeasures()) {
									Measure measure = structure.getMeasures().get(field);
									seq.add(measure.zeroAccumulator(property(totalRecord, field)));
								}
							})))
					.withMethod("init",
						let(List.of(
								cast(arg(0), resultClass),
								cast(arg(1), resultClass)
							),
							variables -> sequence(seq -> {
								Variable totalRecord = variables[0];
								Variable firstRecord = variables[1];

								for (String field : query.resultStoredMeasures()) {
									Measure measure = structure.getMeasures().get(field);
									seq.add(measure.initAccumulatorWithAccumulator(
										property(totalRecord, field),
										property(firstRecord, field)));
								}
							})))
					.withMethod("accumulate",
						let(List.of(
								cast(arg(0), resultClass),
								cast(arg(1), resultClass)
							),
							variables -> sequence(seq -> {
								Variable totalRecord = variables[0];
								Variable record = variables[1];

								for (String field : query.resultStoredMeasures()) {
									Measure measure = structure.getMeasures().get(field);
									seq.add(measure.reduce(
										property(totalRecord, field),
										property(record, field)));
								}
							})))
					.withMethod("computeMeasures",
						let(cast(arg(0), resultClass), totalRecord ->
							sequence(seq -> {
								for (String computedMeasure : query.resultComputedMeasures()) {
									seq.add(set(property(totalRecord, computedMeasure),
										structure.getComputedMeasures().get(computedMeasure).getExpression(totalRecord, structure.getMeasures())));
								}
							})))
					.build()
			);
		}

	}

	@Override
	public String toString() {
		return
			"CubeExecutor{" +
			"aggregationExecutors=" + aggregationExecutors +
			'}';
	}

	// jmx
	@JmxAttribute
	public int getAggregationsChunkSize() {
		return aggregationsChunkSize;
	}

	@JmxAttribute
	public void setAggregationsChunkSize(int aggregationsChunkSize) {
		this.aggregationsChunkSize = aggregationsChunkSize;
		for (AggregationExecutor aggregation : aggregationExecutors.values()) {
			aggregation.setChunkSize(aggregationsChunkSize);
		}
	}

	@JmxAttribute
	public int getAggregationsSorterItemsInMemory() {
		return aggregationsSorterItemsInMemory;
	}

	@JmxAttribute
	public void setAggregationsSorterItemsInMemory(int aggregationsSorterItemsInMemory) {
		this.aggregationsSorterItemsInMemory = aggregationsSorterItemsInMemory;
		for (AggregationExecutor aggregationExecutor : aggregationExecutors.values()) {
			aggregationExecutor.setSorterItemsInMemory(aggregationsSorterItemsInMemory);
		}
	}

	@JmxAttribute
	public int getAggregationsMaxChunksToConsolidate() {
		return aggregationsMaxChunksToConsolidate;
	}

	@JmxAttribute
	public void setAggregationsMaxChunksToConsolidate(int aggregationsMaxChunksToConsolidate) {
		checkArgument(aggregationsMaxChunksToConsolidate > 0, "Nothing to consolidate");
		this.aggregationsMaxChunksToConsolidate = aggregationsMaxChunksToConsolidate;
		for (AggregationExecutor aggregationExecutor : aggregationExecutors.values()) {
			aggregationExecutor.setMaxChunksToConsolidate(aggregationsMaxChunksToConsolidate);
		}
	}

	@JmxAttribute
	public ValueStats getQueryTimes() {
		return queryTimes;
	}

	@JmxAttribute
	public long getQueryErrors() {
		return queryErrors;
	}

	@JmxAttribute
	public Exception getQueryLastError() {
		return queryLastError;
	}

	@JmxAttribute
	public AggregationStats getAggregationStats() {
		return aggregationStats;
	}

	@JmxAttribute
	public Map<String, AggregationExecutor> getAggregationExecutors() {
		return aggregationExecutors;
	}

}
