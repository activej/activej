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

import io.activej.aggregation.*;
import io.activej.aggregation.AggregationState.ConsolidationDebugInfo;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.measure.Measure;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.async.AsyncAccumulator;
import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.ClassBuilder;
import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.ExpressionComparator;
import io.activej.codegen.expression.Variable;
import io.activej.common.api.WithInitializer;
import io.activej.common.record.Record;
import io.activej.common.record.RecordScheme;
import io.activej.common.ref.Ref;
import io.activej.cube.CubeQuery.Ordering;
import io.activej.cube.attributes.AttributeResolver;
import io.activej.cube.function.MeasuresFunction;
import io.activej.cube.function.RecordFunction;
import io.activej.cube.function.TotalsFunction;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerWithResult;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamFilter;
import io.activej.datastream.processor.StreamMapper;
import io.activej.datastream.processor.StreamReducer;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.datastream.processor.StreamSplitter;
import io.activej.etl.LogDataConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.eventloop.jmx.EventloopJmxBeanEx;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.stats.ValueStats;
import io.activej.ot.OTState;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.aggregation.util.Utils.*;
import static io.activej.codegen.expression.ExpressionComparator.leftProperty;
import static io.activej.codegen.expression.ExpressionComparator.rightProperty;
import static io.activej.codegen.expression.Expressions.*;
import static io.activej.codegen.util.Primitives.isWrapperType;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.of;
import static io.activej.common.collection.CollectionUtils.entriesToMap;
import static io.activej.common.collection.CollectionUtils.keysToMap;
import static io.activej.cube.Utils.createResultClass;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import static java.util.stream.Collectors.toList;

/**
 * Represents an OLAP cube. Provides methods for loading and querying data.
 * Also provides functionality for managing aggregations.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class Cube implements ICube, OTState<CubeDiff>, WithInitializer<Cube>, EventloopJmxBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(Cube.class);

	public static final int DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD = 300;

	private final Eventloop eventloop;
	private final Executor executor;
	private final DefiningClassLoader classLoader;
	private final AggregationChunkStorage aggregationChunkStorage;
	private Path temporarySortDir;

	private final Map<String, FieldType> fieldTypes = new LinkedHashMap<>();
	private final Map<String, FieldType> dimensionTypes = new LinkedHashMap<>();
	private final Map<String, Measure> measures = new LinkedHashMap<>();
	private final Map<String, ComputedMeasure> computedMeasures = new LinkedHashMap<>();

	private static final class AttributeResolverContainer {
		private final List<String> attributes = new ArrayList<>();
		private final List<String> dimensions;
		private final AttributeResolver resolver;

		private AttributeResolverContainer(List<String> dimensions, AttributeResolver resolver) {
			this.dimensions = dimensions;
			this.resolver = resolver;
		}
	}

	private final List<AttributeResolverContainer> attributeResolvers = new ArrayList<>();
	private final Map<String, Class<?>> attributeTypes = new LinkedHashMap<>();
	private final Map<String, AttributeResolverContainer> attributes = new LinkedHashMap<>();

	private final Map<String, String> childParentRelations = new LinkedHashMap<>();

	// settings
	private int aggregationsChunkSize = Aggregation.DEFAULT_CHUNK_SIZE;
	private int aggregationsReducerBufferSize = Aggregation.DEFAULT_REDUCER_BUFFER_SIZE;
	private int aggregationsSorterItemsInMemory = Aggregation.DEFAULT_SORTER_ITEMS_IN_MEMORY;
	private int aggregationsMaxChunksToConsolidate = Aggregation.DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE;
	private boolean aggregationsIgnoreChunkReadingExceptions = false;

	private int maxOverlappingChunksToProcessLogs = Cube.DEFAULT_OVERLAPPING_CHUNKS_THRESHOLD;
	private Duration maxIncrementalReloadPeriod = Aggregation.DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD;

	static final class AggregationContainer {
		private final Aggregation aggregation;
		private final List<String> measures;
		private final AggregationPredicate predicate;

		private AggregationContainer(Aggregation aggregation, List<String> measures, AggregationPredicate predicate) {
			this.aggregation = aggregation;
			this.measures = measures;
			this.predicate = predicate;
		}

		@Override
		public String toString() {
			return aggregation.toString();
		}
	}

	// state
	private final Map<String, AggregationContainer> aggregations = new LinkedHashMap<>();

	private CubeClassLoaderCache classLoaderCache;

	// JMX
	private final AggregationStats aggregationStats = new AggregationStats();
	private final ValueStats queryTimes = ValueStats.create(Duration.ofMinutes(10));
	private long queryErrors;
	private Throwable queryLastError;

	Cube(Eventloop eventloop, Executor executor, DefiningClassLoader classLoader,
			AggregationChunkStorage aggregationChunkStorage) {
		this.eventloop = eventloop;
		this.executor = executor;
		this.classLoader = classLoader;
		this.aggregationChunkStorage = aggregationChunkStorage;
	}

	public static Cube create(@NotNull Eventloop eventloop, @NotNull Executor executor, @NotNull DefiningClassLoader classLoader,
			@NotNull AggregationChunkStorage aggregationChunkStorage) {
		return new Cube(eventloop, executor, classLoader, aggregationChunkStorage);
	}

	public Cube withAttribute(String attribute, AttributeResolver resolver) {
		checkArgument(!attributes.containsKey(attribute), "Attribute %s has already been defined", attribute);
		int pos = attribute.indexOf('.');
		if (pos == -1)
			throw new IllegalArgumentException("Attribute identifier is not split into name and dimension");
		String dimension = attribute.substring(0, pos);
		String attributeName = attribute.substring(pos + 1);
		checkArgument(resolver.getAttributeTypes().containsKey(attributeName), "Resolver does not support %s", attribute);
		checkArgument(!isWrapperType(resolver.getAttributeTypes().get(attributeName)), "Unsupported attribute type for %s", attribute);
		List<String> dimensions = getAllParents(dimension);
		checkArgument(dimensions.size() == resolver.getKeyTypes().length, "Parent dimensions: %s, key types: %s", dimensions, asList(resolver.getKeyTypes()));
		for (int i = 0; i < dimensions.size(); i++) {
			String d = dimensions.get(i);
			checkArgument(((Class<?>) dimensionTypes.get(d).getInternalDataType()).equals(resolver.getKeyTypes()[i]), "Dimension type mismatch for %s", d);
		}
		AttributeResolverContainer resolverContainer = null;
		for (AttributeResolverContainer r : attributeResolvers) {
			if (r.resolver == resolver) {
				resolverContainer = r;
				break;
			}
		}
		if (resolverContainer == null) {
			resolverContainer = new AttributeResolverContainer(dimensions, resolver);
			attributeResolvers.add(resolverContainer);
		}
		resolverContainer.attributes.add(attribute);
		attributes.put(attribute, resolverContainer);
		attributeTypes.put(attribute, resolver.getAttributeTypes().get(attributeName));
		return this;
	}

	public Cube withClassLoaderCache(CubeClassLoaderCache classLoaderCache) {
		this.classLoaderCache = classLoaderCache;
		return this;
	}

	public Cube withDimension(String dimensionId, FieldType type) {
		addDimension(dimensionId, type);
		return this;
	}

	public Cube withMeasure(String measureId, Measure measure) {
		addMeasure(measureId, measure);
		return this;
	}

	public Cube withComputedMeasure(String measureId, ComputedMeasure computedMeasure) {
		addComputedMeasure(measureId, computedMeasure);
		return this;
	}

	public Cube withRelation(String child, String parent) {
		addRelation(child, parent);
		return this;
	}

	public Cube withTemporarySortDir(Path temporarySortDir) {
		this.temporarySortDir = temporarySortDir;
		return this;
	}

	public static final class AggregationConfig implements WithInitializer<AggregationConfig> {
		private final String id;
		private final List<String> dimensions = new ArrayList<>();
		private final List<String> measures = new ArrayList<>();
		private AggregationPredicate predicate = AggregationPredicates.alwaysTrue();
		private final List<String> partitioningKey = new ArrayList<>();
		private int chunkSize;
		private int reducerBufferSize;
		private int sorterItemsInMemory;
		private int maxChunksToConsolidate;

		public AggregationConfig(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		public static AggregationConfig id(String id) {
			return new AggregationConfig(id);
		}

		public AggregationConfig withDimensions(Collection<String> dimensions) {
			this.dimensions.addAll(dimensions);
			return this;
		}

		public AggregationConfig withDimensions(String... dimensions) {
			return withDimensions(asList(dimensions));
		}

		public AggregationConfig withMeasures(Collection<String> measures) {
			this.measures.addAll(measures);
			return this;
		}

		public AggregationConfig withMeasures(String... measures) {
			return withMeasures(asList(measures));
		}

		public AggregationConfig withPredicate(AggregationPredicate predicate) {
			this.predicate = predicate;
			return this;
		}

		public AggregationConfig withPartitioningKey(List<String> partitioningKey) {
			this.partitioningKey.addAll(partitioningKey);
			return this;
		}

		public AggregationConfig withPartitioningKey(String... partitioningKey) {
			this.partitioningKey.addAll(asList(partitioningKey));
			return this;
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

	public Cube withAggregation(AggregationConfig aggregationConfig) {
		addAggregation(aggregationConfig);
		return this;
	}

	private static <K, V> Stream<Entry<K, V>> filterEntryKeys(Stream<Entry<K, V>> stream, Predicate<K> predicate) {
		return stream.filter(entry -> predicate.test(entry.getKey()));
	}

	public void addMeasure(String measureId, Measure measure) {
		checkState(aggregations.isEmpty(), "Cannot add measure while aggregations are present");
		measures.put(measureId, measure);
		fieldTypes.put(measureId, measure.getFieldType());
	}

	public void addComputedMeasure(String measureId, ComputedMeasure computedMeasure) {
		checkState(aggregations.isEmpty(), "Cannot add computed measure while aggregations are present");
		computedMeasures.put(measureId, computedMeasure);
	}

	public void addRelation(String child, String parent) {
		childParentRelations.put(child, parent);
	}

	public void addDimension(String dimensionId, FieldType type) {
		checkState(aggregations.isEmpty(), "Cannot add dimension while aggregations are present");
		dimensionTypes.put(dimensionId, type);
		fieldTypes.put(dimensionId, type);
	}

	public void addAggregation(AggregationConfig config) {
		checkArgument(!aggregations.containsKey(config.id), "Aggregation '%s' is already defined", config.id);

		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
				.withInitializer(s -> config.dimensions.forEach(dimensionId ->
						s.withKey(dimensionId, dimensionTypes.get(dimensionId))))
				.withInitializer(s -> config.measures.forEach(measureId ->
						s.withMeasure(measureId, measures.get(measureId))))
				.withInitializer(s -> measures.forEach((measureId, measure) -> {
					if (!config.measures.contains(measureId)) {
						s.withIgnoredMeasure(measureId, measure.getFieldType());
					}
				}))
				.withPartitioningKey(config.partitioningKey);

		Aggregation aggregation = Aggregation.create(eventloop, executor, classLoader, aggregationChunkStorage, structure)
				.withTemporarySortDir(temporarySortDir)
				.withChunkSize(config.chunkSize != 0 ? config.chunkSize : aggregationsChunkSize)
				.withReducerBufferSize(config.reducerBufferSize != 0 ? config.reducerBufferSize : aggregationsReducerBufferSize)
				.withSorterItemsInMemory(config.sorterItemsInMemory != 0 ? config.sorterItemsInMemory : aggregationsSorterItemsInMemory)
				.withMaxChunksToConsolidate(config.maxChunksToConsolidate != 0 ? config.maxChunksToConsolidate : aggregationsMaxChunksToConsolidate)
				.withIgnoreChunkReadingExceptions(aggregationsIgnoreChunkReadingExceptions)
				.withStats(aggregationStats);

		aggregations.put(config.id, new AggregationContainer(aggregation, config.measures, config.predicate));
		logger.info("Added aggregation {} for id '{}'", aggregation, config.id);
	}

	@NotNull
	public Class<?> getAttributeInternalType(String attribute) {
		if (dimensionTypes.containsKey(attribute))
			return dimensionTypes.get(attribute).getInternalDataType();
		if (attributeTypes.containsKey(attribute))
			return attributeTypes.get(attribute);
		throw new IllegalArgumentException("No attribute: " + attribute);
	}

	@NotNull
	public Class<?> getMeasureInternalType(String field) {
		if (measures.containsKey(field))
			return measures.get(field).getFieldType().getInternalDataType();
		if (computedMeasures.containsKey(field))
			return computedMeasures.get(field).getType(measures);
		throw new IllegalArgumentException("No measure: " + field);
	}

	@NotNull
	public Type getAttributeType(String attribute) {
		if (dimensionTypes.containsKey(attribute))
			return dimensionTypes.get(attribute).getDataType();
		if (attributeTypes.containsKey(attribute))
			return attributeTypes.get(attribute);
		throw new IllegalArgumentException("No attribute: " + attribute);
	}

	@NotNull
	public Type getMeasureType(String field) {
		if (measures.containsKey(field))
			return measures.get(field).getFieldType().getDataType();
		if (computedMeasures.containsKey(field))
			return computedMeasures.get(field).getType(measures);
		throw new IllegalArgumentException("No measure: " + field);
	}

	@Override
	public Map<String, Type> getAttributeTypes() {
		Map<String, Type> result = new LinkedHashMap<>();
		for (Entry<String, FieldType> entry : dimensionTypes.entrySet()) {
			result.put(entry.getKey(), entry.getValue().getDataType());
		}
		result.putAll(attributeTypes);
		return result;
	}

	@Override
	public Map<String, Type> getMeasureTypes() {
		Map<String, Type> result = new LinkedHashMap<>();
		for (Entry<String, Measure> entry : measures.entrySet()) {
			result.put(entry.getKey(), entry.getValue().getFieldType().getDataType());
		}
		for (Entry<String, ComputedMeasure> entry : computedMeasures.entrySet()) {
			result.put(entry.getKey(), entry.getValue().getType(measures));
		}
		return result;
	}

	public Aggregation getAggregation(String aggregationId) {
		return aggregations.get(aggregationId).aggregation;
	}

	public Set<String> getAggregationIds() {
		return aggregations.keySet();
	}

	@Override
	public void init() {
		for (AggregationContainer container : aggregations.values()) {
			container.aggregation.getState().init();
		}
	}

	@Override
	public void apply(CubeDiff op) {
		for (String aggregationId : op.keySet()) {
			AggregationDiff aggregationDiff = op.get(aggregationId);
			aggregations.get(aggregationId).aggregation.getState().apply(aggregationDiff);
		}
	}

	public <T> LogDataConsumer<T, CubeDiff> logStreamConsumer(Class<T> inputClass) {
		return logStreamConsumer(inputClass, AggregationPredicates.alwaysTrue());
	}

	public <T> LogDataConsumer<T, CubeDiff> logStreamConsumer(Class<T> inputClass,
			AggregationPredicate predicate) {
		return logStreamConsumer(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass), predicate);
	}

	public <T> LogDataConsumer<T, CubeDiff> logStreamConsumer(Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields) {
		return logStreamConsumer(inputClass, dimensionFields, measureFields, AggregationPredicates.alwaysTrue());
	}

	public <T> LogDataConsumer<T, CubeDiff> logStreamConsumer(Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields,
			AggregationPredicate predicate) {
		return () -> consume(inputClass, dimensionFields, measureFields, predicate)
				.transformResult(result -> result.map(Collections::singletonList));
	}

	public <T> StreamConsumerWithResult<T, CubeDiff> consume(Class<T> inputClass) {
		return consume(inputClass, AggregationPredicates.alwaysTrue());
	}

	public <T> StreamConsumerWithResult<T, CubeDiff> consume(Class<T> inputClass, AggregationPredicate predicate) {
		return consume(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass), predicate);
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this cube.
	 * The returned {@link StreamConsumer} writes to {@link Aggregation}'s chosen using the specified dimensions, measures and input class.
	 *
	 * @param inputClass class of input records
	 * @param <T>        data records type
	 * @return consumer for streaming data to cube
	 */
	public <T> StreamConsumerWithResult<T, CubeDiff> consume(Class<T> inputClass, Map<String, String> dimensionFields, Map<String, String> measureFields,
			AggregationPredicate dataPredicate) {
		logger.info("Started consuming data. Dimensions: {}. Measures: {}", dimensionFields.keySet(), measureFields.keySet());

		StreamSplitter<T, T> streamSplitter = StreamSplitter.create((item, acceptors) -> {
			for (StreamDataAcceptor<T> acceptor : acceptors) {
				acceptor.accept(item);
			}
		});

		AsyncAccumulator<Map<String, AggregationDiff>> diffsAccumulator = AsyncAccumulator.create(new HashMap<>());
		Map<String, AggregationPredicate> compatibleAggregations = getCompatibleAggregationsForDataInput(dimensionFields, measureFields, dataPredicate);
		if (compatibleAggregations.size() == 0) {
			throw new IllegalArgumentException(format("No compatible aggregation for " +
					"dimensions fields: %s, measureFields: %s", dimensionFields, measureFields));
		}

		for (Entry<String, AggregationPredicate> aggregationToDataInputFilterPredicate : compatibleAggregations.entrySet()) {
			String aggregationId = aggregationToDataInputFilterPredicate.getKey();
			AggregationContainer aggregationContainer = aggregations.get(aggregationToDataInputFilterPredicate.getKey());
			Aggregation aggregation = aggregationContainer.aggregation;

			List<String> keys = aggregation.getKeys();
			Map<String, String> aggregationKeyFields = entriesToMap(filterEntryKeys(dimensionFields.entrySet().stream(), keys::contains));
			Map<String, String> aggregationMeasureFields = entriesToMap(filterEntryKeys(measureFields.entrySet().stream(), aggregationContainer.measures::contains));

			AggregationPredicate dataInputFilterPredicate = aggregationToDataInputFilterPredicate.getValue();
			StreamSupplier<T> output = streamSplitter.newOutput();
			if (!dataInputFilterPredicate.equals(AggregationPredicates.alwaysTrue())) {
				Predicate<T> filterPredicate = createFilterPredicate(inputClass, dataInputFilterPredicate, classLoader, fieldTypes);
				output = output
						.transformWith(StreamFilter.create(filterPredicate));
			}
			Promise<AggregationDiff> consume = output.streamTo(aggregation.consume(inputClass, aggregationKeyFields, aggregationMeasureFields));
			diffsAccumulator.addPromise(consume, (accumulator, diff) -> accumulator.put(aggregationId, diff));
		}
		return StreamConsumerWithResult.of(streamSplitter.getInput(), diffsAccumulator.run().promise().map(CubeDiff::of));
	}

	Map<String, AggregationPredicate> getCompatibleAggregationsForDataInput(Map<String, String> dimensionFields,
			Map<String, String> measureFields,
			AggregationPredicate predicate) {
		AggregationPredicate dataPredicate = predicate.simplify();
		Map<String, AggregationPredicate> aggregationToDataInputFilterPredicate = new HashMap<>();
		for (Entry<String, AggregationContainer> aggregationContainer : aggregations.entrySet()) {
			AggregationContainer container = aggregationContainer.getValue();
			Aggregation aggregation = container.aggregation;

			Set<String> dimensions = dimensionFields.keySet();
			if (!dimensions.containsAll(aggregation.getKeys())) continue;

			Map<String, String> aggregationMeasureFields = entriesToMap(filterEntryKeys(measureFields.entrySet().stream(), container.measures::contains));
			if (aggregationMeasureFields.isEmpty()) continue;

			AggregationPredicate containerPredicate = container.predicate.simplify();

			AggregationPredicate intersection = AggregationPredicates.and(containerPredicate, dataPredicate).simplify();
			if (AggregationPredicates.alwaysFalse().equals(intersection)) continue;

			if (intersection.equals(containerPredicate)) {
				aggregationToDataInputFilterPredicate.put(aggregationContainer.getKey(), AggregationPredicates.alwaysTrue());
				continue;
			}

			aggregationToDataInputFilterPredicate.put(aggregationContainer.getKey(), containerPredicate);
		}
		return aggregationToDataInputFilterPredicate;
	}

	static Predicate createFilterPredicate(Class<?> inputClass,
			AggregationPredicate predicate,
			DefiningClassLoader classLoader,
			Map<String, FieldType> keyTypes) {
		return ClassBuilder.create(classLoader, Predicate.class)
				.withClassKey(inputClass, predicate)
				.withMethod("test", boolean.class, singletonList(Object.class),
						predicate.createPredicate(cast(arg(0), inputClass), keyTypes))
				.buildClassAndCreateNewInstance();
	}

	/**
	 * Returns a {@link StreamSupplier} of the records retrieved from cube for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param resultClass class of output records
	 * @return supplier that streams query results
	 */
	public <T> StreamSupplier<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate where,
			Class<T> resultClass) {
		return queryRawStream(dimensions, storedMeasures, where, resultClass, classLoader);
	}

	public <T> StreamSupplier<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate where,
			Class<T> resultClass, DefiningClassLoader queryClassLoader) {

		List<AggregationContainer> compatibleAggregations = getCompatibleAggregationsForQuery(dimensions, storedMeasures, where);

		return queryRawStream(dimensions, storedMeasures, where, resultClass, queryClassLoader, compatibleAggregations);
	}

	private <T, K extends Comparable, S, A> StreamSupplier<T> queryRawStream(List<String> dimensions, List<String> storedMeasures, AggregationPredicate where,
			Class<T> resultClass, DefiningClassLoader queryClassLoader,
			List<AggregationContainer> compatibleAggregations) {
		List<AggregationContainerWithScore> containerWithScores = new ArrayList<>();
		for (AggregationContainer compatibleAggregation : compatibleAggregations) {
			AggregationQuery aggregationQuery = AggregationQuery.create(dimensions, storedMeasures, where);
			double score = compatibleAggregation.aggregation.estimateCost(aggregationQuery);
			containerWithScores.add(new AggregationContainerWithScore(compatibleAggregation, score));
		}
		sort(containerWithScores);

		Class<K> resultKeyClass = createKeyClass(
				keysToMap(dimensions.stream(), dimensionTypes::get),
				queryClassLoader);

		StreamReducer<K, T, A> streamReducer = StreamReducer.create(Comparable::compareTo);
		StreamSupplier<T> queryResultSupplier = streamReducer.getOutput();

		storedMeasures = new ArrayList<>(storedMeasures);
		for (AggregationContainerWithScore aggregationContainerWithScore : containerWithScores) {
			AggregationContainer aggregationContainer = aggregationContainerWithScore.aggregationContainer;
			List<String> compatibleMeasures = storedMeasures.stream().filter(aggregationContainer.measures::contains).collect(toList());
			if (compatibleMeasures.isEmpty())
				continue;
			storedMeasures.removeAll(compatibleMeasures);

			Class<S> aggregationClass = createRecordClass(
					keysToMap(dimensions.stream(), dimensionTypes::get),
					keysToMap(compatibleMeasures.stream(), m -> measures.get(m).getFieldType()),
					queryClassLoader);

			StreamSupplier<S> aggregationSupplier = aggregationContainer.aggregation.query(
					AggregationQuery.create(dimensions, compatibleMeasures, where),
					aggregationClass, queryClassLoader);

			if (storedMeasures.isEmpty() && streamReducer.getInputs().isEmpty()) {
				/*
				If query is fulfilled from the single aggregation,
				just use mapper instead of reducer to copy requested fields.
				 */
				Function<S, T> mapper = createMapper(aggregationClass, resultClass, dimensions,
						compatibleMeasures, queryClassLoader);
				queryResultSupplier = aggregationSupplier
						.transformWith(StreamMapper.create(mapper));
				break;
			}

			Function<S, K> keyFunction = io.activej.aggregation.util.Utils.createKeyFunction(aggregationClass, resultKeyClass, dimensions, queryClassLoader);

			Reducer<K, S, T, A> reducer = aggregationContainer.aggregation.aggregationReducer(aggregationClass, resultClass,
					dimensions, compatibleMeasures, queryClassLoader);

			StreamConsumer<S> streamReducerInput = streamReducer.newInput(keyFunction, reducer);

			aggregationSupplier.streamTo(streamReducerInput);
		}

		return queryResultSupplier;
	}

	List<AggregationContainer> getCompatibleAggregationsForQuery(Collection<String> dimensions,
			Collection<String> storedMeasures,
			AggregationPredicate where) {
		where = where.simplify();
		List<String> allDimensions = Stream.concat(dimensions.stream(), where.getDimensions().stream()).collect(toList());

		List<AggregationContainer> compatibleAggregations = new ArrayList<>();
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			List<String> keys = aggregationContainer.aggregation.getKeys();
			if (!keys.containsAll(allDimensions)) continue;

			List<String> compatibleMeasures = storedMeasures.stream().filter(aggregationContainer.measures::contains).collect(toList());
			if (compatibleMeasures.isEmpty()) continue;
			AggregationPredicate intersection = AggregationPredicates.and(where, aggregationContainer.predicate).simplify();

			if (!intersection.equals(where)) continue;
			compatibleAggregations.add(aggregationContainer);
		}
		return compatibleAggregations;
	}

	static class AggregationContainerWithScore implements Comparable<AggregationContainerWithScore> {
		final AggregationContainer aggregationContainer;
		final double score;

		private AggregationContainerWithScore(AggregationContainer aggregationContainer, double score) {
			this.score = score;
			this.aggregationContainer = aggregationContainer;
		}

		@Override
		public int compareTo(@NotNull AggregationContainerWithScore o) {
			int result;
			result = -Integer.compare(aggregationContainer.measures.size(), o.aggregationContainer.measures.size());
			if (result != 0) return result;
			result = Double.compare(score, o.score);
			if (result != 0) return result;
			result = Integer.compare(aggregationContainer.aggregation.getChunks(), o.aggregationContainer.aggregation.getChunks());
			if (result != 0) return result;
			result = Integer.compare(aggregationContainer.aggregation.getKeys().size(), o.aggregationContainer.aggregation.getKeys().size());
			return result;
		}
	}

	public boolean containsExcessiveNumberOfOverlappingChunks() {
		boolean excessive = false;

		for (AggregationContainer aggregationContainer : aggregations.values()) {
			int numberOfOverlappingChunks = aggregationContainer.aggregation.getNumberOfOverlappingChunks();
			if (numberOfOverlappingChunks > maxOverlappingChunksToProcessLogs) {
				logger.info("Aggregation {} contains {} overlapping chunks", aggregationContainer.aggregation, numberOfOverlappingChunks);
				excessive = true;
			}
		}

		return excessive;
	}

	public Promise<CubeDiff> consolidate(Function<Aggregation, Promise<AggregationDiff>> strategy) {
		logger.info("Launching consolidation");

		Map<String, AggregationDiff> map = new HashMap<>();
		List<AsyncSupplier<Void>> runnables = new ArrayList<>();

		for (Entry<String, AggregationContainer> entry : aggregations.entrySet()) {
			String aggregationId = entry.getKey();
			Aggregation aggregation = entry.getValue().aggregation;

			runnables.add(() -> strategy.apply(aggregation)
					.whenResult(aggregationDiff -> {
						if (!aggregationDiff.isEmpty()) {
							map.put(aggregationId, aggregationDiff);
						}
					})
					.toVoid());
		}

		return Promises.sequence(runnables).map($ -> CubeDiff.of(map));
	}

	private List<String> getAllParents(String dimension) {
		ArrayList<String> chain = new ArrayList<>();
		chain.add(dimension);
		String child = dimension;
		String parent;
		while ((parent = childParentRelations.get(child)) != null) {
			chain.add(0, parent);
			child = parent;
		}
		return chain;
	}

	public Set<Object> getAllChunks() {
		Set<Object> chunks = new HashSet<>();
		for (AggregationContainer container : aggregations.values()) {
			chunks.addAll(container.aggregation.getState().getChunks().keySet());
		}
		return chunks;
	}

	public Map<String, List<ConsolidationDebugInfo>> getConsolidationDebugInfo() {
		Map<String, List<ConsolidationDebugInfo>> m = new HashMap<>();
		for (Entry<String, AggregationContainer> aggregationEntry : aggregations.entrySet()) {
			m.put(aggregationEntry.getKey(), aggregationEntry.getValue().aggregation.getState().getConsolidationDebugInfo());
		}
		return m;
	}

	public DefiningClassLoader getClassLoader() {
		return classLoader;
	}

	// region temp query() method
	@Override
	public Promise<QueryResult> query(CubeQuery cubeQuery) throws QueryException {
		DefiningClassLoader queryClassLoader = getQueryClassLoader(new CubeClassLoaderCache.Key(
				new LinkedHashSet<>(cubeQuery.getAttributes()),
				new LinkedHashSet<>(cubeQuery.getMeasures()),
				cubeQuery.getWhere().getDimensions()));
		long queryStarted = eventloop.currentTimeMillis();
		return new RequestContext<>().execute(queryClassLoader, cubeQuery)
				.whenComplete((queryResult, e) -> {
					if (e == null) {
						queryTimes.recordValue((int) (eventloop.currentTimeMillis() - queryStarted));
					} else {
						queryErrors++;
						queryLastError = e;

						if (e instanceof NoSuchFileException) {
							logger.warn("Query failed because of NoSuchFileException. " + cubeQuery.toString(), e);
						}
					}
				});
	}
	// endregion

	private DefiningClassLoader getQueryClassLoader(CubeClassLoaderCache.Key key) {
		if (classLoaderCache == null)
			return classLoader;
		return classLoaderCache.getOrCreate(key);
	}

	private class RequestContext<R> {
		DefiningClassLoader queryClassLoader;
		CubeQuery query;

		AggregationPredicate queryPredicate;
		AggregationPredicate queryHaving;

		List<AggregationContainer> compatibleAggregations = new ArrayList<>();
		Map<String, Object> fullySpecifiedDimensions;

		final Set<String> resultDimensions = new LinkedHashSet<>();
		final Set<String> resultAttributes = new LinkedHashSet<>();

		final Set<String> resultMeasures = new LinkedHashSet<>();
		final Set<String> resultStoredMeasures = new LinkedHashSet<>();
		final Set<String> resultComputedMeasures = new LinkedHashSet<>();

		Class<R> resultClass;
		Predicate<R> havingPredicate;
		final List<String> resultOrderings = new ArrayList<>();
		Comparator<R> comparator;
		MeasuresFunction<R> measuresFunction;
		TotalsFunction<R, R> totalsFunction;

		final List<String> recordAttributes = new ArrayList<>();
		final List<String> recordMeasures = new ArrayList<>();
		RecordScheme recordScheme;
		RecordFunction recordFunction;

		Promise<QueryResult> execute(DefiningClassLoader queryClassLoader, CubeQuery query) throws QueryException {
			this.queryClassLoader = queryClassLoader;
			this.query = query;

			queryPredicate = query.getWhere().simplify();
			queryHaving = query.getHaving().simplify();
			fullySpecifiedDimensions = queryPredicate.getFullySpecifiedDimensions();

			prepareDimensions();
			prepareMeasures();

			resultClass = createResultClass(resultAttributes, resultMeasures, Cube.this, queryClassLoader);
			recordScheme = createRecordScheme();
			if (query.getReportType() == ReportType.METADATA) {
				return Promise.of(QueryResult.createForMetadata(recordScheme, recordAttributes, recordMeasures));
			}
			measuresFunction = createMeasuresFunction();
			totalsFunction = createTotalsFunction();
			comparator = createComparator();
			havingPredicate = createHavingPredicate();
			recordFunction = createRecordFunction();

			return queryRawStream(new ArrayList<>(resultDimensions), new ArrayList<>(resultStoredMeasures),
					queryPredicate, resultClass, queryClassLoader, compatibleAggregations)
					.toList()
					.then(this::processResults);
		}

		void prepareDimensions() throws QueryException {
			for (String attribute : query.getAttributes()) {
				recordAttributes.add(attribute);
				List<String> dimensions = new ArrayList<>();
				if (dimensionTypes.containsKey(attribute)) {
					dimensions = getAllParents(attribute);
				} else if (attributes.containsKey(attribute)) {
					AttributeResolverContainer resolverContainer = attributes.get(attribute);
					for (String dimension : resolverContainer.dimensions) {
						dimensions.addAll(getAllParents(dimension));
					}
				} else {
					throw new QueryException("Attribute not found: " + attribute);
				}
				resultDimensions.addAll(dimensions);
				resultAttributes.addAll(dimensions);
				resultAttributes.add(attribute);
			}
		}

		void prepareMeasures() {
			Set<String> queryStoredMeasures = new HashSet<>();
			for (String measure : query.getMeasures()) {
				if (computedMeasures.containsKey(measure)) {
					queryStoredMeasures.addAll(computedMeasures.get(measure).getMeasureDependencies());
				} else if (measures.containsKey(measure)) {
					queryStoredMeasures.add(measure);
				}
			}
			compatibleAggregations = getCompatibleAggregationsForQuery(resultDimensions, queryStoredMeasures, queryPredicate);

			Set<String> compatibleMeasures = new LinkedHashSet<>();
			for (AggregationContainer aggregationContainer : compatibleAggregations) {
				compatibleMeasures.addAll(aggregationContainer.measures);
			}
			for (Entry<String, ComputedMeasure> entry : computedMeasures.entrySet()) {
				if (compatibleMeasures.containsAll(entry.getValue().getMeasureDependencies())) {
					compatibleMeasures.add(entry.getKey());
				}
			}

			for (String queryMeasure : query.getMeasures()) {
				if (!compatibleMeasures.contains(queryMeasure) || recordMeasures.contains(queryMeasure))
					continue;
				recordMeasures.add(queryMeasure);
				if (measures.containsKey(queryMeasure)) {
					resultStoredMeasures.add(queryMeasure);
					resultMeasures.add(queryMeasure);
				} else if (computedMeasures.containsKey(queryMeasure)) {
					ComputedMeasure expression = computedMeasures.get(queryMeasure);
					Set<String> dependencies = expression.getMeasureDependencies();
					resultStoredMeasures.addAll(dependencies);
					resultComputedMeasures.add(queryMeasure);
					resultMeasures.addAll(dependencies);
					resultMeasures.add(queryMeasure);
				}
			}
		}

		RecordScheme createRecordScheme() {
			RecordScheme recordScheme = RecordScheme.create();
			for (String attribute : recordAttributes) {
				recordScheme.addField(attribute, getAttributeType(attribute));
			}
			for (String measure : recordMeasures) {
				recordScheme.addField(measure, getMeasureType(measure));
			}
			return recordScheme;
		}

		RecordFunction createRecordFunction() {
			return ClassBuilder.create(queryClassLoader, RecordFunction.class)
					.withClassKey(resultClass, recordScheme.getFields())
					.withMethod("copyAttributes",
							sequence(expressions -> {
								for (String field : recordScheme.getFields()) {
									int fieldIndex = recordScheme.getFieldIndex(field);
									if (dimensionTypes.containsKey(field)) {
										expressions.add(call(arg(1), "put", value(fieldIndex),
												cast(dimensionTypes.get(field).toValue(
														property(cast(arg(0), resultClass), field)), Object.class)));
									}
								}
							}))
					.withMethod("copyMeasures",
							sequence(expressions -> {
								for (String field : recordScheme.getFields()) {
									int fieldIndex = recordScheme.getFieldIndex(field);
									if (!dimensionTypes.containsKey(field)) {
										if (measures.containsKey(field)) {
											Variable fieldValue = property(cast(arg(0), resultClass), field);
											expressions.add(call(arg(1), "put", value(fieldIndex),
													cast(measures.get(field).getFieldType().toValue(
															measures.get(field).valueOfAccumulator(fieldValue)), Object.class)));
										} else {
											expressions.add(call(arg(1), "put", value(fieldIndex),
													cast(property(cast(arg(0), resultClass), field.replace('.', '$')), Object.class)));
										}
									}
								}
							}))
					.buildClassAndCreateNewInstance();
		}

		MeasuresFunction<R> createMeasuresFunction() {
			return ClassBuilder.create(queryClassLoader, MeasuresFunction.class)
					.withClassKey(resultClass, resultComputedMeasures)
					.withInitializer(cb ->
							resultComputedMeasures.forEach(computedMeasure ->
									cb.withField(computedMeasure, computedMeasures.get(computedMeasure).getType(measures))))
					.withMethod("computeMeasures", sequence(list -> {
						for (String computedMeasure : resultComputedMeasures) {
							Expression record = cast(arg(0), resultClass);
							list.add(set(property(record, computedMeasure),
									computedMeasures.get(computedMeasure).getExpression(record, measures)));
						}

					}))
					.buildClassAndCreateNewInstance();
		}

		private Predicate<R> createHavingPredicate() {
			if (queryHaving == AggregationPredicates.alwaysTrue()) return o -> true;
			if (queryHaving == AggregationPredicates.alwaysFalse()) return o -> false;

			return ClassBuilder.create(queryClassLoader, Predicate.class)
					.withClassKey(resultClass, queryHaving)
					.withMethod("test",
							queryHaving.createPredicate(cast(arg(0), resultClass), fieldTypes))
					.buildClassAndCreateNewInstance();
		}

		@SuppressWarnings("unchecked")
		Comparator<R> createComparator() {
			if (query.getOrderings().isEmpty())
				return (o1, o2) -> 0;

			return ClassBuilder.create(queryClassLoader, Comparator.class)
					.withClassKey(resultClass, query.getOrderings())
					.withMethod("compare", of(() -> {
						ExpressionComparator comparator = ExpressionComparator.create();

						for (Ordering ordering : query.getOrderings()) {
							String field = ordering.getField();

							if (resultMeasures.contains(field) || resultAttributes.contains(field)) {
								String property = field.replace('.', '$');
								comparator.with(
										ordering.isAsc() ? leftProperty(resultClass, property) : rightProperty(resultClass, property),
										ordering.isAsc() ? rightProperty(resultClass, property) : leftProperty(resultClass, property),
										true);
								resultOrderings.add(field);
							}
						}

						return comparator;
					}))
					.buildClassAndCreateNewInstance();
		}

		Promise<QueryResult> processResults(List<R> results) {
			R totals;
			try {
				totals = resultClass.getDeclaredConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
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

			Record totalRecord = Record.create(recordScheme);
			recordFunction.copyMeasures(totals, totalRecord);

			List<Promise<Void>> tasks = new ArrayList<>();
			Map<String, Object> filterAttributes = new LinkedHashMap<>();
			for (AttributeResolverContainer resolverContainer : attributeResolvers) {
				List<String> attributes = new ArrayList<>(resolverContainer.attributes);
				attributes.retainAll(resultAttributes);
				if (!attributes.isEmpty()) {
					tasks.add(io.activej.cube.Utils.resolveAttributes(results, resolverContainer.resolver,
							resolverContainer.dimensions, attributes,
							fullySpecifiedDimensions, resultClass, queryClassLoader));
				}
			}

			for (AttributeResolverContainer resolverContainer : attributeResolvers) {
				if (fullySpecifiedDimensions.keySet().containsAll(resolverContainer.dimensions)) {
					tasks.add(resolveSpecifiedDimensions(resolverContainer, filterAttributes));
				}
			}
			return Promises.all(tasks)
					.map($ -> processResults2(results, totals, filterAttributes));
		}

		QueryResult processResults2(List<R> results, R totals, Map<String, Object> filterAttributes) {
			results = results.stream().filter(havingPredicate).collect(toList());

			int totalCount = results.size();

			results = applyLimitAndOffset(results);

			List<Record> resultRecords = new ArrayList<>(results.size());
			for (R result : results) {
				Record record = Record.create(recordScheme);
				recordFunction.copyAttributes(result, record);
				recordFunction.copyMeasures(result, record);
				resultRecords.add(record);
			}

			if (query.getReportType() == ReportType.DATA) {
				return QueryResult.createForData(recordScheme,
						resultRecords,
						recordAttributes,
						recordMeasures,
						resultOrderings,
						filterAttributes);
			}

			if (query.getReportType() == ReportType.DATA_WITH_TOTALS) {
				Record totalRecord = Record.create(recordScheme);
				recordFunction.copyMeasures(totals, totalRecord);
				return QueryResult.createForDataWithTotals(recordScheme,
						resultRecords,
						totalRecord,
						totalCount,
						recordAttributes,
						recordMeasures,
						resultOrderings,
						filterAttributes);
			}

			throw new AssertionError();
		}

		private Promise<Void> resolveSpecifiedDimensions(AttributeResolverContainer resolverContainer,
				Map<String, Object> result) {
			Object[] key = new Object[resolverContainer.dimensions.size()];
			for (int i = 0; i < resolverContainer.dimensions.size(); i++) {
				String dimension = resolverContainer.dimensions.get(i);
				key[i] = fullySpecifiedDimensions.get(dimension);
			}

			Ref<Object> attributesRef = new Ref<>();
			return resolverContainer.resolver.resolveAttributes(singletonList(key),
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
			Integer offset = query.getOffset();
			Integer limit = query.getLimit();
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
						.collect(Collectors.toList());
			}

			return results.subList(start, end);
		}

		TotalsFunction<R, R> createTotalsFunction() {
			return ClassBuilder.create(queryClassLoader, TotalsFunction.class)
					.withClassKey(resultClass, resultStoredMeasures, resultComputedMeasures)
					.withMethod("zero",
							sequence(expressions -> {
								for (String field : resultStoredMeasures) {
									Measure measure = measures.get(field);
									expressions.add(measure.zeroAccumulator(
											property(cast(arg(0), resultClass), field)));
								}
							}))
					.withMethod("init",
							sequence(expressions -> {
								for (String field : resultStoredMeasures) {
									Measure measure = measures.get(field);
									expressions.add(measure.initAccumulatorWithAccumulator(
											property(cast(arg(0), resultClass), field),
											property(cast(arg(1), resultClass), field)));
								}
							}))
					.withMethod("accumulate",
							sequence(expressions -> {
								for (String field : resultStoredMeasures) {
									Measure measure = measures.get(field);
									expressions.add(measure.reduce(
											property(cast(arg(0), resultClass), field),
											property(cast(arg(1), resultClass), field)));
								}
							}))
					.withMethod("computeMeasures",
							sequence(expressions -> {
								for (String computedMeasure : resultComputedMeasures) {
									Expression result = cast(arg(0), resultClass);
									expressions.add(set(property(result, computedMeasure),
											computedMeasures.get(computedMeasure).getExpression(result, measures)));
								}
							}))
					.buildClassAndCreateNewInstance();
		}

	}
	// endregion

	@Override
	public String toString() {
		return "Cube{" +
				"aggregations=" + aggregations +
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
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setChunkSize(aggregationsChunkSize);
		}
	}

	public Cube withAggregationsChunkSize(int aggregationsChunkSize) {
		this.aggregationsChunkSize = aggregationsChunkSize;
		return this;
	}

	public Cube withAggregationsReducerBufferSize(int aggregationsReducerBufferSize) {
		this.aggregationsReducerBufferSize = aggregationsReducerBufferSize;
		return this;
	}

	@JmxAttribute
	public int getAggregationsSorterItemsInMemory() {
		return aggregationsSorterItemsInMemory;
	}

	@JmxAttribute
	public void setAggregationsSorterItemsInMemory(int aggregationsSorterItemsInMemory) {
		this.aggregationsSorterItemsInMemory = aggregationsSorterItemsInMemory;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setSorterItemsInMemory(aggregationsSorterItemsInMemory);
		}
	}

	public Cube withAggregationsSorterItemsInMemory(int aggregationsSorterItemsInMemory) {
		this.aggregationsSorterItemsInMemory = aggregationsSorterItemsInMemory;
		return this;
	}

	@JmxAttribute
	public int getAggregationsMaxChunksToConsolidate() {
		return aggregationsMaxChunksToConsolidate;
	}

	@JmxAttribute
	public void setAggregationsMaxChunksToConsolidate(int aggregationsMaxChunksToConsolidate) {
		this.aggregationsMaxChunksToConsolidate = aggregationsMaxChunksToConsolidate;
		for (AggregationContainer aggregationContainer : aggregations.values()) {
			aggregationContainer.aggregation.setMaxChunksToConsolidate(aggregationsMaxChunksToConsolidate);
		}
	}

	public Cube withAggregationsMaxChunksToConsolidate(int aggregationsMaxChunksToConsolidate) {
		this.aggregationsMaxChunksToConsolidate = aggregationsMaxChunksToConsolidate;
		return this;
	}

	@JmxAttribute
	public boolean getAggregationsIgnoreChunkReadingExceptions() {
		return aggregationsIgnoreChunkReadingExceptions;
	}

	@JmxAttribute
	public void setAggregationsIgnoreChunkReadingExceptions(boolean aggregationsIgnoreChunkReadingExceptions) {
		this.aggregationsIgnoreChunkReadingExceptions = aggregationsIgnoreChunkReadingExceptions;
		for (AggregationContainer aggregation : aggregations.values()) {
			aggregation.aggregation.setIgnoreChunkReadingExceptions(aggregationsIgnoreChunkReadingExceptions);
		}
	}

	public Cube withAggregationsIgnoreChunkReadingExceptions(boolean aggregationsIgnoreChunkReadingExceptions) {
		this.aggregationsIgnoreChunkReadingExceptions = aggregationsIgnoreChunkReadingExceptions;
		return this;
	}

	@JmxAttribute
	public int getMaxOverlappingChunksToProcessLogs() {
		return maxOverlappingChunksToProcessLogs;
	}

	@JmxAttribute
	public void setMaxOverlappingChunksToProcessLogs(int maxOverlappingChunksToProcessLogs) {
		this.maxOverlappingChunksToProcessLogs = maxOverlappingChunksToProcessLogs;
	}

	public Cube withMaxOverlappingChunksToProcessLogs(int maxOverlappingChunksToProcessLogs) {
		this.maxOverlappingChunksToProcessLogs = maxOverlappingChunksToProcessLogs;
		return this;
	}

	@JmxAttribute
	public Duration getMaxIncrementalReloadPeriod() {
		return maxIncrementalReloadPeriod;
	}

	@JmxAttribute
	public void setMaxIncrementalReloadPeriod(Duration maxIncrementalReloadPeriod) {
		this.maxIncrementalReloadPeriod = maxIncrementalReloadPeriod;
	}

	public Cube withMaxIncrementalReloadPeriod(Duration maxIncrementalReloadPeriod) {
		this.maxIncrementalReloadPeriod = maxIncrementalReloadPeriod;
		return this;
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
	public Throwable getQueryLastError() {
		return queryLastError;
	}

	@JmxAttribute
	public AggregationStats getAggregationStats() {
		return aggregationStats;
	}

	@NotNull
	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

}
