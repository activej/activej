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

import io.activej.aggregation.QueryPlan.Sequence;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.measure.Measure;
import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.aggregation.predicate.AggregationPredicate;
import io.activej.aggregation.predicate.AggregationPredicates;
import io.activej.aggregation.util.Utils;
import io.activej.codegen.ClassGenerator;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.builder.AbstractBuilder;
import io.activej.csp.process.frame.FrameFormat;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.StreamConsumerWithResult;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.reducer.StreamReducer;
import io.activej.datastream.processor.transformer.StreamTransformers;
import io.activej.datastream.processor.transformer.sort.StreamSorter;
import io.activej.datastream.processor.transformer.sort.StreamSorterStorage;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import io.activej.serializer.BinarySerializer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.activej.aggregation.util.Utils.*;
import static io.activej.codegen.expression.Expressions.arg;
import static io.activej.codegen.expression.Expressions.cast;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Utils.*;
import static io.activej.datastream.processor.transformer.StreamSupplierTransformer.identity;
import static io.activej.reactor.Reactive.checkInReactorThread;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Represents an aggregation, which aggregates data using custom reducer and preaggregator.
 * Provides methods for loading and querying data.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class Aggregation extends AbstractReactive
		implements IAggregation, ReactiveJmxBeanWithStats {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public static final int DEFAULT_CHUNK_SIZE = 1_000_000;
	public static final int DEFAULT_REDUCER_BUFFER_SIZE = StreamReducer.DEFAULT_BUFFER_SIZE;
	public static final int DEFAULT_SORTER_ITEMS_IN_MEMORY = 1_000_000;
	public static final Duration DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD = Duration.ofMinutes(10);
	public static final int DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE = 1000;

	private final Executor executor;
	private final DefiningClassLoader classLoader;
	private final IAggregationChunkStorage<Object> aggregationChunkStorage;
	private final FrameFormat frameFormat;
	private Path temporarySortDir;

	private final AggregationStructure structure;

	private AggregationOTState state;

	// settings
	private int chunkSize = DEFAULT_CHUNK_SIZE;
	private int reducerBufferSize = DEFAULT_REDUCER_BUFFER_SIZE;
	private int sorterItemsInMemory = DEFAULT_SORTER_ITEMS_IN_MEMORY;
	private Duration maxIncrementalReloadPeriod = DEFAULT_MAX_INCREMENTAL_RELOAD_PERIOD;
	private boolean ignoreChunkReadingExceptions = false;
	private int maxChunksToConsolidate = DEFAULT_MAX_CHUNKS_TO_CONSOLIDATE;

	// jmx

	private AggregationStats stats = new AggregationStats();
	private long consolidationStarted;
	private long consolidationLastTimeMillis;
	private int consolidations;
	private Exception consolidationLastError;

	private Aggregation(Reactor reactor, Executor executor, DefiningClassLoader classLoader,
			IAggregationChunkStorage aggregationChunkStorage, FrameFormat frameFormat, AggregationStructure structure,
			AggregationOTState state) {
		super(reactor);
		this.executor = executor;
		this.classLoader = classLoader;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.frameFormat = frameFormat;
		this.structure = structure;
		this.state = state;
	}

	/**
	 * Instantiates an aggregation with the specified structure, that runs in a given reactor,
	 * uses the specified class loader for creating dynamic classes, saves data and metadata to given storages.
	 * Maximum size of chunk is 1,000,000 bytes.
	 * No more than 1,000,000 records stay in memory while sorting.
	 * Maximum duration of consolidation attempt is 30 minutes.
	 * Consolidated chunks become available for removal in 10 minutes from consolidation.
	 *
	 * @param reactor                 reactor, in which the aggregation is to run
	 * @param executor                executor, that is used for asynchronous work with files
	 * @param classLoader             class loader for defining dynamic classes
	 * @param aggregationChunkStorage storage for data chunks
	 * @param frameFormat             frame format in which data is to be stored
	 */
	public static Builder builder(Reactor reactor, Executor executor, DefiningClassLoader classLoader,
			IAggregationChunkStorage aggregationChunkStorage, FrameFormat frameFormat) {
		return new Aggregation(reactor, executor, classLoader, aggregationChunkStorage, frameFormat, null, null).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, Aggregation> {
		private Builder() {}

		public Builder withStructure(AggregationStructure structure) {
			checkNotBuilt(this);
			return new Aggregation(reactor, executor, classLoader, aggregationChunkStorage, frameFormat, structure, new AggregationOTState(structure)).new Builder();
		}

		public Builder withChunkSize(int chunkSize) {
			checkNotBuilt(this);
			Aggregation.this.chunkSize = chunkSize;
			return this;
		}

		public Builder withReducerBufferSize(int reducerBufferSize) {
			checkNotBuilt(this);
			Aggregation.this.reducerBufferSize = reducerBufferSize;
			return this;
		}

		public Builder withSorterItemsInMemory(int sorterItemsInMemory) {
			checkNotBuilt(this);
			Aggregation.this.sorterItemsInMemory = sorterItemsInMemory;
			return this;
		}

		public Builder withMaxIncrementalReloadPeriod(Duration maxIncrementalReloadPeriod) {
			checkNotBuilt(this);
			Aggregation.this.maxIncrementalReloadPeriod = maxIncrementalReloadPeriod;
			return this;
		}

		public Builder withIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions) {
			checkNotBuilt(this);
			Aggregation.this.ignoreChunkReadingExceptions = ignoreChunkReadingExceptions;
			return this;
		}

		public Builder withMaxChunksToConsolidate(int maxChunksToConsolidate) {
			checkNotBuilt(this);
			Aggregation.this.maxChunksToConsolidate = maxChunksToConsolidate;
			return this;
		}

		public Builder withTemporarySortDir(Path temporarySortDir) {
			checkNotBuilt(this);
			Aggregation.this.temporarySortDir = temporarySortDir;
			return this;
		}

		public Builder withStats(AggregationStats stats) {
			checkNotBuilt(this);
			Aggregation.this.stats = stats;
			return this;
		}

		@Override
		protected Aggregation doBuild() {
			checkNotNull(Aggregation.this.structure, "Aggregation structure not set");
			return Aggregation.this;
		}
	}

	public AggregationStructure getStructure() {
		return structure;
	}

	public AggregationOTState getState() {
		return state;
	}

	public void setState(AggregationOTState state) {
		this.state = state;
	}

	public AggregationOTState detachState() {
		AggregationOTState state = this.state;
		//noinspection AssignmentToNull - in the lifecycle of a component field is not nullable
		this.state = null;
		return state;
	}

	public List<String> getKeys() {
		return structure.getKeys();
	}

	public List<String> getMeasures() {
		return structure.getMeasures();
	}

	public Map<String, FieldType> getKeyTypes() {
		return structure.getKeyTypes();
	}

	public Map<String, FieldType> getMeasureTypes() {
		return structure.getMeasureTypes();
	}

	public List<String> getPartitioningKey() {
		return structure.getPartitioningKey();
	}

	public <K extends Comparable, I, O, A> Reducer<K, I, O, A> aggregationReducer(Class<I> inputClass, Class<O> outputClass,
			List<String> keys, List<String> measures,
			DefiningClassLoader classLoader) {
		return Utils.aggregationReducer(structure, inputClass, outputClass,
				keys, measures, Map.of(), classLoader);
	}

	/**
	 * Provides a {@link StreamConsumer} for streaming data to this aggregation.
	 *
	 * @param inputClass class of input records
	 * @param <T>        data records type
	 * @return consumer for streaming data to aggregation
	 */
	@SuppressWarnings("unchecked")
	public <T, C, K extends Comparable> StreamConsumerWithResult<T, AggregationDiff> consume(
			Class<T> inputClass, Map<String, String> keyFields, Map<String, String> measureFields) {
		checkInReactorThread(this);
		checkArgument(new HashSet<>(getKeys()).equals(keyFields.keySet()), "Expected keys: %s, actual keyFields: %s", getKeys(), keyFields);
		checkArgument(getMeasureTypes().keySet().containsAll(measureFields.keySet()), "Unknown measures: %s", difference(measureFields.keySet(),
				getMeasureTypes().keySet()));

		logger.info("Started consuming data in aggregation {}. Keys: {} Measures: {}", this, keyFields.keySet(), measureFields.keySet());

		Class<K> keyClass = createKeyClass(
				keysToMap(getKeys().stream(), structure.getKeyTypes()::get),
				classLoader);
		Set<String> measureFieldKeys = measureFields.keySet();
		List<String> measures = getMeasureTypes().keySet().stream().filter(measureFieldKeys::contains).collect(toList());

		Class<T> recordClass = createRecordClass(structure, getKeys(), measures, classLoader);

		Aggregate<T, Object> aggregate = createPreaggregator(structure, inputClass, recordClass,
				keyFields, measureFields,
				classLoader);

		Function<T, K> keyFunction = createKeyFunction(inputClass, keyClass, getKeys(), classLoader);
		AggregationGroupReducer<C, T, K> groupReducer = new AggregationGroupReducer<>(aggregationChunkStorage,
				structure, measures,
				recordClass,
				createPartitionPredicate(recordClass, getPartitioningKey(), classLoader),
				keyFunction,
				aggregate, chunkSize, classLoader);

		return StreamConsumerWithResult.of(groupReducer,
				groupReducer.getResult()
						.map(chunks -> AggregationDiff.of(new HashSet<>(chunks)))
						.mapException(e -> new AggregationException("Failed to consume data", e)));
	}

	public <T> StreamConsumerWithResult<T, AggregationDiff> consume(Class<T> inputClass) {
		checkInReactorThread(this);
		return consume(inputClass, scanKeyFields(inputClass), scanMeasureFields(inputClass));
	}

	public double estimateCost(AggregationQuery query) {
		List<String> measures = getMeasures();
		List<String> aggregationFields = query.getMeasures().stream().filter(measures::contains).collect(toList());
		return state.findChunks(query.getPredicate(), aggregationFields).size();
	}

	public <T> StreamSupplier<T> query(AggregationQuery query, Class<T> outputClass) {
		checkInReactorThread(this);
		return query(query, outputClass, classLoader);
	}

	/**
	 * Returns a {@link StreamSupplier} of the records retrieved from aggregation for the specified query.
	 *
	 * @param <T>         type of output objects
	 * @param query       query
	 * @param outputClass class of output records
	 * @return supplier that streams query results
	 */
	@Override
	public <T> StreamSupplier<T> query(AggregationQuery query, Class<T> outputClass, DefiningClassLoader queryClassLoader) {
		checkInReactorThread(this);
		checkArgument(iterate(queryClassLoader, Objects::nonNull, ClassLoader::getParent).anyMatch(isEqual(classLoader)),
				"Unrelated queryClassLoader");
		List<String> fields = getMeasures().stream().filter(query.getMeasures()::contains).collect(toList());
		List<AggregationChunk> allChunks = state.findChunks(query.getPredicate(), fields);
		return consolidatedSupplier(query.getKeys(),
				fields, outputClass, query.getPredicate(), allChunks, queryClassLoader)
				.withEndOfStream(eos -> eos
						.mapException(e -> new AggregationException("Query " + query + " failed", e)));
	}

	private <T> StreamSupplier<T> sortStream(StreamSupplier<T> unsortedStream, Class<T> resultClass,
			List<String> allKeys, List<String> measures, DefiningClassLoader classLoader) {
		Comparator<T> keyComparator = createKeyComparator(resultClass, allKeys, classLoader);
		BinarySerializer<T> binarySerializer = createBinarySerializer(structure, resultClass,
				getKeys(), measures, classLoader);
		Path sortDir;
		if (temporarySortDir != null) {
			sortDir = temporarySortDir;
		} else {
			try {
				sortDir = createSortDir();
			} catch (AggregationException e) {
				return StreamSuppliers.closingWithError(e);
			}
		}
		StreamSorter<T, T> sorter = StreamSorter.create(
				StreamSorterStorage.create(executor, binarySerializer, frameFormat, sortDir),
				Function.identity(), keyComparator, false, sorterItemsInMemory);
		sorter.getInput().getAcknowledgement()
				.whenComplete(() -> {
					if (temporarySortDir == null) {
						deleteSortDirSilent(sortDir);
					}
				});
		return unsortedStream
				.transformWith(sorter);
	}

	private Promise<List<AggregationChunk>> doConsolidation(List<AggregationChunk> chunksToConsolidate) {
		Set<String> aggregationFields = new HashSet<>(getMeasures());
		Set<String> chunkFields = chunksToConsolidate.stream()
				.flatMap(chunk -> chunk.getMeasures().stream())
				.filter(aggregationFields::contains)
				.collect(toSet());

		List<String> measures = getMeasures().stream()
				.filter(chunkFields::contains)
				.collect(toList());
		Class<Object> resultClass = createRecordClass(structure, getKeys(), measures, classLoader);

		StreamSupplier<Object> consolidatedSupplier = consolidatedSupplier(getKeys(), measures, resultClass, AggregationPredicates.alwaysTrue(),
				chunksToConsolidate, classLoader);
		AggregationChunker chunker = AggregationChunker.create(
				structure, measures, resultClass,
				createPartitionPredicate(resultClass, getPartitioningKey(), classLoader),
				aggregationChunkStorage, classLoader, chunkSize);
		return consolidatedSupplier.streamTo(chunker)
				.then(chunker::getResult);
	}

	private static void addChunkToPlan(Map<List<String>, TreeMap<PrimaryKey, List<Sequence>>> planIndex,
			AggregationChunk chunk, List<String> queryFields) {
		queryFields = new ArrayList<>(queryFields);
		queryFields.retainAll(chunk.getMeasures());
		checkArgument(!queryFields.isEmpty(), "All of query fields are contained in measures of a chunk");
		TreeMap<PrimaryKey, List<Sequence>> map = planIndex.computeIfAbsent(queryFields, k -> new TreeMap<>());

		Map.Entry<PrimaryKey, List<Sequence>> entry = map.lowerEntry(chunk.getMinPrimaryKey());
		Sequence sequence;
		if (entry == null) {
			sequence = new Sequence(queryFields);
		} else {
			List<Sequence> list = entry.getValue();
			sequence = list.remove(list.size() - 1);
			if (list.isEmpty()) {
				map.remove(entry.getKey());
			}
		}
		sequence.add(chunk);
		List<Sequence> list = map.computeIfAbsent(chunk.getMaxPrimaryKey(), k -> new ArrayList<>());
		list.add(sequence);
	}

	private static QueryPlan createPlan(List<AggregationChunk> chunks, List<String> queryFields) {
		Map<List<String>, TreeMap<PrimaryKey, List<Sequence>>> index = new HashMap<>();
		chunks = new ArrayList<>(chunks);
		chunks.sort(comparing(AggregationChunk::getMinPrimaryKey));
		for (AggregationChunk chunk : chunks) {
			addChunkToPlan(index, chunk, queryFields);
		}
		List<Sequence> sequences = new ArrayList<>();
		for (TreeMap<PrimaryKey, List<Sequence>> map : index.values()) {
			for (List<Sequence> list : map.values()) {
				sequences.addAll(list);
			}
		}
		return new QueryPlan(sequences);
	}

	private <R, S> StreamSupplier<R> consolidatedSupplier(List<String> queryKeys,
			List<String> measures, Class<R> resultClass,
			AggregationPredicate where,
			List<AggregationChunk> individualChunks,
			DefiningClassLoader queryClassLoader) {
		QueryPlan plan = createPlan(individualChunks, measures);

		logger.info("Query plan for {} in aggregation {}: {}", queryKeys, this, plan);

		boolean alreadySorted = getKeys().subList(0, min(getKeys().size(), queryKeys.size())).equals(queryKeys);

		List<SequenceStream<S>> sequenceStreams = new ArrayList<>();

		for (Sequence sequence : plan.getSequences()) {
			Class<S> sequenceClass = createRecordClass(structure,
					getKeys(),
					sequence.getChunksFields(),
					classLoader);

			StreamSupplier<S> stream = sequenceStream(where, sequence.getChunks(), sequenceClass, queryClassLoader);
			if (!alreadySorted) {
				stream = sortStream(stream, sequenceClass, queryKeys, sequence.getQueryFields(), classLoader);
			}

			sequenceStreams.add(new SequenceStream(stream, sequence.getQueryFields(), sequenceClass));
		}

		return mergeSequences(queryKeys, measures, resultClass, sequenceStreams, queryClassLoader);
	}

	public static final class SequenceStream<S> {
		final StreamSupplier<S> stream;
		final List<String> fields;
		final Class<S> type;

		private SequenceStream(StreamSupplier<S> stream, List<String> fields, Class<S> type) {
			this.stream = stream;
			this.fields = fields;
			this.type = type;
		}
	}

	private <S, R, K extends Comparable> StreamSupplier<R> mergeSequences(List<String> queryKeys, List<String> measures,
			Class<R> resultClass, List<SequenceStream<S>> sequences,
			DefiningClassLoader classLoader) {
		if (sequences.size() == 1 && new HashSet<>(queryKeys).equals(new HashSet<>(getKeys()))) {
			/*
			If there is only one sequential supplier and all aggregation keys are requested, then there is no need for
			using StreamReducer, because all records have unique keys and all we need to do is copy requested measures
			from record class to result class.
			 */
			SequenceStream<S> sequence = sequences.get(0);
			Function<S, R> mapper = createMapper(sequence.type, resultClass,
					queryKeys, measures.stream().filter(sequence.fields::contains).collect(toList()),
					classLoader);
			return sequence.stream
					.transformWith(StreamTransformers.mapper(mapper))
					.transformWith((StreamStats<R>) stats.mergeMapOutput);
		}

		StreamReducer<K, R, Object>.Builder streamReducerBuilder = StreamReducer.builder();
		if (reducerBufferSize != 0 && reducerBufferSize != DEFAULT_REDUCER_BUFFER_SIZE) {
			streamReducerBuilder.withBufferSize(reducerBufferSize);
		}
		StreamReducer<K, R, Object> streamReducer = streamReducerBuilder.build();

		Class<K> keyClass = createKeyClass(
				keysToMap(queryKeys.stream(), structure.getKeyTypes()::get),
				this.classLoader);

		for (SequenceStream<S> sequence : sequences) {
			Function<S, K> extractKeyFunction = createKeyFunction(sequence.type, keyClass, queryKeys, this.classLoader);

			List<String> fields = new ArrayList<>();
			Map<String, Measure> extraFields = new LinkedHashMap<>();
			for (String measure : measures) {
				if (sequence.fields.contains(measure)) {
					fields.add(measure);
				} else {
					extraFields.put(measure, structure.getMeasure(measure));
				}
			}
			Reducer<K, S, R, Object> reducer = Utils.aggregationReducer(structure,
					sequence.type, resultClass,
					queryKeys, fields,
					extraFields,
					classLoader);

			sequence.stream.streamTo(
					streamReducer.newInput(extractKeyFunction, reducer)
							.transformWith((StreamStats<S>) stats.mergeReducerInput));
		}

		return streamReducer.getOutput()
				.transformWith((StreamStats<R>) stats.mergeReducerOutput);
	}

	private <T> StreamSupplier<T> sequenceStream(AggregationPredicate where,
			List<AggregationChunk> individualChunks, Class<T> sequenceClass,
			DefiningClassLoader queryClassLoader) {
		Iterator<AggregationChunk> chunkIterator = individualChunks.iterator();
		return StreamSuppliers.concat(new Iterator<>() {
			@Override
			public boolean hasNext() {
				return chunkIterator.hasNext();
			}

			@Override
			public StreamSupplier<T> next() {
				AggregationChunk chunk = chunkIterator.next();
				return chunkReaderWithFilter(where, chunk, sequenceClass, queryClassLoader);
			}
		});
	}

	private <T> StreamSupplier<T> chunkReaderWithFilter(AggregationPredicate where, AggregationChunk chunk,
			Class<T> chunkRecordClass, DefiningClassLoader queryClassLoader) {
		return StreamSuppliers.ofPromise(
						aggregationChunkStorage.read(structure, chunk.getMeasures(), chunkRecordClass, chunk.getChunkId(), classLoader))
				.transformWith(where != AggregationPredicates.alwaysTrue() ?
						StreamTransformers.filter(
								createPredicate(chunkRecordClass, where, queryClassLoader)) :
						identity());
	}

	private <T> Predicate<T> createPredicate(Class<T> chunkRecordClass,
			AggregationPredicate where, DefiningClassLoader classLoader) {
		return classLoader.ensureClassAndCreateInstance(
				ClassKey.of(Predicate.class, chunkRecordClass, where),
				() -> ClassGenerator.builder(Predicate.class)
						.withMethod("test", boolean.class, List.of(Object.class),
								where.createPredicate(cast(arg(0), chunkRecordClass), getKeyTypes()))
						.build()
		);
	}

	@JmxAttribute
	public int getNumberOfOverlappingChunks() {
		return state.findOverlappingChunks().size();
	}

	public Promise<AggregationDiff> consolidateMinKey() {
		checkInReactorThread(this);
		return consolidateMinKey(Set.of());
	}

	public Promise<AggregationDiff> consolidateMinKey(Set<Object> lockedChunkIds) {
		checkInReactorThread(this);
		return consolidate(getChunksForConsolidation(lockedChunkIds, false));
	}

	public Promise<AggregationDiff> consolidateHotSegment() {
		checkInReactorThread(this);
		return consolidateHotSegment(Set.of());
	}

	public Promise<AggregationDiff> consolidateHotSegment(Set<Object> lockedChunkIds) {
		checkInReactorThread(this);
		return consolidate(getChunksForConsolidation(lockedChunkIds, true));
	}

	public Promise<AggregationDiff> consolidate(List<AggregationChunk> chunks) {
		checkInReactorThread(this);
		checkArgument(state.getChunks().values().containsAll(chunks), "Consolidating unknown chunks");

		consolidationStarted = reactor.currentTimeMillis();
		logger.info("Starting consolidation of aggregation '{}'", this);

		return doConsolidation(chunks)
				.map(newChunks -> AggregationDiff.of(new LinkedHashSet<>(newChunks), new LinkedHashSet<>(chunks)))
				.whenResult(() -> {
					consolidationLastTimeMillis = reactor.currentTimeMillis() - consolidationStarted;
					consolidations++;
				})
				.whenException(e -> {
					consolidationStarted = 0;
					consolidationLastError = e;
				});
	}

	public List<AggregationChunk> getChunksForConsolidation(Set<Object> lockedChunkIds, boolean hotSegment) {
		return hotSegment ?
				state.findChunksForConsolidationHotSegment(maxChunksToConsolidate, lockedChunkIds) :
				state.findChunksForConsolidationMinKey(maxChunksToConsolidate, chunkSize, lockedChunkIds);
	}

	private Path createSortDir() throws AggregationException {
		try {
			return Files.createTempDirectory("aggregation_sort_dir");
		} catch (IOException e) {
			throw new AggregationException("Could not create sort dir", e);
		}
	}

	private void deleteSortDirSilent(Path sortDir) {
		try {
			Files.delete(sortDir);
		} catch (IOException e) {
			logger.warn("Could not delete temporal directory {} : {}", temporarySortDir, e);
		}
	}

	public static String getChunkIds(Iterable<AggregationChunk> chunks) {
		List<Object> ids = new ArrayList<>();
		for (AggregationChunk chunk : chunks) {
			ids.add(chunk.getChunkId());
		}
		return ids.stream().map(Object::toString).collect(Collectors.joining(", "));
	}

	// jmx

	@JmxAttribute
	public Duration getMaxIncrementalReloadPeriod() {
		return maxIncrementalReloadPeriod;
	}

	@JmxAttribute
	public void setMaxIncrementalReloadPeriod(Duration maxIncrementalReloadPeriod) {
		this.maxIncrementalReloadPeriod = maxIncrementalReloadPeriod;
	}

	@JmxAttribute
	public int getChunkSize() {
		return chunkSize;
	}

	@JmxAttribute
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	@JmxAttribute
	public int getSorterItemsInMemory() {
		return sorterItemsInMemory;
	}

	@JmxAttribute
	public void setSorterItemsInMemory(int sorterItemsInMemory) {
		this.sorterItemsInMemory = sorterItemsInMemory;
	}

	@JmxAttribute
	public boolean isIgnoreChunkReadingExceptions() {
		return ignoreChunkReadingExceptions;
	}

	@JmxAttribute
	public void setIgnoreChunkReadingExceptions(boolean ignoreChunkReadingExceptions) {
		this.ignoreChunkReadingExceptions = ignoreChunkReadingExceptions;
	}

	@JmxAttribute
	public int getMaxChunksToConsolidate() {
		return maxChunksToConsolidate;
	}

	@JmxAttribute
	public void setMaxChunksToConsolidate(int maxChunksToConsolidate) {
		this.maxChunksToConsolidate = maxChunksToConsolidate;
	}

	@JmxAttribute
	public @Nullable Integer getConsolidationSeconds() {
		return consolidationStarted == 0 ? null : (int) ((reactor.currentTimeMillis() - consolidationStarted) / 1000);
	}

	@JmxAttribute
	public @Nullable Integer getConsolidationLastTimeSeconds() {
		return consolidationLastTimeMillis == 0 ? null : (int) (consolidationLastTimeMillis / 1000);
	}

	@JmxAttribute
	public int getConsolidations() {
		return consolidations;
	}

	@JmxAttribute
	public Exception getConsolidationLastError() {
		return consolidationLastError;
	}

	@JmxAttribute
	public int getChunks() {
		return state.getChunks().size();
	}

	@JmxAttribute
	public AggregationStats getStats() {
		return stats;
	}

	@Override
	public String toString() {
		return "{" + getKeyTypes().keySet() + " " + getMeasures() + '}';
	}
}
