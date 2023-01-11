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

package io.activej.crdt.storage.local;

import io.activej.async.service.ReactiveService;
import io.activej.common.ApplicationSettings;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtException;
import io.activej.crdt.CrdtTombstone;
import io.activej.crdt.function.CrdtFilter;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.AsyncCrdtStorage;
import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.stats.StreamStats_Basic;
import io.activej.datastream.stats.StreamStats_Detailed;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.stats.EventStats;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import io.activej.reactor.jmx.ReactiveJmxBeanWithStats;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static io.activej.common.Utils.nullify;
import static io.activej.crdt.util.Utils.onItem;

@SuppressWarnings("rawtypes")
public final class CrdtStorage_Map<K extends Comparable<K>, S> extends AbstractReactive
		implements AsyncCrdtStorage<K, S>, WithInitializer<CrdtStorage_Map<K, S>>, ReactiveService, ReactiveJmxBeanWithStats {
	public static final Duration DEFAULT_SMOOTHING_WINDOW = ApplicationSettings.getDuration(CrdtStorage_Map.class, "smoothingWindow", Duration.ofMinutes(1));

	private final CrdtFunction<S> function;

	private final NavigableMap<K, CrdtData<K, S>> map = new TreeMap<>();
	private final NavigableMap<K, CrdtTombstone<K>> tombstones = new TreeMap<>();

	private NavigableMap<K, CrdtData<K, S>> takenMap;
	private NavigableMap<K, CrdtTombstone<K>> takenTombstones;

	private CrdtFilter<S> filter = $ -> true;

	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region JMX
	private boolean detailedStats;

	private final StreamStats_Basic<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final StreamStats_Detailed<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final StreamStats_Basic<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final StreamStats_Detailed<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final StreamStats_Basic<CrdtData<K, S>> takeStats = StreamStats.basic();
	private final StreamStats_Detailed<CrdtData<K, S>> takeStatsDetailed = StreamStats.detailed();
	private final StreamStats_Basic<CrdtTombstone<K>> removeStats = StreamStats.basic();
	private final StreamStats_Detailed<CrdtTombstone<K>> removeStatsDetailed = StreamStats.detailed();

	private final EventStats uploadedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats downloadedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats takenItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats removedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final EventStats singlePuts = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats singleGets = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats singleRemoves = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	private CrdtStorage_Map(Reactor reactor, CrdtFunction<S> function) {
		super(reactor);
		this.function = function;
	}

	public static <K extends Comparable<K>, S> CrdtStorage_Map<K, S> create(Reactor reactor, CrdtFunction<S> crdtFunction) {
		return new CrdtStorage_Map<>(reactor, crdtFunction);
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> CrdtStorage_Map<K, S> create(Reactor reactor) {
		return new CrdtStorage_Map<>(reactor, CrdtFunction.<S>ofCrdtType());
	}

	public CrdtStorage_Map<K, S> withFilter(CrdtFilter<S> filter) {
		this.filter = filter;
		return this;
	}

	public CrdtStorage_Map<K, S> withCurrentTimeProvide(CurrentTimeProvider now) {
		this.now = now;
		return this;
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		checkInReactorThread();
		StreamConsumerToList<CrdtData<K, S>> consumer = StreamConsumerToList.create();
		return Promise.of(consumer.withAcknowledgement(ack -> ack
						.whenResult(() -> consumer.getList().forEach(this::doPut))
						.mapException(e -> new CrdtException("Error while uploading CRDT data", e)))
				.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
				.transformWith(onItem(uploadedItems::recordEvent)));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		checkInReactorThread();
		return Promise.of(StreamSupplier.ofStream(extract(timestamp))
				.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
				.transformWith(onItem(downloadedItems::recordEvent))
				.withEndOfStream(eos -> eos
						.mapException(e -> new CrdtException("Error while downloading CRDT data", e))));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> take() {
		checkInReactorThread();
		if (takenMap != null) {
			assert takenTombstones != null;
			return Promise.ofException(new CrdtException("Data is already being taken"));
		}
		takenMap = new TreeMap<>(map);
		takenTombstones = new TreeMap<>(tombstones);
		map.clear();
		tombstones.clear();

		StreamSupplier<CrdtData<K, S>> supplier = StreamSupplier.ofIterable(takenMap.values())
				.transformWith(detailedStats ? takeStatsDetailed : takeStats)
				.transformWith(onItem(takenItems::recordEvent));
		supplier.getAcknowledgement()
				.whenResult(() -> {
					takenMap = null;
					takenTombstones = null;
				})
				.mapException(e -> {
					takenMap = nullify(takenMap, map -> map.values().forEach(this::doPut));
					takenTombstones = nullify(takenTombstones, map -> map.values().forEach(this::doRemove));

					return new CrdtException("Error while downloading CRDT data", e);
				});
		return Promise.of(supplier);
	}

	@Override
	public Promise<StreamConsumer<CrdtTombstone<K>>> remove() {
		checkInReactorThread();
		StreamConsumerToList<CrdtTombstone<K>> consumer = StreamConsumerToList.create();
		return Promise.of(consumer.withAcknowledgement(ack -> ack
						.whenResult(() -> consumer.getList().forEach(this::doRemove))
						.mapException(e -> new CrdtException("Error while removing CRDT data", e)))
				.transformWith(detailedStats ? removeStatsDetailed : removeStats)
				.transformWith(onItem(removedItems::recordEvent)));
	}

	@Override
	public Promise<Void> ping() {
		checkInReactorThread();
		return Promise.complete();
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread();
		return Promise.complete();
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread();
		return Promise.complete();
	}

	private Stream<CrdtData<K, S>> extract(long timestamp) {
		Map<K, CrdtData<K, S>> map;
		if (takenMap == null) {
			map = this.map;
		} else {
			map = new TreeMap<>();
			doMerge(map, this.map);
			doMerge(map, this.takenMap);
		}

		return map.values().stream()
				.filter(data -> data.getTimestamp() >= timestamp);
	}

	private void doMerge(Map<K, CrdtData<K, S>> to, Map<K, CrdtData<K, S>> from) {
		assert takenTombstones != null;

		for (Map.Entry<K, CrdtData<K, S>> entry : from.entrySet()) {
			K key = entry.getKey();
			CrdtData<K, S> data = entry.getValue();

			CrdtTombstone<K> tombstone = tombstones.get(key);
			if (tombstone != null && tombstone.getTimestamp() >= data.getTimestamp()) {
				continue;
			}

			CrdtTombstone<K> takenTombstone = takenTombstones.get(key);
			if (takenTombstone != null && takenTombstone.getTimestamp() >= data.getTimestamp()) {
				continue;
			}

			to.merge(key, data, (data1, data2) -> {
				if (data1.getTimestamp() > data2.getTimestamp()) return data1;
				return data2;
			});
		}
	}

	private void doPut(CrdtData<K, S> data) {
		K key = data.getKey();

		CrdtTombstone<K> tombstone = tombstones.get(key);
		if (tombstone != null) {
			if (tombstone.getTimestamp() >= data.getTimestamp()) return;
			tombstones.remove(key);
		}

		map.merge(key, data, (a, b) -> {
			S merged = function.merge(a.getState(), a.getTimestamp(), b.getState(), b.getTimestamp());
			long timestamp = Math.max(a.getTimestamp(), b.getTimestamp());
			return filter.test(merged) ? new CrdtData<>(key, timestamp, merged) : null;
		});
	}

	private boolean doRemove(CrdtTombstone<K> tombstone) {
		K key = tombstone.getKey();

		CrdtData<K, S> data = map.get(key);
		boolean removed = data != null;
		if (removed) {
			if (data.getTimestamp() > tombstone.getTimestamp()) return false;
			map.remove(key);
		}

		tombstones.merge(key, tombstone, (a, b) -> new CrdtTombstone<>(key, Math.max(a.getTimestamp(), b.getTimestamp())));
		return true;
	}

	public void put(K key, S state) {
		checkInReactorThread();
		put(new CrdtData<>(key, now.currentTimeMillis(), state));
	}

	public void put(CrdtData<K, S> data) {
		checkInReactorThread();
		singlePuts.recordEvent();
		doPut(data);
	}

	public @Nullable S get(K key) {
		checkInReactorThread();
		singleGets.recordEvent();
		CrdtData<K, S> data = map.get(key);
		return data != null ? data.getState() : null;
	}

	public boolean remove(K key) {
		checkInReactorThread();
		return remove(new CrdtTombstone<>(key, now.currentTimeMillis()));
	}

	public boolean remove(CrdtTombstone<K> tombstone) {
		checkInReactorThread();
		singleRemoves.recordEvent();
		return doRemove(tombstone);
	}

	public Iterator<CrdtData<K, S>> iterator(long timestamp) {
		Iterator<CrdtData<K, S>> iterator = extract(timestamp).iterator();

		// had to hook the remove, so it would be reflected in the storage
		return new Iterator<>() {
			private CrdtData<K, S> current;

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public CrdtData<K, S> next() {
				return current = iterator.next();
			}

			@Override
			public void remove() {
				if (current != null) {
					CrdtStorage_Map.this.remove(current.getKey());
				}
				iterator.remove();
			}
		};
	}

	public Iterator<CrdtData<K, S>> iterator() {
		return iterator(0);
	}

	// region JMX
	@JmxOperation
	public void startDetailedMonitoring() {
		detailedStats = true;
	}

	@JmxOperation
	public void stopDetailedMonitoring() {
		detailedStats = false;
	}

	@JmxAttribute
	public boolean isDetailedStats() {
		return detailedStats;
	}

	@JmxAttribute
	public StreamStats_Basic getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public StreamStats_Detailed getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public StreamStats_Basic getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public StreamStats_Detailed getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public StreamStats_Basic getTakeStats() {
		return takeStats;
	}

	@JmxAttribute
	public StreamStats_Detailed getTakeStatsDetailed() {
		return takeStatsDetailed;
	}

	@JmxAttribute
	public StreamStats_Basic getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public StreamStats_Detailed getRemoveStatsDetailed() {
		return removeStatsDetailed;
	}

	@JmxAttribute
	public EventStats getSinglePuts() {
		return singlePuts;
	}

	@JmxAttribute
	public EventStats getSingleGets() {
		return singleGets;
	}

	@JmxAttribute
	public EventStats getSingleRemoves() {
		return singleRemoves;
	}

	@JmxAttribute
	public EventStats getUploadedItems() {
		return uploadedItems;
	}

	@JmxAttribute
	public EventStats getDownloadedItems() {
		return downloadedItems;
	}

	@JmxAttribute
	public EventStats getTakenItems() {
		return takenItems;
	}

	@JmxAttribute
	public EventStats getRemovedItems() {
		return removedItems;
	}
	// endregion
}
