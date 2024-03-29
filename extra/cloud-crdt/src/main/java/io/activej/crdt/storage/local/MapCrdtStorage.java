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
import io.activej.common.Checks;
import io.activej.common.builder.AbstractBuilder;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtException;
import io.activej.crdt.CrdtTombstone;
import io.activej.crdt.function.CrdtFilter;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.primitives.CrdtType;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.datastream.consumer.StreamConsumer;
import io.activej.datastream.consumer.ToListStreamConsumer;
import io.activej.datastream.stats.BasicStreamStats;
import io.activej.datastream.stats.DetailedStreamStats;
import io.activej.datastream.stats.StreamStats;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.datastream.supplier.StreamSuppliers;
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
import static io.activej.reactor.Reactive.checkInReactorThread;

@SuppressWarnings("rawtypes")
public final class MapCrdtStorage<K extends Comparable<K>, S> extends AbstractReactive
	implements ICrdtStorage<K, S>, ReactiveService, ReactiveJmxBeanWithStats {

	private static final boolean CHECKS = Checks.isEnabled(MapCrdtStorage.class);

	public static final Duration DEFAULT_SMOOTHING_WINDOW = ApplicationSettings.getDuration(MapCrdtStorage.class, "smoothingWindow", Duration.ofMinutes(1));

	private final CrdtFunction<S> function;

	private final NavigableMap<K, CrdtData<K, S>> map = new TreeMap<>();
	private final NavigableMap<K, CrdtTombstone<K>> tombstones = new TreeMap<>();

	private NavigableMap<K, CrdtData<K, S>> takenMap;
	private NavigableMap<K, CrdtTombstone<K>> takenTombstones;

	private CrdtFilter<S> filter = $ -> true;

	private CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	// region JMX
	private boolean detailedStats;

	private final BasicStreamStats<CrdtData<K, S>> uploadStats = StreamStats.basic();
	private final DetailedStreamStats<CrdtData<K, S>> uploadStatsDetailed = StreamStats.detailed();
	private final BasicStreamStats<CrdtData<K, S>> downloadStats = StreamStats.basic();
	private final DetailedStreamStats<CrdtData<K, S>> downloadStatsDetailed = StreamStats.detailed();
	private final BasicStreamStats<CrdtData<K, S>> takeStats = StreamStats.basic();
	private final DetailedStreamStats<CrdtData<K, S>> takeStatsDetailed = StreamStats.detailed();
	private final BasicStreamStats<CrdtTombstone<K>> removeStats = StreamStats.basic();
	private final DetailedStreamStats<CrdtTombstone<K>> removeStatsDetailed = StreamStats.detailed();

	private final EventStats uploadedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats downloadedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats takenItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats removedItems = EventStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final EventStats singlePuts = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats singleGets = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final EventStats singleRemoves = EventStats.create(DEFAULT_SMOOTHING_WINDOW);
	// endregion

	private MapCrdtStorage(Reactor reactor, CrdtFunction<S> function) {
		super(reactor);
		this.function = function;
	}

	public static <K extends Comparable<K>, S> MapCrdtStorage<K, S> create(Reactor reactor, CrdtFunction<S> crdtFunction) {
		return MapCrdtStorage.<K, S>builder(reactor, crdtFunction).build();
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> MapCrdtStorage<K, S> create(Reactor reactor) {
		return MapCrdtStorage.<K, S>builder(reactor).build();
	}

	public static <K extends Comparable<K>, S> MapCrdtStorage<K, S>.Builder builder(Reactor reactor, CrdtFunction<S> crdtFunction) {
		return new MapCrdtStorage<K, S>(reactor, crdtFunction).new Builder();
	}

	public static <K extends Comparable<K>, S extends CrdtType<S>> MapCrdtStorage<K, S>.Builder builder(Reactor reactor) {
		return new MapCrdtStorage<K, S>(reactor, CrdtFunction.ofCrdtType()).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, MapCrdtStorage<K, S>> {
		private Builder() {}

		public Builder withFilter(CrdtFilter<S> filter) {
			checkNotBuilt(this);
			MapCrdtStorage.this.filter = filter;
			return this;
		}

		public Builder withCurrentTimeProvide(CurrentTimeProvider now) {
			checkNotBuilt(this);
			MapCrdtStorage.this.now = now;
			return this;
		}

		@Override
		protected MapCrdtStorage<K, S> doBuild() {
			return MapCrdtStorage.this;
		}
	}

	@Override
	public Promise<StreamConsumer<CrdtData<K, S>>> upload() {
		if (CHECKS) checkInReactorThread(this);
		ToListStreamConsumer<CrdtData<K, S>> consumer = ToListStreamConsumer.create();
		return Promise.of(consumer.withAcknowledgement(ack -> ack
				.whenResult(() -> consumer.getList().forEach(this::doPut))
				.mapException(e -> new CrdtException("Error while uploading CRDT data", e)))
			.transformWith(detailedStats ? uploadStatsDetailed : uploadStats)
			.transformWith(onItem(uploadedItems::recordEvent)));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> download(long timestamp) {
		if (CHECKS) checkInReactorThread(this);
		return Promise.of(StreamSuppliers.ofStream(extract(timestamp))
			.transformWith(detailedStats ? downloadStatsDetailed : downloadStats)
			.transformWith(onItem(downloadedItems::recordEvent))
			.withEndOfStream(eos -> eos
				.mapException(e -> new CrdtException("Error while downloading CRDT data", e))));
	}

	@Override
	public Promise<StreamSupplier<CrdtData<K, S>>> take() {
		if (CHECKS) checkInReactorThread(this);
		if (takenMap != null) {
			assert takenTombstones != null;
			return Promise.ofException(new CrdtException("Data is already being taken"));
		}
		takenMap = new TreeMap<>(map);
		takenTombstones = new TreeMap<>(tombstones);
		map.clear();
		tombstones.clear();

		StreamSupplier<CrdtData<K, S>> supplier = StreamSuppliers.ofIterable(takenMap.values())
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
		if (CHECKS) checkInReactorThread(this);
		ToListStreamConsumer<CrdtTombstone<K>> consumer = ToListStreamConsumer.create();
		return Promise.of(consumer.withAcknowledgement(ack -> ack
				.whenResult(() -> consumer.getList().forEach(this::doRemove))
				.mapException(e -> new CrdtException("Error while removing CRDT data", e)))
			.transformWith(detailedStats ? removeStatsDetailed : removeStats)
			.transformWith(onItem(removedItems::recordEvent)));
	}

	@Override
	public Promise<Void> ping() {
		if (CHECKS) checkInReactorThread(this);
		return Promise.complete();
	}

	@Override
	public Promise<?> start() {
		checkInReactorThread(this);
		return Promise.complete();
	}

	@Override
	public Promise<?> stop() {
		checkInReactorThread(this);
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
		if (CHECKS) checkInReactorThread(this);
		put(new CrdtData<>(key, now.currentTimeMillis(), state));
	}

	public void put(CrdtData<K, S> data) {
		if (CHECKS) checkInReactorThread(this);
		singlePuts.recordEvent();
		doPut(data);
	}

	public @Nullable S get(K key) {
		if (CHECKS) checkInReactorThread(this);
		singleGets.recordEvent();
		CrdtData<K, S> data = map.get(key);
		return data != null ? data.getState() : null;
	}

	public boolean remove(K key) {
		if (CHECKS) checkInReactorThread(this);
		return remove(new CrdtTombstone<>(key, now.currentTimeMillis()));
	}

	public boolean remove(CrdtTombstone<K> tombstone) {
		if (CHECKS) checkInReactorThread(this);
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
					MapCrdtStorage.this.remove(current.getKey());
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
	public BasicStreamStats getUploadStats() {
		return uploadStats;
	}

	@JmxAttribute
	public DetailedStreamStats getUploadStatsDetailed() {
		return uploadStatsDetailed;
	}

	@JmxAttribute
	public BasicStreamStats getDownloadStats() {
		return downloadStats;
	}

	@JmxAttribute
	public DetailedStreamStats getDownloadStatsDetailed() {
		return downloadStatsDetailed;
	}

	@JmxAttribute
	public BasicStreamStats getTakeStats() {
		return takeStats;
	}

	@JmxAttribute
	public DetailedStreamStats getTakeStatsDetailed() {
		return takeStatsDetailed;
	}

	@JmxAttribute
	public BasicStreamStats getRemoveStats() {
		return removeStats;
	}

	@JmxAttribute
	public DetailedStreamStats getRemoveStatsDetailed() {
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
