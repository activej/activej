package io.activej.crdt.wal;

import io.activej.async.service.EventloopService;
import io.activej.crdt.CrdtData;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.TreeMap;

public class InMemoryWriteAheadLog<K extends Comparable<K>, S> implements WriteAheadLog<K, S>, EventloopService {
	private final Map<K, S> map = new TreeMap<>();

	private final Eventloop eventloop;
	private final CrdtFunction<S> function;
	private final CrdtStorage<K, S> storage;

	public InMemoryWriteAheadLog(Eventloop eventloop, CrdtFunction<S> function, CrdtStorage<K, S> storage) {
		this.eventloop = eventloop;
		this.function = function;
		this.storage = storage;
	}

	@Override
	public Promise<Void> put(K key, S value) {
		map.merge(key, value, function::merge);
		return Promise.complete();
	}

	@Override
	public Promise<Void> flush() {
		if(map.isEmpty()) return Promise.complete();

		Map<K, S> sorted = new TreeMap<>(map);
		map.clear();
		return storage.upload()
				.then(consumer -> StreamSupplier.ofStream(sorted.entrySet().stream()
						.map(entry -> new CrdtData<>(entry.getKey(), entry.getValue())))
						.streamTo(consumer))
				.whenException(e -> sorted.forEach(this::put));
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<?> start() {
		return Promise.complete();
	}

	@Override
	public @NotNull Promise<?> stop() {
		return flush();
	}
}
