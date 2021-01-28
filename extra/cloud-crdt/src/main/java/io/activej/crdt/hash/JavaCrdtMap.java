package io.activej.crdt.hash;

import io.activej.async.service.EventloopService;
import io.activej.crdt.function.CrdtFunction;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.datastream.StreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.TreeMap;

public class JavaCrdtMap<K extends Comparable<K>, S> implements CrdtMap<K, S>, EventloopService {
	private final Map<K, S> map = new TreeMap<>();

	private final Eventloop eventloop;
	private final CrdtFunction<S> crdtFunction;

	@Nullable
	private final CrdtStorage<K, S> storage;

	public JavaCrdtMap(Eventloop eventloop, CrdtFunction<S> crdtFunction) {
		this.eventloop = eventloop;
		this.crdtFunction = crdtFunction;
		this.storage = null;
	}

	public JavaCrdtMap(Eventloop eventloop, CrdtFunction<S> crdtFunction, @NotNull CrdtStorage<K, S> storage) {
		this.eventloop = eventloop;
		this.crdtFunction = crdtFunction;
		this.storage = storage;
	}

	@Override
	public Promise<@Nullable S> get(K key) {
		return Promise.of(map.get(key));
	}

	@Override
	public Promise<@Nullable S> put(K key, S value) {
		return Promise.of(map.merge(key, value, crdtFunction::merge));
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<?> start() {
		return storage == null ?
				Promise.complete() :
				storage.download()
						.then(supplier -> supplier.streamTo(StreamConsumer.of(crdtData -> map.put(crdtData.getKey(), crdtData.getState()))));
	}

	@Override
	public @NotNull Promise<?> stop() {
		return Promise.complete();
	}
}
