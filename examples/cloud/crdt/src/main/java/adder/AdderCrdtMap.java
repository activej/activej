package adder;

import io.activej.async.function.AsyncRunnable;
import io.activej.async.function.AsyncRunnables;
import io.activej.async.service.ReactiveService;
import io.activej.crdt.hash.ICrdtMap;
import io.activej.crdt.storage.ICrdtStorage;
import io.activej.datastream.consumer.StreamConsumers;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class AdderCrdtMap extends AbstractReactive
	implements ICrdtMap<Long, SimpleSumsCrdtState>, ReactiveService {
	private final Map<Long, SimpleSumsCrdtState> map = new TreeMap<>();

	private final String localServerId;
	private final AsyncRunnable refresh;

	public AdderCrdtMap(Reactor reactor, String localServerId, ICrdtStorage<Long, DetailedSumsCrdtState> storage) {
		super(reactor);
		this.localServerId = localServerId;
		this.refresh = AsyncRunnables.reuse(() -> doRefresh(storage));
	}

	@Override
	public Promise<@Nullable SimpleSumsCrdtState> get(Long key) {
		return Promise.of(map.get(key));
	}

	@Override
	public Promise<Void> refresh() {
		return refresh.run();
	}

	@Override
	public Promise<@Nullable SimpleSumsCrdtState> put(Long key, SimpleSumsCrdtState value) {
		return Promise.of(map.merge(key, value, SimpleSumsCrdtState::merge));
	}

	@Override
	public Promise<?> start() {
		return refresh();
	}

	@Override
	public Promise<?> stop() {
		return Promise.complete();
	}

	public Map<Long, SimpleSumsCrdtState> getMap() {
		return Collections.unmodifiableMap(map);
	}

	private Promise<Void> doRefresh(ICrdtStorage<Long, DetailedSumsCrdtState> storage) {
		return storage.download()
			.then(supplier -> supplier.streamTo(
				StreamConsumers.ofConsumer(crdtData -> {
					DetailedSumsCrdtState globalState = crdtData.getState();

					float localSum = globalState.getSumFor(localServerId);
					float otherSum = globalState.getSumExcept(localServerId);

					map.put(crdtData.getKey(), SimpleSumsCrdtState.of(localSum, otherSum));
				})));
	}
}
