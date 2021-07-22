package adder;

import io.activej.async.function.AsyncSupplier;
import io.activej.async.function.AsyncSuppliers;
import io.activej.async.service.EventloopService;
import io.activej.crdt.hash.CrdtMap;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.datastream.StreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.TreeMap;

public class AdderCrdtMap implements CrdtMap<Long, SimpleSumsCrdtState>, EventloopService {
	private final Map<Long, SimpleSumsCrdtState> map = new TreeMap<>();

	private final Eventloop eventloop;
	private final String localServerId;
	private final AsyncSupplier<Void> refresh;

	public AdderCrdtMap(Eventloop eventloop, String localServerId, @NotNull CrdtStorage<Long, DetailedSumsCrdtState> storage) {
		this.eventloop = eventloop;
		this.localServerId = localServerId;
		this.refresh = AsyncSuppliers.reuse(() -> doRefresh(storage));
	}

	@Override
	public Promise<@Nullable SimpleSumsCrdtState> get(Long key) {
		return Promise.of(map.get(key));
	}

	@Override
	public Promise<Void> refresh() {
		return refresh.get();
	}

	@Override
	public Promise<@Nullable SimpleSumsCrdtState> put(Long key, SimpleSumsCrdtState value) {
		return Promise.of(map.merge(key, value, SimpleSumsCrdtState::merge));
	}

	@Override
	public @NotNull Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public @NotNull Promise<?> start() {
		return refresh();
	}

	@Override
	public @NotNull Promise<?> stop() {
		return Promise.complete();
	}

	private Promise<Void> doRefresh(CrdtStorage<Long, DetailedSumsCrdtState> storage) {
		return storage.download()
				.then(supplier -> supplier.streamTo(
						StreamConsumer.of(crdtData -> {
							DetailedSumsCrdtState globalState = crdtData.getState();

							float localSum = globalState.getSumFor(localServerId);
							float otherSum = globalState.getSumExcept(localServerId);

							map.put(crdtData.getKey(), SimpleSumsCrdtState.of(localSum, otherSum));
						})));
	}
}
