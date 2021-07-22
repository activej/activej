package adder;

import io.activej.crdt.primitives.CrdtMergable;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;

import java.util.HashMap;
import java.util.Map;

import static io.activej.serializer.BinarySerializers.*;

public final class DetailedSumsCrdtState implements CrdtMergable<DetailedSumsCrdtState> {
	public static final BinarySerializer<DetailedSumsCrdtState> SERIALIZER = new DetailedSumsCrdtState.Serializer();

	private final Map<String, Float> sums;

	public DetailedSumsCrdtState() {
		this.sums = new HashMap<>();
	}

	public DetailedSumsCrdtState(Map<String, Float> sums) {
		this.sums = new HashMap<>(sums);
	}

	public static DetailedSumsCrdtState of(String serverId, float sum) {
		Map<String, Float> map = new HashMap<>();
		map.put(serverId, sum);
		return new DetailedSumsCrdtState(map);
	}

	public float getSumFor(String serverId) {
		return sums.get(serverId);
	}

	public float getSumExcept(String serverId) {
		float sum = 0;
		for (Map.Entry<String, Float> entry : sums.entrySet()) {
			if (entry.getKey().equals(serverId)) continue;
			sum += entry.getValue();
		}
		return sum;
	}

	@Override
	public DetailedSumsCrdtState merge(DetailedSumsCrdtState other) {
		DetailedSumsCrdtState newState = new DetailedSumsCrdtState();
		newState.sums.putAll(this.sums);

		other.sums.forEach((serverId, delta) -> newState.sums.merge(serverId, delta, Math::max));

		return newState;
	}

	private static class Serializer implements BinarySerializer<DetailedSumsCrdtState> {
		private static final BinarySerializer<Map<String, Float>> MAP_SERIALIZER = ofMap(ISO_88591_SERIALIZER, FLOAT_SERIALIZER);

		@Override
		public void encode(BinaryOutput out, DetailedSumsCrdtState item) {
			MAP_SERIALIZER.encode(out, item.sums);
		}

		@Override
		public DetailedSumsCrdtState decode(BinaryInput in) {
			return new DetailedSumsCrdtState(MAP_SERIALIZER.decode(in));
		}
	}
}
