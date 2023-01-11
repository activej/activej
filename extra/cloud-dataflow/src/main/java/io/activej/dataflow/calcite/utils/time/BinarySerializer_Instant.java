package io.activej.dataflow.calcite.utils.time;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.time.Instant;

public final class BinarySerializer_Instant implements BinarySerializer<Instant> {
	private static final BinarySerializer_Instant INSTANCE = new BinarySerializer_Instant();

	private BinarySerializer_Instant() {
	}

	public static BinarySerializer_Instant getInstance() {
		return INSTANCE;
	}

	@Override
	public void encode(BinaryOutput out, Instant instant) {
		out.writeVarLong(instant.getEpochSecond());
		out.writeVarInt(instant.getNano());
	}

	@Override
	public Instant decode(BinaryInput in) throws CorruptedDataException {
		return Instant.ofEpochSecond(
				in.readVarLong(),
				in.readVarInt()
		);
	}
}
