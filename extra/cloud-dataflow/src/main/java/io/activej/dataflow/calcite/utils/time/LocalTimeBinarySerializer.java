package io.activej.dataflow.calcite.utils.time;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.time.LocalTime;

public final class LocalTimeBinarySerializer implements BinarySerializer<LocalTime> {
	private static final LocalTimeBinarySerializer INSTANCE = new LocalTimeBinarySerializer();

	private LocalTimeBinarySerializer() {
	}

	public static LocalTimeBinarySerializer getInstance() {
		return INSTANCE;
	}

	@Override
	public void encode(BinaryOutput out, LocalTime localTime) {
		out.writeVarInt(localTime.getHour());
		out.writeVarInt(localTime.getMinute());
		out.writeVarInt(localTime.getSecond());
		out.writeVarInt(localTime.getNano());
	}

	@Override
	public LocalTime decode(BinaryInput in) throws CorruptedDataException {
		return LocalTime.of(
				in.readVarInt(),
				in.readVarInt(),
				in.readVarInt(),
				in.readVarInt()
		);
	}
}
