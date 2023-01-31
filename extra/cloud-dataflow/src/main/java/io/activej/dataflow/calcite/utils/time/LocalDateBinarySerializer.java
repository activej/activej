package io.activej.dataflow.calcite.utils.time;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.time.LocalDate;

public final class LocalDateBinarySerializer implements BinarySerializer<LocalDate> {
	private static final LocalDateBinarySerializer INSTANCE = new LocalDateBinarySerializer();

	private LocalDateBinarySerializer() {
	}

	public static LocalDateBinarySerializer getInstance() {
		return INSTANCE;
	}

	@Override
	public void encode(BinaryOutput out, LocalDate localDate) {
		out.writeVarInt(localDate.getYear());
		out.writeVarInt(localDate.getMonthValue());
		out.writeVarInt(localDate.getDayOfMonth());
	}

	@Override
	public LocalDate decode(BinaryInput in) throws CorruptedDataException {
		return LocalDate.of(
				in.readVarInt(),
				in.readVarInt(),
				in.readVarInt()
		);
	}

}
