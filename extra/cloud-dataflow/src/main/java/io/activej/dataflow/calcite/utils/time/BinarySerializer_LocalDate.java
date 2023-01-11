package io.activej.dataflow.calcite.utils.time;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.time.LocalDate;

public final class BinarySerializer_LocalDate implements BinarySerializer<LocalDate> {
	private static final BinarySerializer_LocalDate INSTANCE = new BinarySerializer_LocalDate();

	private BinarySerializer_LocalDate() {
	}

	public static BinarySerializer_LocalDate getInstance() {
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
