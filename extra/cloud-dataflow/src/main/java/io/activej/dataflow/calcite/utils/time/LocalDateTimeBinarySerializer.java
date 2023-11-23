package io.activej.dataflow.calcite.utils.time;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.time.LocalDateTime;

public final class LocalDateTimeBinarySerializer implements BinarySerializer<LocalDateTime> {
	private static final LocalDateTimeBinarySerializer INSTANCE = new LocalDateTimeBinarySerializer();

	private LocalDateTimeBinarySerializer() {
	}

	public static LocalDateTimeBinarySerializer getInstance() {
		return INSTANCE;
	}

	@Override
	public void encode(BinaryOutput out, LocalDateTime localDateTime) {
		LocalDateBinarySerializer.getInstance().encode(out, localDateTime.toLocalDate());
		LocalTimeBinarySerializer.getInstance().encode(out, localDateTime.toLocalTime());
	}

	@Override
	public LocalDateTime decode(BinaryInput in) throws CorruptedDataException {
		return LocalDateTime.of(
			LocalDateBinarySerializer.getInstance().decode(in),
			LocalTimeBinarySerializer.getInstance().decode(in)
		);
	}
}
