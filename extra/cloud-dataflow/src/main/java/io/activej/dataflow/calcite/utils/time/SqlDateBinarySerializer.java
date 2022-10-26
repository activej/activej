package io.activej.dataflow.calcite.utils.time;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.sql.Date;
import java.time.LocalDate;

public final class SqlDateBinarySerializer implements BinarySerializer<Date> {
	private static final SqlDateBinarySerializer INSTANCE = new SqlDateBinarySerializer();

	private SqlDateBinarySerializer() {
	}

	public static SqlDateBinarySerializer getInstance() {
		return INSTANCE;
	}

	@Override
	public void encode(BinaryOutput out, Date item) {
		LocalDate localDate = item.toLocalDate();
		out.writeVarInt(localDate.getYear());
		out.writeVarInt(localDate.getMonthValue());
		out.writeVarInt(localDate.getDayOfMonth());
	}

	@Override
	public Date decode(BinaryInput in) throws CorruptedDataException {
		return Date.valueOf(
				LocalDate.of(
						in.readVarInt(),
						in.readVarInt(),
						in.readVarInt()
				)
		);
	}

}
