package io.activej.dataflow.calcite.utils.time;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public final class SqlTimestampBinarySerializer implements BinarySerializer<Timestamp> {
	private static final SqlTimestampBinarySerializer INSTANCE = new SqlTimestampBinarySerializer();

	private SqlTimestampBinarySerializer() {
	}

	public static SqlTimestampBinarySerializer getInstance() {
		return INSTANCE;
	}

	@Override
	public void encode(BinaryOutput out, Timestamp item) {
		LocalDateTime localDateTime = item.toLocalDateTime();

		out.writeVarInt(localDateTime.getYear());
		out.writeVarInt(localDateTime.getMonthValue());
		out.writeVarInt(localDateTime.getDayOfMonth());
		out.writeVarInt(localDateTime.getHour());
		out.writeVarInt(localDateTime.getMinute());
		out.writeVarInt(localDateTime.getSecond());
		out.writeVarInt(localDateTime.getNano());
	}

	@Override
	public Timestamp decode(BinaryInput in) throws CorruptedDataException {
		return Timestamp.valueOf(
				LocalDateTime.of(
						in.readVarInt(),
						in.readVarInt(),
						in.readVarInt(),
						in.readVarInt(),
						in.readVarInt(),
						in.readVarInt(),
						in.readVarInt()
				)
		);
	}
}
