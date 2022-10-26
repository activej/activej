package io.activej.dataflow.calcite.utils.time;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.sql.Time;
import java.time.LocalTime;

public final class SqlTimeBinarySerializer implements BinarySerializer<Time> {
	private static final SqlTimeBinarySerializer INSTANCE = new SqlTimeBinarySerializer();

	private SqlTimeBinarySerializer() {
	}

	public static SqlTimeBinarySerializer getInstance() {
		return INSTANCE;
	}

	@Override
	public void encode(BinaryOutput out, Time item) {
		LocalTime localTime = item.toLocalTime();
		out.writeVarInt(localTime.getHour());
		out.writeVarInt(localTime.getMinute());
		out.writeVarInt(localTime.getSecond());
		out.writeVarInt(localTime.getNano());
	}

	@Override
	public Time decode(BinaryInput in) throws CorruptedDataException {
		return Time.valueOf(
				LocalTime.of(
						in.readVarInt(),
						in.readVarInt(),
						in.readVarInt(),
						in.readVarInt()
				)
		);
	}
}
