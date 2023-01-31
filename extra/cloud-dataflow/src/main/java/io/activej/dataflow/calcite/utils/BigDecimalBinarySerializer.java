package io.activej.dataflow.calcite.utils;

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class BigDecimalBinarySerializer implements BinarySerializer<BigDecimal> {
	private static final BigDecimalBinarySerializer INSTANCE = new BigDecimalBinarySerializer();

	private BigDecimalBinarySerializer() {
	}

	public static BigDecimalBinarySerializer getInstance() {
		return INSTANCE;
	}

	@Override
	public void encode(BinaryOutput out, BigDecimal item) {
		out.writeVarInt(item.scale());
		byte[] bytes = item.unscaledValue().toByteArray();
		out.writeVarInt(bytes.length);
		out.write(bytes);
	}

	@Override
	public BigDecimal decode(BinaryInput in) throws CorruptedDataException {
		int scale = in.readVarInt();
		byte[] bytes = new byte[in.readVarInt()];
		in.read(bytes);
		BigInteger unscaledValue = new BigInteger(bytes);
		return new BigDecimal(unscaledValue, scale);
	}
}
