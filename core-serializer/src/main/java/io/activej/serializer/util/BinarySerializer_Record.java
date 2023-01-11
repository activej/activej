package io.activej.serializer.util;

import io.activej.common.initializer.WithInitializer;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

public final class BinarySerializer_Record implements BinarySerializer<Record>, WithInitializer<BinarySerializer_Record> {
	private final RecordScheme scheme;
	private final BinarySerializer<?>[] fieldSerializers;

	private BinarySerializer_Record(RecordScheme scheme) {
		this.scheme = scheme;
		this.fieldSerializers = new BinarySerializer[scheme.size()];
	}

	public static BinarySerializer_Record create(RecordScheme scheme) {
		return new BinarySerializer_Record(scheme);
	}

	public BinarySerializer_Record withField(String field, BinarySerializer<?> fieldSerializer) {
		this.fieldSerializers[scheme.getFieldIndex(field)] = fieldSerializer;
		return this;
	}

	@Override
	public void encode(BinaryOutput out, Record record) {
		for (int i = 0; i < fieldSerializers.length; i++) {
			//noinspection unchecked
			BinarySerializer<Object> serializer = (BinarySerializer<Object>) fieldSerializers[i];
			serializer.encode(out, record.get(i));
		}
	}

	@Override
	public Record decode(BinaryInput in) throws CorruptedDataException {
		Record record = scheme.record();
		for (int i = 0; i < fieldSerializers.length; i++) {
			BinarySerializer<?> serializer = fieldSerializers[i];
			record.set(i, serializer.decode(in));
		}
		return record;
	}
}
