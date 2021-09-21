package io.activej.serializer.util;

import io.activej.codegen.util.WithInitializer;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

public final class RecordSerializer implements BinarySerializer<Record>, WithInitializer<RecordSerializer> {
	private final RecordScheme scheme;
	private final BinarySerializer<?>[] fieldSerializers;

	private RecordSerializer(RecordScheme scheme) {
		this.scheme = scheme;
		this.fieldSerializers = new BinarySerializer[scheme.size()];
	}

	public static RecordSerializer create(RecordScheme scheme) {
		return new RecordSerializer(scheme);
	}

	public RecordSerializer withField(String field, BinarySerializer<?> fieldSerializer) {
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
