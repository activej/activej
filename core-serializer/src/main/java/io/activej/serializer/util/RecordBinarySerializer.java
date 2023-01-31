package io.activej.serializer.util;

import io.activej.common.builder.AbstractBuilder;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CorruptedDataException;

public final class RecordBinarySerializer implements BinarySerializer<Record> {
	private final RecordScheme scheme;
	private final BinarySerializer<?>[] fieldSerializers;

	private RecordBinarySerializer(RecordScheme scheme) {
		this.scheme = scheme;
		this.fieldSerializers = new BinarySerializer[scheme.size()];
	}

	public static RecordBinarySerializer create(RecordScheme scheme) {
		return builder(scheme).build();
	}

	public static Builder builder(RecordScheme scheme) {
		return new RecordBinarySerializer(scheme).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, RecordBinarySerializer> {
		private Builder() {}

		public Builder withField(String field, BinarySerializer<?> fieldSerializer) {
			checkNotBuilt(this);
			RecordBinarySerializer.this.fieldSerializers[scheme.getFieldIndex(field)] = fieldSerializer;
			return this;
		}

		@Override
		protected RecordBinarySerializer doBuild() {
			return RecordBinarySerializer.this;
		}
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
