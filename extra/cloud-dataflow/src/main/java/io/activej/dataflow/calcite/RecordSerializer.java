package io.activej.dataflow.calcite;

import io.activej.common.Checks;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.*;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.List;

import static io.activej.common.Checks.checkState;

public final class RecordSerializer implements BinarySerializer<Record> {
	private static final boolean CHECK = Checks.isEnabled(RecordSerializer.class);

	private final BinarySerializerLocator locator;

	private @Nullable RecordScheme encodeRecordScheme;
	private @Nullable RecordScheme decodeRecordScheme;

	private BinarySerializer<Object> @Nullable [] fieldSerializers;

	private RecordSerializer(BinarySerializerLocator locator) {
		this.locator = locator;
	}

	public static RecordSerializer create(BinarySerializerLocator locator) {
		return new RecordSerializer(locator);
	}

	@Override
	public void encode(BinaryOutput out, Record item) {
		if (encodeRecordScheme == null) {
			encodeRecordScheme = item.getScheme();
			assert encodeRecordScheme != null;

			if (CHECK && decodeRecordScheme != null) {
				checkState(decodeRecordScheme.equals(encodeRecordScheme));
			}

			ensureFieldSerializers(encodeRecordScheme);
			locator.get(RecordScheme.class).encode(out, encodeRecordScheme);
		}

		Object[] array = item.toArray();

		assert fieldSerializers != null;
		for (int i = 0; i < array.length; i++) {
			fieldSerializers[i].encode(out, array[i]);
		}
	}

	@Override
	public Record decode(BinaryInput in) throws CorruptedDataException {
		if (decodeRecordScheme == null) {
			decodeRecordScheme = locator.get(RecordScheme.class).decode(in);
			assert decodeRecordScheme != null;

			if (CHECK && encodeRecordScheme != null) {
				checkState(decodeRecordScheme.equals(encodeRecordScheme));
			}

			ensureFieldSerializers(decodeRecordScheme);
		}

		assert fieldSerializers != null;
		Record record = decodeRecordScheme.record();
		for (int i = 0; i < decodeRecordScheme.size(); i++) {
			Object fieldValue = fieldSerializers[i].decode(in);
			record.set(i, fieldValue);
		}

		return record;
	}

	@SuppressWarnings("unchecked")
	private void ensureFieldSerializers(RecordScheme scheme) {
		if (fieldSerializers != null) return;

		fieldSerializers = new BinarySerializer[scheme.size()];

		List<Type> types = scheme.getTypes();

		for (int i = 0; i < types.size(); i++) {
			Type type = types.get(i);
			fieldSerializers[i] = BinarySerializers.ofNullable(locator.get(type));
		}
	}
}
