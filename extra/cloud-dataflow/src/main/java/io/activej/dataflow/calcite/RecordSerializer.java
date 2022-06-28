package io.activej.dataflow.calcite;

import io.activej.common.Checks;
import io.activej.dataflow.inject.BinarySerializerModule.BinarySerializerLocator;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.*;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkState;

public final class RecordSerializer implements BinarySerializer<Record> {
	private static final boolean CHECK = Checks.isEnabled(RecordSerializer.class);

	private final BinarySerializerLocator locator;
	private final BinarySerializer<String> stringSerializer;

	private @Nullable RecordScheme encodeRecordScheme;
	private @Nullable RecordScheme decodeRecordScheme;

	private BinarySerializer<Object> @Nullable [] fieldSerializers;

	private RecordSerializer(BinarySerializerLocator locator, BinarySerializer<String> stringSerializer) {
		this.locator = locator;
		this.stringSerializer = stringSerializer;
	}

	public static RecordSerializer create(BinarySerializerLocator locator) {
		return new RecordSerializer(locator, locator.get(String.class));
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
			SerializableRecordScheme serializableRecordScheme = SerializableRecordScheme.fromRecordScheme(encodeRecordScheme);
			locator.get(SerializableRecordScheme.class).encode(out, serializableRecordScheme);
		}

		Map<String, Object> map = item.toMap();

		int i = 0;
		assert fieldSerializers != null;
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			stringSerializer.encode(out, entry.getKey());
			fieldSerializers[i++].encode(out, entry.getValue());
		}
	}

	@Override
	public Record decode(BinaryInput in) throws CorruptedDataException {
		if (decodeRecordScheme == null) {
			SerializableRecordScheme serializableRecordScheme = locator.get(SerializableRecordScheme.class).decode(in);
			decodeRecordScheme = serializableRecordScheme.toRecordScheme();
			assert decodeRecordScheme != null;

			if (CHECK && encodeRecordScheme != null) {
				checkState(decodeRecordScheme.equals(encodeRecordScheme));
			}

			ensureFieldSerializers(decodeRecordScheme);
		}

		assert fieldSerializers != null;
		Record record = decodeRecordScheme.record();
		for (int i = 0; i < decodeRecordScheme.size(); i++) {
			String fieldName = stringSerializer.decode(in);
			Object fieldValue = fieldSerializers[i].decode(in);

			record.set(fieldName, fieldValue);
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
