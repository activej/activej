package io.activej.dataflow.calcite;

import io.activej.codegen.DefiningClassLoader;
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

	private final DefiningClassLoader classLoader;
	private final BinarySerializerLocator locator;
	private final BinarySerializer<Integer> intSerializer;

	private @Nullable RecordScheme encodeRecordScheme;
	private @Nullable RecordScheme decodeRecordScheme;

	private BinarySerializer<Object> @Nullable [] fieldSerializers;

	private RecordSerializer(DefiningClassLoader classLoader, BinarySerializerLocator locator, BinarySerializer<Integer> intSerializer) {
		this.classLoader = classLoader;
		this.locator = locator;
		this.intSerializer = intSerializer;
	}

	public static RecordSerializer create(DefiningClassLoader classLoader, BinarySerializerLocator locator) {
		return new RecordSerializer(classLoader, locator, locator.get(int.class));
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

		Object[] array = item.toArray();

		assert fieldSerializers != null;
		for (int i = 0; i < array.length; i++) {
			intSerializer.encode(out, i);
			fieldSerializers[i].encode(out, array[i]);
		}
	}

	@Override
	public Record decode(BinaryInput in) throws CorruptedDataException {
		if (decodeRecordScheme == null) {
			SerializableRecordScheme serializableRecordScheme = locator.get(SerializableRecordScheme.class).decode(in);
			decodeRecordScheme = serializableRecordScheme.toRecordScheme(classLoader);
			assert decodeRecordScheme != null;

			if (CHECK && encodeRecordScheme != null) {
				checkState(decodeRecordScheme.equals(encodeRecordScheme));
			}

			ensureFieldSerializers(decodeRecordScheme);
		}

		assert fieldSerializers != null;
		Record record = decodeRecordScheme.record();
		for (int i = 0; i < decodeRecordScheme.size(); i++) {
			int index = intSerializer.decode(in);
			Object fieldValue = fieldSerializers[i].decode(in);

			record.set(index, fieldValue);
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
