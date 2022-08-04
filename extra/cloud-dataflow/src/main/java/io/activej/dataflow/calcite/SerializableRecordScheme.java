package io.activej.dataflow.calcite;

import io.activej.codegen.DefiningClassLoader;
import io.activej.record.RecordScheme;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;

public final class SerializableRecordScheme {
	private final LinkedHashMap<String, Type> fields;

	public SerializableRecordScheme(@Deserialize("fields") LinkedHashMap<String, Type> fields) {
		this.fields = fields;
	}

	public static SerializableRecordScheme fromRecordScheme(RecordScheme recordScheme) {
		List<String> fieldNames = recordScheme.getFields();
		List<Type> fieldTypes = recordScheme.getTypes();

		assert fieldNames.size() == fieldTypes.size();

		LinkedHashMap<String, Type> fields = new LinkedHashMap<>(2 * recordScheme.size());
		for (int i = 0; i < fieldNames.size(); i++) {
			String fieldName = fieldNames.get(i);
			Type fieldType = fieldTypes.get(i);

			fields.put(fieldName, fieldType);
		}

		return new SerializableRecordScheme(fields);
	}

	public RecordScheme toRecordScheme(DefiningClassLoader classLoader) {
		RecordScheme recordScheme = RecordScheme.create(classLoader);

		fields.forEach(recordScheme::addField);

		return recordScheme.build();
	}

	@Serialize(order = 1)
	public LinkedHashMap<String, Type> getFields() {
		return fields;
	}

	@Override
	public String toString() {
		return "SerializableRecordScheme{" +
				"fields=" + fields +
				'}';
	}
}
