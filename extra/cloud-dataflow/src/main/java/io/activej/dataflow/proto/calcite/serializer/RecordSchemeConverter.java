package io.activej.dataflow.proto.calcite.serializer;

import com.google.protobuf.ProtocolStringList;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.proto.calcite.JavaTypeProto.JavaType;
import io.activej.dataflow.proto.calcite.RecordSchemeProto;
import io.activej.record.RecordScheme;
import io.activej.serializer.CorruptedDataException;

import java.util.List;

public final class RecordSchemeConverter {

	private RecordSchemeConverter() {
	}

	public static RecordSchemeProto.RecordScheme convert(RecordScheme recordScheme) {
		RecordSchemeProto.RecordScheme.Builder builder = RecordSchemeProto.RecordScheme.newBuilder();

		builder.addAllFieldNames(recordScheme.getFields());
		builder.addAllTypes(recordScheme.getTypes().stream()
				.map(JavaTypeConverter::convert)
				.toList());

		return builder.build();
	}

	public static RecordScheme convert(DefiningClassLoader classLoader, RecordSchemeProto.RecordScheme recordScheme) {
		RecordScheme scheme = RecordScheme.create(classLoader);

		ProtocolStringList fieldNamesList = recordScheme.getFieldNamesList();
		List<JavaType> typesList = recordScheme.getTypesList();
		if (fieldNamesList.size() != typesList.size()) {
			throw new CorruptedDataException("Lengths of field names and field types mismatch");
		}

		for (int i = 0; i < fieldNamesList.size(); i++) {
			String fieldName = fieldNamesList.get(i);
			JavaType type = typesList.get(i);

			scheme.addField(fieldName, JavaTypeConverter.convert(type));
		}

		return scheme
				.withComparator(scheme.getFields())
				.build();
	}
}
