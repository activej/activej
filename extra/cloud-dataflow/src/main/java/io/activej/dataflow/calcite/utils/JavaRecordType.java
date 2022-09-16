package io.activej.dataflow.calcite.utils;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public final class JavaRecordType extends RelRecordType {
	private final Class<?> clazz;

	public JavaRecordType(List<RelDataTypeField> fields, Class<?> clazz) {
		super(fields);
		this.clazz = Objects.requireNonNull(clazz, "clazz");
	}

	public Class<?> getClazz() {
		return clazz;
	}

	@Override
	public boolean equals(@Nullable Object obj) {
		return this == obj
				|| obj instanceof JavaRecordType
				&& Objects.equals(fieldList, ((JavaRecordType) obj).fieldList)
				&& clazz == ((JavaRecordType) obj).clazz;
	}

	@Override
	public int hashCode() {
		return Objects.hash(fieldList, clazz);
	}

}
