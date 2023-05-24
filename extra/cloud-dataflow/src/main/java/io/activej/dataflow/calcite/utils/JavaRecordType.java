package io.activej.dataflow.calcite.utils;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public final class JavaRecordType extends RelRecordType {
	private final Class<?> clazz;

	public JavaRecordType(List<RelDataTypeField> fields, Class<?> clazz) {
		super(StructKind.FULLY_QUALIFIED, fields, true);
		this.clazz = Objects.requireNonNull(clazz, "clazz");
	}

	public Class<?> getClazz() {
		return clazz;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		return
			this == o ||
			(
				o instanceof JavaRecordType other &&
				this.clazz == other.clazz &&
				Objects.equals(this.fieldList, other.fieldList)
			);
	}

	@Override
	public int hashCode() {
		return Objects.hash(fieldList, clazz);
	}

}
