package io.activej.dataflow.calcite.operand;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.rex.RexDynamicParam;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.List;

import static io.activej.codegen.expression.Expressions.*;

public final class OperandFieldAccess implements Operand<OperandFieldAccess> {
	private final Operand<?> objectOperand;
	private final String fieldName;
	private final DefiningClassLoader classLoader;

	public OperandFieldAccess(Operand<?> objectOperand, String fieldName, DefiningClassLoader classLoader) {
		this.objectOperand = objectOperand;
		this.fieldName = fieldName;
		this.classLoader = classLoader;
	}

	public OperandFieldAccess(Operand<?> objectOperand, String fieldName) {
		this(objectOperand, fieldName, DefiningClassLoader.create());
	}

	@Override
	public <T> @Nullable T getValue(Record record) {
		Object object = objectOperand.getValue(record);
		if (object == null) return null;

		Class<?> objectClass = object.getClass();

		FieldGetter fieldGetter = classLoader.ensureClassAndCreateInstance(
				ClassKey.of(FieldGetter.class, objectClass, fieldName),
				() -> ClassBuilder.create(FieldGetter.class)
						.withMethod("getField", property(cast(arg(0), objectClass), fieldName))
		);

		return fieldGetter.getField(object, fieldName);
	}

	@Override
	public Type getFieldType(RecordScheme original) {
		Class<?> fieldType = (Class<?>) objectOperand.getFieldType(original);

		if (fieldType.isRecord()) {
			for (RecordComponent recordComponent : fieldType.getRecordComponents()) {
				if (recordComponent.getName().equals(fieldName)) return recordComponent.getGenericType();
			}
		}

		for (Field field : fieldType.getFields()) {
			if (field.getName().equals(fieldName)) {
				return field.getGenericType();
			}
		}

		String capitalized = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
		String getMethod = "get" + capitalized;
		String isMethod = "is" + capitalized;
		for (Method method : fieldType.getMethods()) {
			if (method.getName().equals(getMethod) || method.getName().equals(isMethod)) {
				return method.getGenericReturnType();
			}
		}

		throw new IllegalArgumentException();
	}

	@Override
	public String getFieldName(RecordScheme original) {
		return objectOperand.getFieldName(original) + '.' + fieldName;
	}

	@Override
	public OperandFieldAccess materialize(List<Object> params) {
		return new OperandFieldAccess(
				objectOperand.materialize(params),
				fieldName,
				classLoader
		);
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return objectOperand.getParams();
	}

	public Operand<?> getObjectOperand() {
		return objectOperand;
	}

	public String getFieldName() {
		return fieldName;
	}

	public interface FieldGetter {
		@Nullable <T> T getField(Object object, String fieldName);
	}

	@Override
	public String toString() {
		return "OperandFieldAcces[" +
				"objectOperand=" + objectOperand + ", " +
				"fieldName=" + fieldName + ']';
	}
}
