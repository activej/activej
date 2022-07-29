package io.activej.dataflow.calcite.where;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.Utils;
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
import static io.activej.common.Checks.checkNotNull;

public final class OperandFieldAccess implements Operand {
	private final Operand objectOperand;
	private final Operand fieldNameOperand;
	private final DefiningClassLoader classLoader;

	public OperandFieldAccess(Operand objectOperand, Operand fieldNameOperand, DefiningClassLoader classLoader) {
		this.objectOperand = objectOperand;
		this.fieldNameOperand = fieldNameOperand;
		this.classLoader = classLoader;
	}

	public OperandFieldAccess(Operand objectOperand, Operand fieldNameOperand) {
		this(objectOperand, fieldNameOperand, DefiningClassLoader.create());
	}

	@Override
	public <T> @Nullable T getValue(Record record) {
		Object object = objectOperand.getValue(record);
		if (object == null) return null;

		String fieldName = fieldNameOperand.getValue(record);
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
		String fieldName = checkNotNull(fieldNameOperand.getValue(original.record()));

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
		return objectOperand.getFieldName(original) + '.' + fieldNameOperand.getFieldName(original);
	}

	@Override
	public Operand materialize(List<Object> params) {
		return new OperandFieldAccess(
				objectOperand.materialize(params),
				fieldNameOperand.materialize(params),
				classLoader
		);
	}

	@Override
	public List<RexDynamicParam> getParams() {
		return Utils.concat(objectOperand.getParams(), fieldNameOperand.getParams());
	}

	public Operand getObjectOperand() {
		return objectOperand;
	}

	public Operand getFieldNameOperand() {
		return fieldNameOperand;
	}

	public interface FieldGetter {
		@Nullable <T> T getField(Object object, String fieldName);
	}

	@Override
	public String toString() {
		return "OperandFieldAcces[" +
				"objectOperand=" + objectOperand + ", " +
				"fieldNameOperand=" + fieldNameOperand + ']';
	}
}
