package io.activej.dataflow.calcite.where;

import io.activej.codegen.ClassBuilder;
import io.activej.codegen.ClassKey;
import io.activej.codegen.DefiningClassLoader;
import io.activej.record.Record;
import org.jetbrains.annotations.Nullable;

import static io.activej.codegen.expression.Expressions.*;

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
