package io.activej.dataflow.calcite.utils;

import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.util.Primitives;
import io.activej.common.exception.ToDoException;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.function.ProjectionFunction;
import io.activej.dataflow.calcite.inject.CalciteServerModule;
import io.activej.dataflow.calcite.operand.*;
import io.activej.dataflow.dataset.Datasets;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.record.Record;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.*;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Utils {
	@SuppressWarnings("unchecked")
	public static <T> T toJavaType(RexLiteral literal) {
		SqlTypeName typeName = literal.getTypeName();
		if (typeName.getFamily() == SqlTypeFamily.CHARACTER) {
			return (T) literal.getValueAs(String.class);
		}

		return (T) literal.getValue();
	}

	public static Operand<?> toOperand(RexNode node, DefiningClassLoader classLoader) {
		if (node instanceof RexDynamicParam dynamicParam) {
			return new OperandScalar(Value.unmaterializedValue(dynamicParam));
		} else if (node instanceof RexCall call) {
			switch (call.getKind()) {
				case CAST -> {
					int castType = call.getType().getSqlTypeName().getJdbcOrdinal();
					Operand<?> operand = toOperand(call.getOperands().get(0), classLoader);
					return new OperandCast(operand, castType);
				}
				case OTHER_FUNCTION -> {
					SqlOperator operator = call.getOperator();
					if (operator instanceof ProjectionFunction projectionFunction) {
						List<Operand<?>> operands = call.getOperands()
								.stream()
								.map(operand -> toOperand(operand, classLoader))
								.collect(Collectors.toList());

						return projectionFunction.toOperandFunction(operands);
					}
				}
			}
		} else if (node instanceof RexLiteral literal) {
			Value value = Value.materializedValue(literal);
			return new OperandScalar(value);
		} else if (node instanceof RexInputRef inputRef) {
			return new OperandRecordField(inputRef.getIndex());
		} else if (node instanceof RexFieldAccess fieldAccess) {
			Operand<?> objectOperand = toOperand(fieldAccess.getReferenceExpr(), classLoader);
			return new OperandFieldAccess(objectOperand, fieldAccess.getField().getName(), classLoader);
		}
		throw new IllegalArgumentException("Unknown node: " + node);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public static int compareToUnknown(Comparable left, Comparable right) {
		Class<? extends Comparable> leftClass = left.getClass();
		Class<? extends Comparable> rightClass = right.getClass();

		if (leftClass == rightClass) {
			return left.compareTo(right);
		}

		if (left instanceof Number number1 && right instanceof Number number2) {
			return Double.compare(number1.doubleValue(), number2.doubleValue());
		}

		return left.compareTo(right);
	}

	public static boolean equalsUnknown(Object object1, Object object2) {
		if (object1.getClass() != object2.getClass() &&
				object1 instanceof Number number1 && object2 instanceof Number number2) {
			return Double.compare(number1.doubleValue(), number2.doubleValue()) == 0;
		}

		return object1.equals(object2);
	}

	public static SortedDataset<Record, Record> singleDummyDataset() {
		return Datasets.sortedDatasetOfId(CalciteServerModule.CALCITE_SINGLE_DUMMY_DATASET, Record.class, Record.class, Function.identity(), RecordKeyComparator.getInstance());
	}

	public static RelDataType toRowType(RelDataTypeFactory typeFactory, Type type) {
		if (type instanceof Class<?> cls) {
			if (cls == String.class) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
			}
			if (cls == Timestamp.class) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
			}
			if (cls == Time.class) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
			}
			if (cls == Date.class) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
			}
			if (cls.isPrimitive() || cls.isEnum() || Primitives.isWrapperType(cls)) {
				return typeFactory.createJavaType(cls);
			}

			if (cls.isRecord()) {
				RecordComponent[] recordComponents = cls.getRecordComponents();
				List<RelDataTypeField> fields = new ArrayList<>();

				for (RecordComponent recordComponent : recordComponents) {
					fields.add(new RelDataTypeFieldImpl(
							recordComponent.getName(),
							fields.size(),
							toRowType(typeFactory, recordComponent.getGenericType())
					));
				}

				return new JavaRecordType(fields, cls);
			}

			if (cls.isArray()) {
				RelDataType elementType = toRowType(typeFactory, cls.getComponentType());
				return typeFactory.createArrayType(elementType, -1);
			}

			List<RelDataTypeField> fields = new ArrayList<>();

			for (Field field : cls.getFields()) {
				if (Modifier.isStatic(field.getModifiers())) continue;

				fields.add(new RelDataTypeFieldImpl(
						field.getName(),
						fields.size(),
						toRowType(typeFactory, field.getGenericType())
				));
			}
			return new JavaRecordType(fields, cls);
		}

		if (type instanceof ParameterizedType parameterizedType) {
			Type rawType = parameterizedType.getRawType();
			if (rawType == List.class) {
				RelDataType elementType = toRowType(typeFactory, parameterizedType.getActualTypeArguments()[0]);
				return typeFactory.createArrayType(elementType, -1);
			}
			if (rawType == Set.class) {
				RelDataType elementType = toRowType(typeFactory, parameterizedType.getActualTypeArguments()[0]);
				return typeFactory.createMultisetType(elementType, -1);
			}
			if (rawType == Map.class) {
				Type[] typeArguments = parameterizedType.getActualTypeArguments();
				RelDataType keyType = toRowType(typeFactory, typeArguments[0]);
				RelDataType valueType = toRowType(typeFactory, typeArguments[1]);
				return typeFactory.createMapType(keyType, valueType);
			}
		}

		if (type instanceof GenericArrayType genericArrayType) {
			RelDataType elementType = toRowType(typeFactory, genericArrayType.getGenericComponentType());
			return typeFactory.createArrayType(elementType, -1);
		}

		throw new ToDoException();
	}
}
