package io.activej.dataflow.calcite.utils;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.ToDoException;
import io.activej.dataflow.calcite.RecordStreamSchema;
import io.activej.dataflow.calcite.Value;
import io.activej.dataflow.calcite.function.ProjectionFunction;
import io.activej.dataflow.calcite.inject.CalciteServerModule;
import io.activej.dataflow.calcite.operand.*;
import io.activej.dataflow.dataset.Datasets;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.dataflow.graph.StreamSchema;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.types.Primitives;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

public final class Utils {

	private static final StreamSchema<Record> EMPTY_STREAM_SCHEME = RecordStreamSchema.create(RecordScheme.builder().build());

	private static final StreamCodec<BigDecimal> BIG_DECIMAL_STREAM_CODEC = StreamCodec.create((scale, bytes) -> {
				BigInteger unscaledValue = new BigInteger(bytes);
				return new BigDecimal(unscaledValue, scale);
			},
			BigDecimal::scale, StreamCodecs.ofVarInt(),
			bigDecimal -> bigDecimal.unscaledValue().toByteArray(), StreamCodecs.ofByteArray()
	);

	private static final StreamCodec<LocalDate> LOCAL_DATE_STREAM_CODEC = StreamCodec.create(LocalDate::of,
			LocalDate::getYear, StreamCodecs.ofVarInt(),
			LocalDate::getMonthValue, StreamCodecs.ofVarInt(),
			LocalDate::getDayOfMonth, StreamCodecs.ofVarInt()
	);

	private static final StreamCodec<LocalTime> LOCAL_TIME_STREAM_CODEC = StreamCodec.create(LocalTime::of,
			LocalTime::getHour, StreamCodecs.ofVarInt(),
			LocalTime::getMinute, StreamCodecs.ofVarInt(),
			LocalTime::getSecond, StreamCodecs.ofVarInt(),
			LocalTime::getNano, StreamCodecs.ofVarInt()
	);

	private static final StreamCodec<Instant> INSTANT_STREAM_CODEC = StreamCodec.create(Instant::ofEpochSecond,
			Instant::getEpochSecond, StreamCodecs.ofVarLong(),
			Instant::getNano, StreamCodecs.ofVarInt()
	);

	private static final LinkedHashMap<Class<?>, StreamCodec<?>> VALUE_CODECS = new LinkedHashMap<>();

	static {
		addValueCodec(BigDecimal.class, BIG_DECIMAL_STREAM_CODEC);
		addValueCodec(String.class, StreamCodecs.ofString());
		addValueCodec(Boolean.class, StreamCodecs.ofBoolean());
		addValueCodec(Integer.class, StreamCodecs.ofInt());
		addValueCodec(Long.class, StreamCodecs.ofLong());
		addValueCodec(Float.class, StreamCodecs.ofFloat());
		addValueCodec(Double.class, StreamCodecs.ofDouble());
		addValueCodec(LocalDate.class, LOCAL_DATE_STREAM_CODEC);
		addValueCodec(LocalTime.class, LOCAL_TIME_STREAM_CODEC);
		addValueCodec(Instant.class, INSTANT_STREAM_CODEC);
	}

	private static <T> void addValueCodec(Class<T> cls, StreamCodec<T> codec) {
		VALUE_CODECS.put(cls, codec);
	}

	private static final StreamCodec<Object> VALUE_OBJECT_STREAM_CODEC = StreamCodecs.ofSubtype(VALUE_CODECS);

	public static final StreamCodec<Value> VALUE_STREAM_CODEC = StreamCodec.of(
			(output, item) -> VALUE_OBJECT_STREAM_CODEC.encode(output, item.getValue()),
			input -> {
				Object decoded = VALUE_OBJECT_STREAM_CODEC.decode(input);
				return Value.materializedValue(decoded.getClass(), decoded);
			}
	);

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	public static <T> T toJavaType(RexLiteral literal) {
		SqlTypeName typeName = literal.getTypeName();
		if (typeName.getFamily() == SqlTypeFamily.CHARACTER) {
			return (T) literal.getValueAs(String.class);
		}
		if (typeName.getFamily() == SqlTypeFamily.DATE) {
			Integer daysSinceEpoch = literal.getValueAs(Integer.class);
			return (T) LocalDate.ofEpochDay(daysSinceEpoch);
		}
		if (typeName.getFamily() == SqlTypeFamily.TIME) {
			Integer millisOfDay = literal.getValueAs(Integer.class);
			assert millisOfDay % 1000 == 0; // Millis are ignored
			int secondsOfDay = millisOfDay / 1000;
			return (T) LocalTime.ofSecondOfDay(secondsOfDay);
		}
		if (typeName.getFamily() == SqlTypeFamily.TIMESTAMP) {
			Long epochMillis = literal.getValueAs(Long.class);
			return (T) Instant.ofEpochMilli(epochMillis);
		}

		return (T) literal.getValue();
	}

	public static Operand<?> toOperand(RexNode node, DefiningClassLoader classLoader) {
		if (node instanceof RexDynamicParam dynamicParam) {
			return new Operand_Scalar(Value.unmaterializedValue(dynamicParam));
		} else if (node instanceof RexCall call) {
			switch (call.getKind()) {
				case CAST -> {
					int castType = call.getType().getSqlTypeName().getJdbcOrdinal();
					Operand<?> operand = toOperand(call.getOperands().get(0), classLoader);
					return new Operand_Cast(operand, castType);
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
			return new Operand_Scalar(value);
		} else if (node instanceof RexInputRef inputRef) {
			return new Operand_RecordField(inputRef.getIndex());
		} else if (node instanceof RexFieldAccess fieldAccess) {
			Operand<?> objectOperand = toOperand(fieldAccess.getReferenceExpr(), classLoader);
			return new Operand_FieldAccess(objectOperand, fieldAccess.getField().getName(), classLoader);
		}
		throw new IllegalArgumentException("Unknown node: " + node);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public static int compareToUnknown(Comparable left, Comparable right) {
		left = tryCoerceEnum(left, right);
		right = tryCoerceEnum(right, left);

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
		object1 = tryCoerceEnum(object1, object2);
		object2 = tryCoerceEnum(object2, object1);

		if (object1.getClass() != object2.getClass() &&
				object1 instanceof Number number1 && object2 instanceof Number number2) {
			return Double.compare(number1.doubleValue(), number2.doubleValue()) == 0;
		}

		return object1.equals(object2);
	}

	private static <T> T tryCoerceEnum(T from, T to) {
		if (from instanceof String aString && to instanceof Enum<?> anEnum) {
			//noinspection unchecked
			return (T) Enum.valueOf(anEnum.getClass(), aString);
		}
		return from;
	}

	public static SortedDataset<Record, Record> singleDummyDataset() {
		return Datasets.sortedDatasetOfId(CalciteServerModule.CALCITE_SINGLE_DUMMY_DATASET, EMPTY_STREAM_SCHEME, Record.class, IdentityFunction.getInstance(), RecordKeyComparator.getInstance());
	}

	public static RelDataType toRowType(RelDataTypeFactory typeFactory, Type type) {
		if (type instanceof Class<?> cls) {
			if (cls == String.class) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
			}
			if (cls == Instant.class) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
			}
			if (cls == LocalTime.class) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIME), true);
			}
			if (cls == LocalDate.class) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DATE), true);
			}
			if (cls.isEnum()) {
				return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
			}
			if (cls.isPrimitive() || Primitives.isWrapperType(cls)) {
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

	public static boolean isSortable(Class<?> cls) {
		return cls.isPrimitive() || Comparable.class.isAssignableFrom(cls);
	}

	private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
			.parseCaseInsensitive()
			.append(ISO_LOCAL_DATE)
			.appendLiteral(' ')
			.append(ISO_LOCAL_TIME)
			.toFormatter();

	public static Instant parseInstantFromTimestampString(String timestampString) {
		TemporalAccessor parsed = DATE_TIME_FORMATTER.parse(timestampString);
		return LocalDateTime.from(parsed).toInstant(ZoneOffset.UTC);
	}
}
