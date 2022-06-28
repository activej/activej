package io.activej.dataflow.calcite;

import io.activej.codegen.DefiningClassLoader;
import io.activej.codegen.expression.Expression;
import io.activej.common.Checks;
import io.activej.record.Record;
import io.activej.record.RecordProjection;
import io.activej.record.RecordScheme;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeClass;
import io.activej.serializer.annotations.SerializeNullable;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.common.Checks.checkArgument;

public final class RecordProjectionFn implements Function<Record, Record> {
	private static final boolean CHECK = Checks.isEnabled(RecordProjectionFn.class);

	private final List<FieldProjection> fieldProjections;

	private @Nullable RecordProjection projection;

	public RecordProjectionFn(@Deserialize("fieldProjections") List<FieldProjection> fieldProjections) {
		this.fieldProjections = new ArrayList<>(fieldProjections);
	}

	@Override
	public Record apply(Record record) {
		if (projection == null) {
			RecordScheme original = record.getScheme();
			DefiningClassLoader classLoader = record.getScheme().getClassLoader();
			RecordScheme schemeTo = RecordScheme.create(classLoader);
			Map<String, UnaryOperator<Expression>> mapping = new HashMap<>();
			for (FieldProjection fieldProjection : fieldProjections) {
				String fieldName = fieldProjection.getFieldName(original);
				schemeTo.withField(fieldName, fieldProjection.getFieldType(original));
				UnaryOperator<Expression> previous = mapping.put(fieldName, fieldProjection.getMapping(original));
				if (previous != null) {
					throw new IllegalArgumentException();
				}
			}
			projection = RecordProjection.projection(original, schemeTo, mapping);
		}

		if (CHECK) {
			checkArgument(record.getScheme().equals(projection.getSchemeFrom()));
		}

		return projection.apply(record);
	}

	@Serialize(order = 1)
	public List<FieldProjection> getFieldProjections() {
		return new ArrayList<>(fieldProjections);
	}

	@SerializeClass(subclasses = {FieldProjectionAsIs.class, FieldProjectionConstant.class, FieldProjectionMapGet.class, FieldProjectionListGet.class})
	public interface FieldProjection {
		String getFieldName(RecordScheme original);

		Type getFieldType(RecordScheme original);

		UnaryOperator<Expression> getMapping(RecordScheme original);
	}

	public static final class FieldProjectionAsIs implements FieldProjection {
		@Serialize(order = 1)
		public final int index;

		public FieldProjectionAsIs(@Deserialize("index") int index) {
			this.index = index;
		}

		@Override
		public String getFieldName(RecordScheme original) {
			return original.getField(index);
		}

		@Override
		public Type getFieldType(RecordScheme original) {
			return original.getFieldType(index);
		}

		@Override
		public UnaryOperator<Expression> getMapping(RecordScheme original) {
			String fieldName = getFieldName(original);
			return recordFrom -> original.property(recordFrom, fieldName);
		}
	}

	public static final class FieldProjectionMapGet implements FieldProjection {
		@Serialize(order = 1)
		@SerializeNullable
		public final String fieldName;
		@Serialize(order = 2)
		public final int mapIndex;
		@Serialize(order = 3)
		@SerializeClass(subclasses = {Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Character.class, Boolean.class, String.class})
		public final Object key;

		public FieldProjectionMapGet(@Deserialize("fieldName") String fieldName, @Deserialize("mapIndex") int mapIndex, @Deserialize("key") Object key) {
			this.fieldName = fieldName;
			this.mapIndex = mapIndex;
			this.key = key;
		}

		@Override
		public String getFieldName(RecordScheme original) {
			if (fieldName != null) return fieldName;

			return original.getField(mapIndex) + ".get(" + key.toString() + ")";
		}

		@Override
		public Type getFieldType(RecordScheme original) {
			return ((ParameterizedType) original.getFieldType(mapIndex)).getActualTypeArguments()[1];
		}

		@Override
		public UnaryOperator<Expression> getMapping(RecordScheme original) {
			String mapField = original.getField(mapIndex);
			return recordFrom -> call(cast(original.property(recordFrom, mapField), Map.class), "get", value(key));
		}
	}

	public static final class FieldProjectionListGet implements FieldProjection {
		@Serialize(order = 1)
		@SerializeNullable
		public final String fieldName;
		@Serialize(order = 2)
		public final int listIndex;
		@Serialize(order = 3)
		public final int key;

		public FieldProjectionListGet(@Deserialize("fieldName") String fieldName, @Deserialize("listIndex") int listIndex, @Deserialize("key") int key) {
			this.fieldName = fieldName;
			this.listIndex = listIndex;
			this.key = key;
		}

		@Override
		public String getFieldName(RecordScheme original) {
			if (fieldName != null) return fieldName;

			return original.getField(listIndex) + ".get(" + key + ")";
		}

		@Override
		public Type getFieldType(RecordScheme original) {
			return ((ParameterizedType) original.getFieldType(listIndex)).getActualTypeArguments()[0];
		}

		@Override
		public UnaryOperator<Expression> getMapping(RecordScheme original) {
			String listField = original.getField(listIndex);
			return recordFrom -> let(cast(original.property(recordFrom, listField), List.class), list ->
					ifGe(value(key), call(list, "size"),
							nullRef(Object.class),
							call(list, "get", value(key))));
		}
	}

	public static final class FieldProjectionConstant implements FieldProjection {
		@Serialize(order = 1)
		public final String fieldName;
		@Serialize(order = 2)
		@SerializeClass(subclasses = {Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Character.class, Boolean.class, String.class})
		public final Object constant;

		public FieldProjectionConstant(@Deserialize("fieldName") String fieldName, @Deserialize("constant") Object constant) {
			this.fieldName = fieldName;
			this.constant = constant;
		}

		@Override
		public String getFieldName(RecordScheme original) {
			return fieldName;
		}

		@Override
		public Type getFieldType(RecordScheme original) {
			return constant.getClass();
		}

		@Override
		public UnaryOperator<Expression> getMapping(RecordScheme original) {
			return $ -> value(constant);
		}
	}
}
