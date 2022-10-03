package io.activej.dataflow.calcite;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.utils.JavaRecordType;
import io.activej.dataflow.calcite.utils.Utils;
import io.activej.datastream.processor.StreamReducers;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.record.RecordSetter;
import io.activej.types.TypeT;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;
import static io.activej.inject.util.Utils.checkState;

public final class DataflowTableBuilder<T> {
	private final DefiningClassLoader classLoader;
	private final String tableName;
	private final Class<T> type;
	private final LinkedHashMap<String, ColumnEntry> columns = new LinkedHashMap<>();
	private final @Nullable Reducer<Record, Record, Record, ?> reducer;

	private int[] primaryKeyIndexes = new int[0];

	private DataflowTableBuilder(DefiningClassLoader classLoader, String tableName, Class<T> type, @Nullable Reducer<Record, Record, Record, ?> reducer) {
		this.tableName = tableName;
		this.classLoader = classLoader;
		this.type = type;
		this.reducer = reducer;
	}

	public static <T> DataflowTableBuilder<T> create(DefiningClassLoader classLoader, String tableName, Class<T> type) {
		return new DataflowTableBuilder<>(classLoader, tableName, type, null);
	}

	public static <T> DataflowTableBuilder<T> createPartitioned(DefiningClassLoader classLoader, String tableName, Class<T> type) {
		return new DataflowTableBuilder<>(classLoader, tableName, type, StreamReducers.deduplicateReducer());
	}

	public static <T> DataflowTableBuilder<T> createPartitioned(DefiningClassLoader classLoader, String tableName, Class<T> type, Reducer<Record, Record, Record, ?> reducer) {
		return new DataflowTableBuilder<>(classLoader, tableName, type, reducer);
	}

	public <C> DataflowTableBuilder<T> withColumn(String columnName, Class<C> columnType, Function<T, C> getter) {
		addColumn(columnName, columnType, getter, relDataTypeFactory -> Utils.toRowType(relDataTypeFactory, columnType));
		return this;
	}

	public <C> DataflowTableBuilder<T> withColumn(String columnName, Class<C> columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
		addColumn(columnName, columnType, getter, typeFactory);
		return this;
	}

	public <C> DataflowTableBuilder<T> withColumn(String columnName, TypeT<C> columnType, Function<T, C> getter) {
		addColumn(columnName, columnType.getType(), getter, relDataTypeFactory -> Utils.toRowType(relDataTypeFactory, columnType.getType()));
		return this;
	}

	public <C> DataflowTableBuilder<T> withColumn(String columnName, TypeT<C> columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
		addColumn(columnName, columnType.getType(), getter, typeFactory);
		return this;
	}

	private <C> void addColumn(String columnName, Type columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
		ColumnEntry columnEntry = new ColumnEntry(
				columnType,
				getter,
				typeFactory
		);

		ColumnEntry old = columns.put(columnName, columnEntry);

		checkArgument(old == null, "Column '" + columnName + "' was already added");
	}

	public DataflowTableBuilder<T> withPrimaryKeyIndexes(int... primaryKeyIndexes) {
		checkState(reducer != null, "Primary key indexes only apply to partitioned tables");
		this.primaryKeyIndexes = primaryKeyIndexes;
		return this;
	}

	public DataflowTable build() {
		RecordFunction<T> recordFunction = createRecordFunction(classLoader);
		Function<RelDataTypeFactory, RelDataType> typeFactory = createTypeFactory();

		return reducer == null ?
				DataflowTable.create(tableName, type, typeFactory, recordFunction) :
				DataflowPartitionedTable.create(tableName, type, typeFactory, recordFunction)
						.withReducer(reducer)
						.withPrimaryKeyIndexes(primaryKeyIndexes);
	}

	private Function<RelDataTypeFactory, RelDataType> createTypeFactory() {
		return relDataTypeFactory -> {
			List<RelDataTypeField> fields = new ArrayList<>(columns.size());

			for (Map.Entry<String, ColumnEntry> entry : columns.entrySet()) {
				fields.add(new RelDataTypeFieldImpl(
						entry.getKey(),
						fields.size(),
						entry.getValue().typeFactory.apply(relDataTypeFactory))
				);
			}

			return new JavaRecordType(fields, type);
		};
	}

	private RecordFunction<T> createRecordFunction(DefiningClassLoader classLoader) {
		RecordScheme scheme = RecordScheme.create(classLoader);

		for (Map.Entry<String, ColumnEntry> entry : columns.entrySet()) {
			scheme.addField(entry.getKey(), entry.getValue().type);
		}

		RecordScheme finalScheme = scheme.withComparator(scheme.getFields()).build();

		Object[] gettersAndSetters = new Object[finalScheme.size() * 2];
		Iterator<ColumnEntry> entryIterator = columns.values().iterator();
		for (int i = 0, j = 0; i < gettersAndSetters.length; ) {
			assert entryIterator.hasNext();
			gettersAndSetters[i++] = entryIterator.next().getter;
			gettersAndSetters[i++] = finalScheme.setter(j++);
		}
		assert !entryIterator.hasNext();

		return new RecordFunction<>() {
			@Override
			public RecordScheme getScheme() {
				return finalScheme;
			}

			@Override
			@SuppressWarnings("unchecked")
			public Record apply(T t) {
				Record record = finalScheme.record();

				for (int i = 0; i < gettersAndSetters.length; ) {
					Function<T, Object> getter = (Function<T, Object>) gettersAndSetters[i++];
					RecordSetter<Object> setter = (RecordSetter<Object>) gettersAndSetters[i++];
					setter.set(record, getter.apply(t));
				}

				return record;
			}
		};
	}

	private final class ColumnEntry {
		private final Type type;
		private final Function<T, ?> getter;
		private final Function<RelDataTypeFactory, RelDataType> typeFactory;

		private ColumnEntry(Type type, Function<T, ?> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
			this.type = type;
			this.getter = getter;
			this.typeFactory = typeFactory;
		}
	}
}
