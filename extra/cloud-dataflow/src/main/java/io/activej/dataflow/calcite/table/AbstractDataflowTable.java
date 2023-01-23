/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.activej.dataflow.calcite.table;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.builder.AbstractBuilder;
import io.activej.dataflow.calcite.RecordFunction;
import io.activej.dataflow.calcite.rel.DataflowTableScan;
import io.activej.dataflow.calcite.utils.JavaRecordType;
import io.activej.dataflow.calcite.utils.Utils;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.record.RecordSetter;
import io.activej.types.TypeT;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;

public abstract class AbstractDataflowTable<T> extends AbstractTable implements TranslatableTable {
	private final String tableName;
	private final Class<T> type;
	private final RecordFunction<T> recordFunction;
	private final Function<RelDataTypeFactory, RelDataType> relDataTypeFactory;

	private RelDataType relDataType;

	protected AbstractDataflowTable(String tableName, Class<T> type, Function<RelDataTypeFactory, RelDataType> relDataTypeFactory, RecordFunction<T> recordFunction) {
		this.tableName = tableName;
		this.type = type;
		this.recordFunction = recordFunction;
		this.relDataTypeFactory = relDataTypeFactory;
	}

	@SuppressWarnings("unchecked")
	public abstract static class Builder<Self extends Builder<Self, DT, T>, DT extends AbstractDataflowTable<T>, T>
			extends AbstractBuilder<Self, DT> {
		protected final DefiningClassLoader classLoader;
		protected final String tableName;
		protected final Class<T> type;
		protected final LinkedHashMap<String, ColumnEntry<?>> columns = new LinkedHashMap<>();

		protected Builder(DefiningClassLoader classLoader, String tableName, Class<T> type) {
			this.tableName = tableName;
			this.classLoader = classLoader;
			this.type = type;
		}

		public final <C> Self withColumn(String columnName, Class<C> columnType, Function<T, C> getter) {
			checkNotBuilt(this);
			addColumn(columnName, columnType, getter, relDataTypeFactory -> Utils.toRowType(relDataTypeFactory, columnType));
			return (Self) this;
		}

		public final <C> Self withColumn(String columnName, Class<C> columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
			checkNotBuilt(this);
			addColumn(columnName, columnType, getter, typeFactory);
			return (Self) this;
		}

		public final <C> Self withColumn(String columnName, TypeT<C> columnType, Function<T, C> getter) {
			checkNotBuilt(this);
			addColumn(columnName, columnType.getType(), getter, relDataTypeFactory -> Utils.toRowType(relDataTypeFactory, columnType.getType()));
			return (Self) this;
		}

		public final <C> Self withColumn(String columnName, TypeT<C> columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
			checkNotBuilt(this);
			addColumn(columnName, columnType.getType(), getter, typeFactory);
			return (Self) this;
		}

		protected final <C> void addColumn(String columnName, Type columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
			ColumnEntry<C> columnEntry = new ColumnEntry<>(
					columnType,
					getter,
					typeFactory
			);

			ColumnEntry<?> old = columns.put(columnName, columnEntry);

			checkArgument(old == null, "Column '" + columnName + "' was already added");
		}

		private Function<RelDataTypeFactory, RelDataType> createTypeFactory() {
			return relDataTypeFactory -> {
				List<RelDataTypeField> fields = new ArrayList<>(columns.size());

				for (Map.Entry<String, ColumnEntry<?>> entry : columns.entrySet()) {
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
			RecordScheme.Builder schemeBuilder = RecordScheme.builder(classLoader);

			for (Map.Entry<String, ColumnEntry<?>> entry : columns.entrySet()) {
				schemeBuilder.withField(entry.getKey(), entry.getValue().type);
			}

			RecordScheme finalScheme = schemeBuilder.withComparatorFields(new ArrayList<>(columns.keySet())).build();

			Object[] gettersAndSetters = new Object[finalScheme.size() * 2];
			Iterator<ColumnEntry<?>> entryIterator = columns.values().iterator();
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

		@Override
		protected final DT doBuild() {
			RecordFunction<T> recordFunction = createRecordFunction(classLoader);
			Function<RelDataTypeFactory, RelDataType> typeFactory = createTypeFactory();

			return buildTable(recordFunction, typeFactory);
		}

		protected abstract DT buildTable(RecordFunction<T> recordFunction, Function<RelDataTypeFactory, RelDataType> typeFactory);

		private final class ColumnEntry<C> {
			private final Type type;
			private final Function<T, C> getter;
			private final Function<RelDataTypeFactory, RelDataType> typeFactory;

			private ColumnEntry(Type type, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
				this.type = type;
				this.getter = getter;
				this.typeFactory = typeFactory;
			}
		}
	}

	@Override
	public final RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (relDataType == null) {
			relDataType = relDataTypeFactory.apply(typeFactory);
		}
		return relDataType;
	}

	public final String getTableName() {
		return tableName;
	}

	public final Class<T> getType() {
		return type;
	}

	public final RecordFunction<T> getRecordFunction() {
		return recordFunction;
	}

	@Override
	public final RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		return DataflowTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
	}
}
