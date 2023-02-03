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
import io.activej.dataflow.calcite.RecordFunction;
import io.activej.dataflow.calcite.utils.Utils;
import io.activej.datastream.processor.reducer.Reducers;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.types.TypeT;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.lang.reflect.Type;
import java.util.*;
import java.util.function.Function;

public final class DataflowPartitionedTable<T> extends AbstractDataflowTable<T> {
	private final Set<Integer> primaryKeyIndexes = new HashSet<>();

	private final Reducer<Record, Record, Record, ?> reducer;

	private DataflowPartitionedTable(
			String tableName,
			Class<T> type,
			Function<RelDataTypeFactory, RelDataType> relDataTypeFactory,
			RecordFunction<T> recordFunction,
			Reducer<Record, Record, Record, ?> reducer,
			Collection<Integer> primaryKeyIndexes
	) {
		super(tableName, type, relDataTypeFactory, recordFunction);
		this.reducer = reducer;
		this.primaryKeyIndexes.addAll(primaryKeyIndexes);
	}

	public static <T> Builder<T> builder(DefiningClassLoader classLoader, String tableName, Class<T> type) {
		return new Builder<>(classLoader, tableName, type);
	}

	public static final class Builder<T> extends AbstractDataflowTable.Builder<Builder<T>, DataflowPartitionedTable<T>, T> {
		private final List<Integer> primaryKeyIndexes = new ArrayList<>();
		private Function<RecordScheme, Reducer<Record, Record, Record, ?>> reducerFactory = $ -> Reducers.deduplicateReducer();

		private Builder(DefiningClassLoader classLoader, String tableName, Class<T> type) {
			super(classLoader, tableName, type);
		}

		public <A> Builder<T> withReducer(Reducer<Record, Record, Record, A> reducer) {
			this.reducerFactory = $ -> reducer;
			return this;
		}

		public <A> Builder<T> withReducerFactory(Function<RecordScheme, Reducer<Record, Record, Record, A>> reducerFactory) {
			this.reducerFactory = reducerFactory::apply;
			return this;
		}

		public <C> Builder<T> withKeyColumn(String columnName, Class<C> columnType, Function<T, C> getter) {
			addKeyColumn(columnName, columnType, getter, relDataTypeFactory -> Utils.toRowType(relDataTypeFactory, columnType));
			return this;
		}

		public <C> Builder<T> withKeyColumn(String columnName, Class<C> columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
			addKeyColumn(columnName, columnType, getter, typeFactory);
			return this;
		}

		public <C> Builder<T> withKeyColumn(String columnName, TypeT<C> columnType, Function<T, C> getter) {
			addKeyColumn(columnName, columnType.getType(), getter, relDataTypeFactory -> Utils.toRowType(relDataTypeFactory, columnType.getType()));
			return this;
		}

		public <C> Builder<T> withKeyColumn(String columnName, TypeT<C> columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
			addKeyColumn(columnName, columnType.getType(), getter, typeFactory);
			return this;
		}

		private <C> void addKeyColumn(String columnName, Type columnType, Function<T, C> getter, Function<RelDataTypeFactory, RelDataType> typeFactory) {
			int index = columns.size();
			addColumn(columnName, columnType, getter, typeFactory);
			primaryKeyIndexes.add(index);
		}

		@Override
		protected DataflowPartitionedTable<T> buildTable(RecordFunction<T> recordFunction, Function<RelDataTypeFactory, RelDataType> typeFactory) {
			Reducer<Record, Record, Record, ?> reducer = reducerFactory.apply(recordFunction.getScheme());

			return new DataflowPartitionedTable<>(tableName, type, typeFactory, recordFunction, reducer, primaryKeyIndexes);
		}
	}

	public Set<Integer> getPrimaryKeyIndexes() {
		return Collections.unmodifiableSet(primaryKeyIndexes);
	}

	public Reducer<Record, Record, Record, ?> getReducer() {
		return reducer;
	}
}
