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
package io.activej.dataflow;

import io.activej.dataflow.calcite.DataflowTable;
import io.activej.dataflow.calcite.RecordFunction;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.record.Record;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class DataflowPartitionedTable<T> extends DataflowTable<T> {
	private final Set<Integer> primaryKeyIndexes = new HashSet<>();

	private @Nullable Reducer<Record, Record, Record, Record> reducer;

	private DataflowPartitionedTable(Class<T> type, Function<RelDataTypeFactory, RelDataType> relDataTypeFactory, RecordFunction<T> recordFunction) {
		super(type, relDataTypeFactory, recordFunction);
	}

	public static <T> DataflowPartitionedTable<T> create(Class<T> cls, RecordFunction<T> recordFunction) {
		return new DataflowPartitionedTable<>(cls, typeFactory -> toRowType(typeFactory, cls), recordFunction);
	}

	public static <T> DataflowPartitionedTable<T> create(Class<T> type, Function<RelDataTypeFactory, RelDataType> relDataTypeFactory, RecordFunction<T> recordFunction) {
		return new DataflowPartitionedTable<>(type, relDataTypeFactory, recordFunction);
	}

	public DataflowPartitionedTable<T> withPrimaryKeyIndexes(int... indexes) {
		primaryKeyIndexes.addAll(Arrays.stream(indexes).boxed().toList());
		return this;
	}

	public DataflowPartitionedTable<T> withReducer(Reducer<Record, Record, Record, Record> reducer) {
		this.reducer = reducer;
		return this;
	}

	public Set<Integer> getPrimaryKeyIndexes() {
		return Collections.unmodifiableSet(primaryKeyIndexes);
	}

	public @Nullable Reducer<Record, Record, Record, Record> getReducer() {
		return reducer;
	}
}
