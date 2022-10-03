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
package io.activej.dataflow.calcite;

import io.activej.dataflow.calcite.rel.FilterableTableScan;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.function.Function;

import static io.activej.dataflow.calcite.utils.Utils.toRowType;

public class DataflowTable extends AbstractTable implements TranslatableTable {
	private final String tableName;
	private final Class<?> type;
	private final RecordFunction<?> recordFunction;
	private final Function<RelDataTypeFactory, RelDataType> relDataTypeFactory;

	private RelDataType relDataType;

	protected DataflowTable(String tableName, Class<?> type, Function<RelDataTypeFactory, RelDataType> relDataTypeFactory, RecordFunction<?> recordFunction) {
		this.tableName = tableName;
		this.type = type;
		this.recordFunction = recordFunction;
		this.relDataTypeFactory = relDataTypeFactory;
	}

	public static <T> DataflowTable create(String tableName, Class<T> cls, RecordFunction<T> recordFunction) {
		return new DataflowTable(tableName, cls, typeFactory -> toRowType(typeFactory, cls), recordFunction);
	}

	public static <T> DataflowTable create(String tableName, Class<T> cls, Function<RelDataTypeFactory, RelDataType> relDataTypeFactory, RecordFunction<T> recordFunction) {
		return new DataflowTable(tableName, cls, relDataTypeFactory, recordFunction);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (relDataType == null) {
			relDataType = relDataTypeFactory.apply(typeFactory);
		}
		return relDataType;
	}

	public String getTableName() {
		return tableName;
	}

	public Class<?> getType() {
		return type;
	}

	public RecordFunction<?> getRecordFunction() {
		return recordFunction;
	}

	@Override
	public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		return FilterableTableScan.create(context.getCluster(), relOptTable, context.getTableHints());
	}
}
