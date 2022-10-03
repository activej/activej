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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.NameSet;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static io.activej.common.Checks.checkArgument;

public final class DataflowSchema extends AbstractSchema {
	private final Map<String, DataflowTable> tableMap = new TreeMap<>(NameSet.COMPARATOR);

	private DataflowSchema() {
		super();
	}

	public static DataflowSchema create() {
		return new DataflowSchema();
	}

	public DataflowSchema withTable(DataflowTable table) {
		DataflowTable prev = tableMap.put(table.getTableName(), table);
		checkArgument(prev == null, "Duplicate table names: " + table.getTableName());
		return this;
	}

	public DataflowSchema withTables(Collection<DataflowTable> tables) {
		for (DataflowTable table : tables) {
			String tableName = table.getTableName();
			DataflowTable prev = tableMap.put(tableName, table);

			checkArgument(prev == null, "Duplicate table names: " + tableName);
		}
		return this;
	}

	@Override
	protected Map<String, Table> getTableMap() {
		return Collections.unmodifiableMap(tableMap);
	}

	public Map<String, DataflowTable> getDataflowTableMap() {
		return Collections.unmodifiableMap(tableMap);
	}
}
