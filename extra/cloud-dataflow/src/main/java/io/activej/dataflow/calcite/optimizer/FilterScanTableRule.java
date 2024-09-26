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
package io.activej.dataflow.calcite.optimizer;

import io.activej.dataflow.calcite.rel.DataflowTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;

public class FilterScanTableRule extends RelRule<FilterScanTableRule.Config> {

	private FilterScanTableRule(FilterScanTableRule.Config config) {
		super(config);
	}

	public static FilterScanTableRule create() {
		return new FilterScanTableRule(Config.INSTANCE);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		LogicalFilter filter = call.rel(0);
		DataflowTableScan scan = call.rel(1);

		if (scan.getCondition() != null) return;

		RexNode condition = filter.getCondition();
		if (condition.isAlwaysTrue()) return;

		scan.setCondition(condition);

		call.transformTo(scan);
	}

	public static final class Config extends LimitedConfig {
		private static final Config INSTANCE = new Config();

		@Override
		public RelOptRule toRule() {
			return new FilterScanTableRule(this);
		}

		@Override
		public OperandTransform operandSupplier() {
			return b -> b.operand(LogicalFilter.class)
				.oneInput(b1 -> b1.operand(DataflowTableScan.class).noInputs());
		}
	}
}
