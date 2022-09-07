package io.activej.dataflow.calcite.rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;

public class FilterableTableScan extends TableScan {
	private @Nullable RexNode condition;

	private FilterableTableScan(RelOptCluster cluster, RelTraitSet traitSet,
			RelOptTable table, List<RelHint> hints, @Nullable RexNode condition) {
		super(cluster, traitSet, hints, table);
		this.condition = condition;
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		assert traitSet.containsIfApplicable(Convention.NONE);
		assert inputs.isEmpty();
		return this;
	}

	public static FilterableTableScan create(RelOptCluster cluster,
			final RelOptTable relOptTable, List<RelHint> hints) {
		final Table table = relOptTable.unwrap(Table.class);
		final RelTraitSet traitSet =
				cluster.traitSetOf(Convention.NONE)
						.replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
							if (table != null) {
								return table.getStatistic().getCollations();
							}
							return ImmutableList.of();
						});
		return new FilterableTableScan(cluster, traitSet, relOptTable, hints, null);
	}

	@Override
	public RelNode withHints(List<RelHint> hintList) {
		return new FilterableTableScan(getCluster(), traitSet, table, hintList, condition);
	}

	public void setCondition(@NotNull RexNode condition) {
		this.condition = condition;
	}

	public @Nullable RexNode getCondition() {
		return condition;
	}

	@Override
	public RelWriter explainTerms(RelWriter pw) {
		return super.explainTerms(pw)
				.item("condition", condition);
	}

	@Override
	public boolean deepEquals(Object obj) {
		return super.deepEquals(obj)
				&& Objects.equals(condition, ((FilterableTableScan) obj).condition);
	}

	@Override
	public int deepHashCode() {
		return super.deepHashCode() + Objects.hashCode(condition);
	}
}
