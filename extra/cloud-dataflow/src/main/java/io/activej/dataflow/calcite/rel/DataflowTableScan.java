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

public class DataflowTableScan extends TableScan {
	private @Nullable RexNode condition;

	private @Nullable RexNode offset;
	private @Nullable RexNode limit;

	private DataflowTableScan(RelOptCluster cluster, RelTraitSet traitSet,
			RelOptTable table, List<RelHint> hints, @Nullable RexNode condition,
			@Nullable RexNode offset, @Nullable RexNode limit) {
		super(cluster, traitSet, hints, table);
		this.condition = condition;
		this.offset = offset;
		this.limit = limit;
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		assert traitSet.containsIfApplicable(Convention.NONE);
		assert inputs.isEmpty();
		return this;
	}

	public static DataflowTableScan create(RelOptCluster cluster,
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
		return new DataflowTableScan(cluster, traitSet, relOptTable, hints, null, null, null);
	}

	@Override
	public RelNode withHints(List<RelHint> hintList) {
		return new DataflowTableScan(getCluster(), traitSet, table, hintList, condition, offset, limit);
	}

	public void setCondition(@NotNull RexNode condition) {
		this.condition = condition;
	}

	public @Nullable RexNode getCondition() {
		return condition;
	}

	public void setOffset(@NotNull RexNode offset) {
		this.offset = offset;
	}

	public @Nullable RexNode getOffset() {
		return offset;
	}

	public void setLimit(@NotNull RexNode limit) {
		this.limit = limit;
	}

	public @Nullable RexNode getLimit() {
		return limit;
	}

	@Override
	public RelWriter explainTerms(RelWriter pw) {
		return super.explainTerms(pw)
				.item("condition", condition)
				.item("offset", offset)
				.item("limit", limit);
	}

	@Override
	public boolean deepEquals(Object obj) {
		if (!super.deepEquals(obj)) {
			return false;
		}
		DataflowTableScan other = (DataflowTableScan) obj;
		return Objects.equals(condition, other.condition) &&
				Objects.equals(offset, other.offset) &&
				Objects.equals(limit, other.limit);
	}

	@Override
	public int deepHashCode() {
		return super.deepHashCode() +
				Objects.hashCode(condition) +
				Objects.hashCode(offset) +
				Objects.hashCode(limit);
	}
}
