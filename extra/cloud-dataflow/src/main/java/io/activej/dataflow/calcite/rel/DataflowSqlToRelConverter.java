package io.activej.dataflow.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public final class DataflowSqlToRelConverter extends SqlToRelConverter {
	private final RelBuilder relBuilder;

	public DataflowSqlToRelConverter(RelOptTable.ViewExpander viewExpander, @Nullable SqlValidator validator, Prepare.CatalogReader catalogReader, RelOptCluster cluster, SqlRexConvertletTable convertletTable, Config config) {
		super(viewExpander, validator, catalogReader, cluster, convertletTable, config);

		this.relBuilder = config.getRelBuilderFactory().create(cluster, null)
				.transform(config.getRelBuilderConfigTransform());
	}

	@Override
	protected RelNode createJoin(Blackboard bb, RelNode leftRel, RelNode rightRel, RexNode joinCond, JoinRelType joinType) {
		RelNode node = super.createJoin(bb, leftRel, rightRel, joinCond, joinType);
		if (!(node instanceof Join join) || joinType != JoinRelType.LEFT) return node;

		int leftFieldCount = join.getLeft().getRowType().getFieldCount();
		List<RelDataTypeField> fieldList = node.getRowType().getFieldList();
		List<RelDataTypeField> converted = new ArrayList<>(fieldList);
		for (int i = leftFieldCount; i < fieldList.size(); i++) {
			RelDataTypeField original = fieldList.get(i);
			converted.set(i, new RelDataTypeFieldImpl(
					original.getName(),
					original.getIndex(),
					typeFactory.createTypeWithNullability(original.getType(), true)
			));
		}
		return relBuilder.push(node)
				.convert(new RelRecordType(converted), false)
				.build();
	}
}
