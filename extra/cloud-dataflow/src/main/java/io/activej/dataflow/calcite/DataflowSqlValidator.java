package io.activej.dataflow.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class DataflowSqlValidator extends SqlValidatorImpl {
	public static final String ALIAS_PREFIX = "$ALIAS_";
	public static final String SYNTHETIC_PREFIX = "$SYNTH_";

	public static final SqlValidatorUtil.Suggester ALIAS_SUGGESTER = (original, attempt, size) ->
			SYNTHETIC_PREFIX + Util.first(original, SqlUtil.GENERATED_EXPR_ALIAS_PREFIX) + attempt;

	public DataflowSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, Config config) {
		super(opTab, catalogReader, typeFactory, config);
	}

	@Override
	public SqlNode validate(SqlNode topNode) {
		return super.validate(topNode);
	}

	@Override
	public @Nullable String deriveAlias(SqlNode node, int ordinal) {
		String alias = super.deriveAlias(node, ordinal);
		if (alias != null && node.getKind() == SqlKind.AS && !alias.startsWith(SYNTHETIC_PREFIX)) {
			return ALIAS_PREFIX + alias;
		}
		return alias;
	}

	@Override
	protected void addToSelectList(List<SqlNode> list, Set<String> aliases, List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
		String alias = SqlValidatorUtil.getAlias(exp, -1);
		String uniqueAlias =
				SqlValidatorUtil.uniquify(
						alias, aliases, ALIAS_SUGGESTER);
		if (!Objects.equals(alias, uniqueAlias)) {
			exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
		}
		fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)));
		list.add(exp);
	}
}
