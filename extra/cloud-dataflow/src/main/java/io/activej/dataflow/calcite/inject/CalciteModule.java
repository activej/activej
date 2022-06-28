package io.activej.dataflow.calcite.inject;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.SqlDataflow;
import io.activej.dataflow.calcite.CalciteSqlDataflow;
import io.activej.dataflow.calcite.RecordFunction;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.annotation.Provides;
import io.activej.inject.binding.Binding;
import io.activej.inject.binding.BindingType;
import io.activej.inject.module.AbstractModule;
import io.activej.types.Types;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import static io.activej.common.Checks.checkState;
import static java.util.Collections.singletonList;

public final class CalciteModule extends AbstractModule {

	public static final String DATAFLOW_SCHEMA_NAME = "DATAFLOW";

	@Provides
	Schema schema(SchemaFactory schemaFactory) {
		return schemaFactory.create(null, DATAFLOW_SCHEMA_NAME, Map.of());
	}

	@Provides
	CalciteSchema calciteSchema(Schema schema) {
		return CalciteSchema.createRootSchema(true).add(DATAFLOW_SCHEMA_NAME, schema);
	}

	@Provides
	RelDataTypeFactory typeFactory() {
		return new JavaTypeFactoryImpl();
	}

	@Provides
	CalciteCatalogReader catalogReader(CalciteSchema calciteSchema, RelDataTypeFactory typeFactory) {
		return new CalciteCatalogReader(calciteSchema, singletonList(DATAFLOW_SCHEMA_NAME), typeFactory, CalciteConnectionConfig.DEFAULT);
	}

	@Provides
	SqlOperatorTable operatorTable(Set<SqlOperator> customOperators) {
		SqlOperatorTable standard = SqlStdOperatorTable.instance();
		SqlOperatorTable custom = new ListSqlOperatorTable(new ArrayList<>(customOperators));

		return SqlOperatorTables.chain(standard, custom);
	}

	@Provides
	SqlValidator validator(SqlOperatorTable operatorTable, CalciteCatalogReader catalogReader, RelDataTypeFactory typeFactory) {
		return SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, SqlValidator.Config.DEFAULT);
	}

	@Provides
	RexBuilder rexBuilder(RelDataTypeFactory typeFactory) {
		return new RexBuilder(typeFactory);
	}

	@Provides
	RelOptPlanner planner() {
		return new HepPlanner(HepProgram.builder().build());
	}

	@Provides
	RelOptCluster cluster(RelOptPlanner planner, RexBuilder rexBuilder) {
		return RelOptCluster.create(planner, rexBuilder);
	}

	@Provides
	SqlToRelConverter sqlToRelConverter(RelOptCluster cluster, SqlValidator validator, CalciteCatalogReader catalogReader) {
		return new SqlToRelConverter(ViewExpanders.simpleContext(cluster), validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.CONFIG);
	}

	@Provides
	SqlDataflow sqlDataflow(DataflowClient client, Injector injector, SqlToRelConverter sqlToRelConverter, RelOptPlanner planner) {
		Binding<?> graphBinding = injector.getBinding(DataflowGraph.class);
		checkState(graphBinding != null && graphBinding.getType() == BindingType.TRANSIENT);

		return CalciteSqlDataflow.create(client, () -> injector.getInstance(DataflowGraph.class), sqlToRelConverter, planner,
				cls -> injector.getInstance(Key.ofType(Types.parameterizedType(RecordFunction.class, cls))));
	}
}
