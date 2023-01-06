package io.activej.dataflow.calcite.inject;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.AsyncSqlDataflow;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowSqlValidator;
import io.activej.dataflow.calcite.RelToDatasetConverter;
import io.activej.dataflow.calcite.SqlDataflow;
import io.activej.dataflow.calcite.rel.DataflowSqlToRelConverter;
import io.activej.dataflow.graph.Partition;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.nio.NioReactor;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.Lex;
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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;

public final class CalciteClientModule extends AbstractModule {

	public static final String DATAFLOW_SCHEMA_NAME = "DATAFLOW";

	private CalciteClientModule() {
	}

	public static CalciteClientModule create() {
		return new CalciteClientModule();
	}

	@Override
	protected void configure() {
		install(new CalciteCommonModule());
		install(new SqlFunctionModule());

		bind(AsyncSqlDataflow.class);
	}

	@Provides
	CalciteSchema calciteSchema(DataflowSchema schema) {
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
		SqlOperatorTable custom = SqlOperatorTables.of(new ArrayList<>(customOperators));

		return SqlOperatorTables.chain(standard, custom);
	}

	@Provides
	SqlValidator validator(SqlOperatorTable operatorTable, CalciteCatalogReader catalogReader, RelDataTypeFactory typeFactory) {
		return new DataflowSqlValidator(operatorTable, catalogReader, typeFactory, SqlValidator.Config.DEFAULT);
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
	SqlToRelConverter.Config sqlToRelConverterConfig() {
		return SqlToRelConverter.CONFIG;
	}

	@Provides
	SqlToRelConverter sqlToRelConverter(RelOptCluster cluster, SqlValidator validator, CalciteCatalogReader catalogReader, SqlToRelConverter.Config sqlToRelConverterConfig) {
		return new DataflowSqlToRelConverter(ViewExpanders.simpleContext(cluster), validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, sqlToRelConverterConfig);
	}

	@Provides
	SqlParser.Config parserConfig() {
		return SqlParser.config()
				.withLex(Lex.JAVA)
				.withQuoting(Quoting.DOUBLE_QUOTE);
	}

	@Provides
	RelToDatasetConverter relToDatasetConverter(DefiningClassLoader classLoader) {
		return RelToDatasetConverter.create(classLoader);
	}

	@Provides
	SqlDataflow calciteSqlDataflow(NioReactor reactor, DataflowClient client, SqlParser.Config parserConfig, SqlToRelConverter sqlToRelConverter, RelOptPlanner planner,
			List<Partition> partitions, RelToDatasetConverter relToDatasetConverter) {
		return SqlDataflow.create(reactor, client, partitions, parserConfig, sqlToRelConverter, planner, relToDatasetConverter);
	}
}
