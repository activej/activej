package io.activej.dataflow.calcite;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowException;
import io.activej.dataflow.SqlDataflow;
import io.activej.dataflow.collector.Collector;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.record.Record;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.activej.common.Checks.checkNotNull;

public final class CalciteSqlDataflow implements SqlDataflow {
	private final DataflowClient client;
	private final Supplier<DataflowGraph> graphSupplier;

	private final SqlToRelConverter converter;
	private final SqlValidator validator;
	private final RelOptPlanner planner;
	private final Function<Class<?>, RecordFunction<Object>> recordFnFactory;

	private RelTraitSet traits = RelTraitSet.createEmpty();

	private CalciteSqlDataflow(DataflowClient client, Supplier<DataflowGraph> graphSupplier, SqlToRelConverter converter,
			RelOptPlanner planner, Function<Class<?>, RecordFunction<Object>> recordFnFactory) {
		this.client = client;
		this.graphSupplier = graphSupplier;
		this.converter = converter;
		this.validator = checkNotNull(converter.validator);
		this.planner = planner;
		this.recordFnFactory = recordFnFactory;
	}

	public static CalciteSqlDataflow create(DataflowClient client, Supplier<DataflowGraph> graphSupplier, SqlToRelConverter converter,
			RelOptPlanner planner, Function<Class<?>, RecordFunction<Object>> recordFnFactory) {
		return new CalciteSqlDataflow(client, graphSupplier, converter, planner, recordFnFactory);
	}

	public CalciteSqlDataflow withTraits(RelTraitSet traits) {
		this.traits = traits;
		return this;
	}

	@Override
	public Promise<StreamSupplier<Record>> query(String sql) {
		SqlNode sqlNode;
		try {
			sqlNode = parse(sql);
		} catch (SqlParseException e) {
			return Promise.ofException(new DataflowException(e));
		}

		sqlNode = validate(sqlNode);

		RelRoot root = convert(sqlNode);

		RelNode node = optimize(root);

		return toResultSupplier(node);
	}

	private static SqlNode parse(String sql) throws SqlParseException {
		SqlParser parser = SqlParser.create(sql);
		return parser.parseStmt();
	}

	private SqlNode validate(SqlNode sqlNode) {
		return validator.validate(sqlNode);
	}

	private RelRoot convert(SqlNode sqlNode) {
		return converter.convertQuery(sqlNode, false, true);
	}

	private RelNode optimize(RelRoot root) {
		Program program = Programs.standard();
		return program.run(planner, root.rel, traits, Collections.emptyList(), Collections.emptyList());
	}

	private Promise<StreamSupplier<Record>> toResultSupplier(RelNode node) {
		DataflowShuttle shuttle = new DataflowShuttle(recordFnFactory);
		node.accept(shuttle);

		Collector<Record> collector = new Collector<>(shuttle.result(), client);

		DataflowGraph graph = graphSupplier.get();
		StreamSupplier<Record> result = collector.compile(graph);

		return graph.execute()
				.map($ -> result);
	}
}
