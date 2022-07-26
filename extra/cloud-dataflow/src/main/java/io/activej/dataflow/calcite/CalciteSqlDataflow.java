package io.activej.dataflow.calcite;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowException;
import io.activej.dataflow.SqlDataflow;
import io.activej.dataflow.calcite.DataflowShuttle.UnmaterializedDataset;
import io.activej.dataflow.collector.Collector;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.datastream.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.record.Record;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.Collections;
import java.util.List;

import static io.activej.common.Checks.checkNotNull;

public final class CalciteSqlDataflow implements SqlDataflow {
	private final DataflowClient client;
	private final List<Partition> partitions;

	private final SqlParser parser;
	private final SqlToRelConverter converter;
	private final SqlValidator validator;
	private final RelOptPlanner planner;
	private final DefiningClassLoader classLoader;

	private RelTraitSet traits = RelTraitSet.createEmpty();

	private CalciteSqlDataflow(DataflowClient client, List<Partition> partitions, SqlParser parser,
			SqlToRelConverter converter, RelOptPlanner planner, DefiningClassLoader classLoader) {
		this.client = client;
		this.partitions = partitions;
		this.parser = parser;
		this.converter = converter;
		this.validator = checkNotNull(converter.validator);
		this.planner = planner;
		this.classLoader = classLoader;
	}

	public static CalciteSqlDataflow create(DataflowClient client, List<Partition> partitions, SqlParser parser,
			SqlToRelConverter converter, RelOptPlanner planner, DefiningClassLoader classLoader) {
		return new CalciteSqlDataflow(client, partitions, parser, converter, planner, classLoader);
	}

	public CalciteSqlDataflow withTraits(RelTraitSet traits) {
		this.traits = traits;
		return this;
	}

	@Override
	public Promise<StreamSupplier<Record>> query(String sql) {
		RelNode node;
		try {
			node = convertToNode(sql);
		} catch (DataflowException e) {
			return Promise.ofException(e);
		}

		TransformationResult transformed = transform(node);

		return queryDataflow(transformed.dataset().materialize(Collections.emptyList()));
	}

	public RelNode convertToNode(String sql) throws DataflowException {
		SqlNode sqlNode;
		try {
			sqlNode = parser.parseQuery(sql);
			if (sqlNode.getKind() != SqlKind.SELECT && sqlNode.getKind() != SqlKind.ORDER_BY) { // `SELECT ... ORDER BY ...` is considered to have ORDER_BY kind for some reason
				throw new DataflowException("Only 'SELECT' queries are allowed");
			}
		} catch (SqlParseException e) {
			throw new DataflowException(e);
		}

		sqlNode = validate(sqlNode);

		RelRoot root = convert(sqlNode);

		return optimize(root);
	}

	public TransformationResult transform(RelNode node) {
		DataflowShuttle shuttle = new DataflowShuttle(classLoader);
		node.accept(shuttle);

		return new TransformationResult(node.getRowType().getFieldList(), shuttle.getParameters(), shuttle.getUnmaterializedDataset());
	}

	public Promise<StreamSupplier<Record>> queryDataflow(Dataset<Record> dataset) {
		Collector<Record> collector = new Collector<>(dataset, client);

		DataflowGraph graph = new DataflowGraph(client, partitions);
		StreamSupplier<Record> result = collector.compile(graph);

		return graph.execute()
				.map($ -> result);
	}

	private SqlNode validate(SqlNode sqlNode) {
		return validator.validate(sqlNode);
	}

	private RelRoot convert(SqlNode sqlNode) {
		if (RelMetadataQueryBase.THREAD_PROVIDERS.get() == null) {
			RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.DEFAULT);
		}
		return converter.convertQuery(sqlNode, false, true);
	}

	private RelNode optimize(RelRoot root) {
		Program program = Programs.standard();
		return program.run(planner, root.rel, traits, Collections.emptyList(), Collections.emptyList());
	}

	public record TransformationResult(List<RelDataTypeField> fields, List<RexDynamicParam> parameters, UnmaterializedDataset dataset) {

	}
}
