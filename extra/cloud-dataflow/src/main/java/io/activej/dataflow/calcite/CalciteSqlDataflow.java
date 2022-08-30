package io.activej.dataflow.calcite;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowException;
import io.activej.dataflow.SqlDataflow;
import io.activej.dataflow.calcite.DataflowShuttle.UnmaterializedDataset;
import io.activej.dataflow.collector.Collector;
import io.activej.dataflow.collector.MergeCollector;
import io.activej.dataflow.collector.UnionCollector;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.LocallySortedDataset;
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
		} catch (DataflowException | SqlParseException e) {
			return Promise.ofException(e);
		}

		TransformationResult transformed = transform(node);

		return queryDataflow(transformed.dataset().materialize(Collections.emptyList()));
	}

	public RelNode convertToNode(String sql) throws SqlParseException, DataflowException {
		SqlNode sqlNode = parser.parseQuery(sql);

		sqlNode = validator.validate(sqlNode);

		if (sqlNode.getKind() != SqlKind.SELECT && sqlNode.getKind() != SqlKind.UNION) {
			throw new DataflowException("Only 'SELECT' queries are allowed");
		}

		RelRoot root = convert(sqlNode);

		return optimize(root);
	}

	public TransformationResult transform(RelNode node) {
		DataflowShuttle shuttle = new DataflowShuttle(classLoader);
		node.accept(shuttle);

		return new TransformationResult(node.getRowType().getFieldList(), shuttle.getParameters(), shuttle.getUnmaterializedDataset());
	}

	public Promise<StreamSupplier<Record>> queryDataflow(Dataset<Record> dataset) {
		Collector<Record> calciteCollector = dataset instanceof LocallySortedDataset<?, Record> sortedDataset ?
				MergeCollector.create(sortedDataset, client, false) :
				UnionCollector.create(dataset, client);

		DataflowGraph graph = new DataflowGraph(client, partitions);
		StreamSupplier<Record> result = calciteCollector.compile(graph);

		return graph.execute()
				.map($ -> result);
	}

	private RelRoot convert(SqlNode sqlNode) {
		if (RelMetadataQueryBase.THREAD_PROVIDERS.get() == null) {
			RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.DEFAULT);
		}
		return converter.convertQuery(sqlNode, false, true);
	}

	private RelNode optimize(RelRoot root) {
		Program program = Programs.standard();
		return program.run(planner, root.project(), traits, Collections.emptyList(), Collections.emptyList());
	}

	public record TransformationResult(List<RelDataTypeField> fields, List<RexDynamicParam> parameters,
	                                   UnmaterializedDataset dataset) {
	}
}
