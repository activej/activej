package io.activej.dataflow.calcite;

import io.activej.dataflow.DataflowClient;
import io.activej.dataflow.DataflowException;
import io.activej.dataflow.SqlDataflow;
import io.activej.dataflow.calcite.RelToDatasetConverter.ConversionResult;
import io.activej.dataflow.calcite.optimizer.ParameterizedTableRule;
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
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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

	private final SqlParser.Config parserConfig;
	private final SqlToRelConverter converter;
	private final SqlValidator validator;
	private final RelOptPlanner planner;
	private final RelToDatasetConverter relToDatasetConverter;

	private RelTraitSet traits = RelTraitSet.createEmpty();

	private CalciteSqlDataflow(DataflowClient client, List<Partition> partitions, SqlParser.Config parserConfig,
			SqlToRelConverter converter, RelOptPlanner planner, RelToDatasetConverter relToDatasetConverter) {
		this.client = client;
		this.partitions = partitions;
		this.parserConfig = parserConfig;
		this.converter = converter;
		this.validator = checkNotNull(converter.validator);
		this.planner = planner;
		this.relToDatasetConverter = relToDatasetConverter;
	}

	public static CalciteSqlDataflow create(DataflowClient client, List<Partition> partitions, SqlParser.Config parserConfig,
			SqlToRelConverter converter, RelOptPlanner planner, RelToDatasetConverter relToDatasetConverter) {
		return new CalciteSqlDataflow(client, partitions, parserConfig, converter, planner, relToDatasetConverter);
	}

	public CalciteSqlDataflow withTraits(RelTraitSet traits) {
		this.traits = traits;
		return this;
	}

	@Override
	public Promise<StreamSupplier<Record>> query(String sql) {
		try {
			return Promise.of(queryDataflow(convertToDataset(sql)));
		} catch (DataflowException | SqlParseException e) {
			return Promise.ofException(e);
		}
	}

	public RelNode convertToNode(String sql) throws SqlParseException, DataflowException {
		SqlParser sqlParser = SqlParser.create(sql, parserConfig);
		SqlNode sqlNode = sqlParser.parseQuery();

		sqlNode = validator.validate(sqlNode);

		if (sqlNode.getKind() != SqlKind.SELECT && sqlNode.getKind() != SqlKind.UNION) {
			throw new DataflowException("Only 'SELECT' queries are allowed");
		}

		RelRoot root = convert(sqlNode);

		return optimize(root);
	}

	public Dataset<Record> convertToDataset(String sql) throws SqlParseException, DataflowException {
		RelNode node = convertToNode(sql);
		ConversionResult transformed = convert(node);

		return transformed.unmaterializedDataset().materialize(Collections.emptyList());
	}

	public ConversionResult convert(RelNode node) {
		return relToDatasetConverter.convert(node);
	}

	public StreamSupplier<Record> queryDataflow(Dataset<Record> dataset) {
		Collector<Record> calciteCollector = createCollector(dataset);

		DataflowGraph graph = new DataflowGraph(client, partitions);
		StreamSupplier<Record> result = calciteCollector.compile(graph);

		graph.execute();
		return result;
	}

	private RelRoot convert(SqlNode sqlNode) {
		if (RelMetadataQueryBase.THREAD_PROVIDERS.get() == null) {
			RelMetadataQueryBase.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.DEFAULT);
		}
		return converter.convertQuery(sqlNode, false, true);
	}

	private RelNode optimize(RelRoot root) {
		HepProgramBuilder builder = new HepProgramBuilder();
		builder.addRuleCollection(List.of(CoreRules.FILTER_INTO_JOIN, ParameterizedTableRule.create()));
		Program prog = Programs.of(builder.build(), true, DefaultRelMetadataProvider.INSTANCE);
		Program program = Programs.sequence(prog);
		return program.run(planner, root.project(), traits, Collections.emptyList(), Collections.emptyList());
	}

	public CalciteSchema getSchema() {
		return validator.getCatalogReader().getRootSchema();
	}

	public RelDataTypeFactory getTypeFactory() {
		return validator.getTypeFactory();
	}

	public String explainPlan(String query) throws SqlParseException, DataflowException {
		return convertToNode(query).explain();
	}

	public String explainGraph(String query) throws SqlParseException, DataflowException {
		return convertToDataset(query).toGraphViz();
	}

	public String explainNodes(String query) throws SqlParseException, DataflowException {
		Dataset<Record> dataset = convertToDataset(query);
		Collector<Record> calciteCollector = createCollector(dataset);

		DataflowGraph graph = new DataflowGraph(client, partitions);
		calciteCollector.compile(graph);

		return graph.toGraphViz();
	}

	private Collector<Record> createCollector(Dataset<Record> dataset) {
		return dataset instanceof LocallySortedDataset<?, Record> sortedDataset ?
				MergeCollector.create(sortedDataset, client, false) :
				UnionCollector.create(dataset, client);
	}
}
