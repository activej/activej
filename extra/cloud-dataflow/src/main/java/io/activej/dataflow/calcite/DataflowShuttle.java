package io.activej.dataflow.calcite;

import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.ToDoException;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjection;
import io.activej.dataflow.calcite.aggregation.*;
import io.activej.dataflow.calcite.join.RecordInnerJoiner;
import io.activej.dataflow.calcite.join.RecordKeyFunction;
import io.activej.dataflow.calcite.sort.RecordComparator;
import io.activej.dataflow.calcite.sort.RecordComparator.FieldSort;
import io.activej.dataflow.calcite.where.*;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.Datasets;
import io.activej.dataflow.dataset.LocallySortedDataset;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.datastream.processor.StreamReducers.ReducerToResult.InputToOutput;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.common.Checks.*;
import static io.activej.common.Utils.last;

public class DataflowShuttle extends RelShuttleImpl {
	private final DefiningClassLoader classLoader;

	private final Queue<UnmaterializedDataset> currentQueue = new ArrayDeque<>();
	private final Set<RexDynamicParam> params = new TreeSet<>(Comparator.comparingInt(RexDynamicParam::getIndex));

	public DataflowShuttle(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public RelNode visit(LogicalCalc calc) {
		RelNode result = super.visit(calc);

		RexProgram program = calc.getProgram();

		UnmaterializedDataset current = checkNotNull(currentQueue.poll());

		RexLocalRef condition = program.getCondition();
		if (condition != null) {
			RexNode conditionNode = program.expandLocalRef(condition);
			current = filter(current, conditionNode);
		}

		if (program.getInputRowType().equals(program.getOutputRowType())) {
			currentQueue.add(current);
			return result;
		}

		List<FieldProjection> projections = new ArrayList<>();
		for (RexNode rexNode : program.expandList(program.getProjectList())) {
			projections.add(new FieldProjection(toOperand(rexNode, classLoader), null));
		}

		RecordProjectionFn projectionFn = new RecordProjectionFn(projections);

		UnmaterializedDataset finalCurrent = current;

		currentQueue.add(UnmaterializedDataset.of(
				projectionFn.getToScheme(current.getScheme(), ($1, $2) -> null),
				params -> Datasets.map(finalCurrent.materialize(params), projectionFn.materialize(params), Record.class)));

		return result;
	}

	@Override
	public RelNode visit(TableScan scan) {
		RelNode result = super.visit(scan);

		UnmaterializedDataset scanned = scan(scan);

		currentQueue.add(scanned);
		return result;
	}

	@Override
	public RelNode visit(LogicalFilter filter) {
		RelNode result = super.visit(filter);

		UnmaterializedDataset current = checkNotNull(currentQueue.poll());

		UnmaterializedDataset newResult = filter(current, filter.getCondition());

		currentQueue.add(newResult);
		return result;
	}

	@Override
	public RelNode visit(LogicalJoin join) {
		RelNode result = super.visit(join);

		UnmaterializedDataset left = checkNotNull(currentQueue.poll());
		UnmaterializedDataset right = checkNotNull(currentQueue.poll());

		UnmaterializedDataset joined = join(join, left, right, getJoinKeyType(join));

		currentQueue.add(joined);
		return result;
	}

	@Override
	public RelNode visit(LogicalAggregate aggregate) {
		RelNode result = super.visit(aggregate);

		UnmaterializedDataset current = checkNotNull(currentQueue.poll());

		List<AggregateCall> callList = aggregate.getAggCallList();
		List<FieldReducer<?, ?, ?>> fieldReducers = new ArrayList<>(callList.size());

		for (AggregateCall aggregateCall : callList) {
			SqlAggFunction aggregation = aggregateCall.getAggregation();

			List<Integer> argList = aggregateCall.getArgList();
			int fieldIndex = switch (argList.size()) {
				case 0 -> -1;
				case 1 -> argList.get(0);
				default -> throw new AssertionError();
			};

			FieldReducer<?, ?, ?> fieldReducer = switch (aggregation.getKind()) {
				case COUNT -> new CountReducer<>(fieldIndex);
				case SUM -> {
					SqlTypeName sqlTypeName = aggregate.getInput().getRowType().getFieldList().get(fieldIndex).getValue().getSqlTypeName();
					if (sqlTypeName == SqlTypeName.TINYINT || sqlTypeName == SqlTypeName.SMALLINT ||
							sqlTypeName == SqlTypeName.INTEGER || sqlTypeName == SqlTypeName.BIGINT)
						yield new SumReducerInteger<>(fieldIndex);
					if (sqlTypeName == SqlTypeName.FLOAT || sqlTypeName == SqlTypeName.DOUBLE ||
							sqlTypeName == SqlTypeName.REAL)
						yield new SumReducerDecimal<>(fieldIndex);
					throw new AssertionError("SUM() is not supported for type: " + sqlTypeName);
				}
				case AVG -> new AvgReducer(fieldIndex);
				case MIN -> new MinReducer<>(fieldIndex);
				case MAX -> new MaxReducer<>(fieldIndex);
				default -> throw new AssertionError();
			};

			fieldReducers.add(fieldReducer);
		}

		RecordReducer recordReducer = new RecordReducer(fieldReducers);

		currentQueue.add(UnmaterializedDataset.of(
				recordReducer.createScheme(current.getScheme()),
				params -> {
					Dataset<Record> materialized = current.materialize(params);
					LocallySortedDataset<RecordScheme, Record> sorted = Datasets.castToSorted(materialized, RecordScheme.class, new RecordSchemeFunction(), new EqualObjectComparator<>());

					InputToOutput<RecordScheme, Record, Record, Object[]> reducer = new InputToOutput<>(recordReducer);

					return Datasets.localReduce(sorted, reducer, Record.class, new RecordSchemeFunction());
				}));

		return result;
	}

	@Override
	public RelNode visit(LogicalMatch match) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(TableFunctionScan scan) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalValues values) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalProject project) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalCorrelate correlate) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalUnion union) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalIntersect intersect) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalMinus minus) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalSort sort) {
		RelNode result = super.visit(sort);

		UnmaterializedDataset current = checkNotNull(currentQueue.poll());

		UnmaterializedDataset sorted = sort(sort, current);

		currentQueue.add(sorted);
		return result;
	}

	@Override
	public RelNode visit(LogicalExchange exchange) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalTableModify modify) {
		throw new ToDoException();
	}

	private static <T> UnmaterializedDataset scan(TableScan scan) {
		RelOptTable table = scan.getTable();
		List<String> names = table.getQualifiedName();

		//noinspection unchecked
		DataflowTable<T> dataflowTable = table.unwrap(DataflowTable.class);
		assert dataflowTable != null;

		Dataset<T> dataset = Datasets.datasetOfId(last(names).toLowerCase(), dataflowTable.getType());
		RecordFunction<T> mapper = dataflowTable.getRecordFunction();

		return UnmaterializedDataset.of(mapper.getScheme(), $ -> Datasets.map(dataset, mapper, Record.class));
	}

	private static <T extends Comparable<T>> Class<? extends T> getJoinKeyType(LogicalJoin join) {
		RexNode condition = join.getCondition();
		if (condition.getKind() == SqlKind.EQUALS) {
			RexCall rexCall = (RexCall) condition;
			List<RexNode> operands = rexCall.getOperands();
			checkArgument(operands.size() == 2);

			JavaType left = ((JavaType) operands.get(0).getType());
			JavaType right = ((JavaType) operands.get(1).getType());

			//noinspection unchecked
			Class<T> leftClass = left.getJavaClass();
			checkArgument(leftClass == right.getJavaClass());
			checkArgument(leftClass.isPrimitive() || Comparable.class.isAssignableFrom(leftClass));

			return leftClass;
		}
		throw new IllegalArgumentException("Unsupported type: " + condition.getKind());
	}

	private static UnmaterializedDataset sort(LogicalSort sort, UnmaterializedDataset dataset) {
		List<RelDataTypeField> fieldList = sort.getRowType().getFieldList();
		List<FieldSort> sorts = new ArrayList<>(fieldList.size());
		for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
			int fieldIndex = fieldCollation.getFieldIndex();
			Direction direction = fieldCollation.getDirection();

			boolean asc;
			if (direction == Direction.ASCENDING) asc = true;
			else if (direction == Direction.DESCENDING) asc = false;
			else throw new IllegalArgumentException("Unsupported sort direction: " + direction);

			sorts.add(new FieldSort(fieldIndex, asc));
		}

		RecordComparator comparator = new RecordComparator(sorts);

		return UnmaterializedDataset.of(
				dataset.getScheme(),
				params -> Datasets.localSort(dataset.materialize(params), Record.class, Function.identity(), comparator)
		);
	}

	private static <K extends Comparable<K>> UnmaterializedDataset join(LogicalJoin join, UnmaterializedDataset left, UnmaterializedDataset right, Class<K> keyClass) {
		RexInputRef leftInputNode = (RexInputRef) ((RexCall) join.getCondition()).getOperands().get(0);
		RexInputRef rightInputNode = (RexInputRef) ((RexCall) join.getCondition()).getOperands().get(1);

		int leftIndex = leftInputNode.getIndex();
		int rightIndex = rightInputNode.getIndex() - join.getLeft().getRowType().getFieldCount();

		RecordKeyFunction<K> leftKeyFunction = new RecordKeyFunction<>(leftIndex);
		RecordKeyFunction<K> rightKeyFunction = new RecordKeyFunction<>(rightIndex);

		return UnmaterializedDataset.of(
				RecordInnerJoiner.createScheme(left.getScheme(), right.getScheme()),
				params -> {

					SortedDataset<K, Record> sortedLeft = Datasets.castToSorted(left.materialize(params), keyClass, leftKeyFunction, Comparator.naturalOrder());
					SortedDataset<K, Record> sortedRight = Datasets.castToSorted(right.materialize(params), keyClass, rightKeyFunction, Comparator.naturalOrder());

					return Datasets.join(
							sortedLeft,
							sortedRight,
							new RecordInnerJoiner<>(),
							Record.class,
							leftKeyFunction
					);
				});
	}

	@Override
	public RelNode visit(RelNode other) {
		if (other instanceof LogicalCalc calc) {
			// visit() not overridden in LogicalCalc for some reason

			return visit(calc);
		}
		return super.visit(other);
	}

	public UnmaterializedDataset getUnmaterializedDataset() {
		checkState(currentQueue.size() == 1);

		return currentQueue.peek();
	}

	public List<RexDynamicParam> getParameters() {
		checkState(currentQueue.size() == 1);

		return new ArrayList<>(params);
	}

	private UnmaterializedDataset filter(UnmaterializedDataset dataset, RexNode conditionNode) {
		SqlKind kind = conditionNode.getKind();

		if (kind == SqlKind.LITERAL) {
			RexLiteral literal = (RexLiteral) conditionNode;
			if (literal.isAlwaysFalse()) {
				return UnmaterializedDataset.of(dataset.getScheme(), $ -> Datasets.empty(Record.class));
			} else //noinspection StatementWithEmptyBody
				if (literal.isAlwaysTrue()) {
					// Do nothing
				} else {
					throw new IllegalArgumentException("Unknown literal: " + literal.getValueAs(Object.class));
				}
		} else if (conditionNode instanceof RexCall call) {
			WherePredicate wherePredicate = toWherePredicate(call);
			return UnmaterializedDataset.of(
					dataset.getScheme(),
					params -> Datasets.filter(dataset.materialize(params), wherePredicate.materialize(params)));
		} else {
			throw new IllegalArgumentException("Unknown condition: " + kind);
		}
		return dataset;
	}

	private WherePredicate toWherePredicate(RexCall conditionNode) {
		List<RexNode> operands = conditionNode.getOperands();
		return switch (conditionNode.getKind()) {
			case OR -> new OrPredicate(operands.stream()
					.map(rexNode -> toWherePredicate((RexCall) rexNode))
					.collect(Collectors.toList()));
			case AND -> new AndPredicate(operands.stream()
					.map(rexNode -> toWherePredicate((RexCall) rexNode))
					.collect(Collectors.toList()));
			case EQUALS ->
					new EqPredicate(toOperand(operands.get(0), classLoader), toOperand(operands.get(1), classLoader));
			case NOT_EQUALS ->
					new NotEqPredicate(toOperand(operands.get(0), classLoader), toOperand(operands.get(1), classLoader));
			case GREATER_THAN ->
					new GtPredicate(toOperand(operands.get(0), classLoader), toOperand(operands.get(1), classLoader));
			case GREATER_THAN_OR_EQUAL ->
					new GePredicate(toOperand(operands.get(0), classLoader), toOperand(operands.get(1), classLoader));
			case LESS_THAN ->
					new LtPredicate(toOperand(operands.get(0), classLoader), toOperand(operands.get(1), classLoader));
			case LESS_THAN_OR_EQUAL ->
					new LePredicate(toOperand(operands.get(0), classLoader), toOperand(operands.get(1), classLoader));
			case BETWEEN ->
					new BetweenPredicate(toOperand(operands.get(0), classLoader), toOperand(operands.get(1), classLoader), toOperand(operands.get(2), classLoader));
			case IN -> {
				List<Operand> options = operands.subList(1, operands.size())
						.stream()
						.map(option -> Utils.toOperand(option, classLoader))
						.collect(Collectors.toList());
				yield new InPredicate(toOperand(operands.get(0), classLoader), options);
			}
			case LIKE ->
					new LikePredicate(toOperand(operands.get(0), classLoader), toOperand(operands.get(1), classLoader));
			case IS_NULL -> new IsNullPredicate(toOperand(operands.get(0), classLoader));
			case IS_NOT_NULL -> new IsNotNullPredicate(toOperand(operands.get(0), classLoader));

			default -> throw new IllegalArgumentException("Not supported condition:" + conditionNode.getKind());
		};
	}

	private Operand toOperand(RexNode node, DefiningClassLoader classLoader) {
		Operand operand = Utils.toOperand(node, classLoader);
		params.addAll(operand.getParams());
		return operand;
	}

	public interface UnmaterializedDataset {
		Dataset<Record> materialize(List<Object> params);

		RecordScheme getScheme();

		static UnmaterializedDataset of(RecordScheme scheme, Function<List<Object>, Dataset<Record>> materializeFn) {
			return new UnmaterializedDataset() {
				@Override
				public Dataset<Record> materialize(List<Object> params) {
					return materializeFn.apply(params);
				}

				@Override
				public RecordScheme getScheme() {
					return scheme;
				}
			};
		}
	}
}
