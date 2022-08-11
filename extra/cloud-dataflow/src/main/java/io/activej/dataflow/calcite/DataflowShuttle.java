package io.activej.dataflow.calcite;

import com.google.common.collect.ImmutableList;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.exception.ToDoException;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjection;
import io.activej.dataflow.calcite.aggregation.*;
import io.activej.dataflow.calcite.join.RecordInnerJoiner;
import io.activej.dataflow.calcite.join.RecordKeyFunction;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.OperandRecordField;
import io.activej.dataflow.calcite.operand.OperandScalar;
import io.activej.dataflow.calcite.utils.RecordKeyComparator;
import io.activej.dataflow.calcite.utils.RecordSortComparator;
import io.activej.dataflow.calcite.utils.RecordSortComparator.FieldSort;
import io.activej.dataflow.calcite.utils.Utils;
import io.activej.dataflow.calcite.where.*;
import io.activej.dataflow.dataset.*;
import io.activej.dataflow.graph.DataflowContext;
import io.activej.dataflow.graph.DataflowGraph;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.graph.StreamId;
import io.activej.datastream.processor.StreamLimiter;
import io.activej.datastream.processor.StreamSkip;
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
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.activej.common.Checks.*;
import static io.activej.common.Utils.last;
import static io.activej.datastream.processor.StreamReducers.mergeReducer;

public class DataflowShuttle extends RelShuttleImpl {
	private final DefiningClassLoader classLoader;

	private final Deque<UnmaterializedDataset> datasetStack = new ArrayDeque<>();
	private final Set<RexDynamicParam> params = new TreeSet<>(Comparator.comparingInt(RexDynamicParam::getIndex));

	public DataflowShuttle(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public RelNode visit(LogicalCalc calc) {
		RelNode result = super.visit(calc);

		RexProgram program = calc.getProgram();

		UnmaterializedDataset current = checkNotNull(datasetStack.pop());

		RexLocalRef condition = program.getCondition();
		if (condition != null) {
			RexNode conditionNode = program.expandLocalRef(condition);
			current = filter(current, conditionNode);
		}

		if (program.getInputRowType().equals(program.getOutputRowType())) {
			datasetStack.push(current);
			return result;
		}

		List<FieldProjection> projections = new ArrayList<>();
		for (RexNode rexNode : program.expandList(program.getProjectList())) {
			projections.add(new FieldProjection(toOperand(rexNode), null));
		}

		RecordProjectionFn projectionFn = RecordProjectionFn.create(projections);

		UnmaterializedDataset finalCurrent = current;
		RecordScheme scheme = current.getScheme();

		datasetStack.push(UnmaterializedDataset.of(
				projectionFn.getToScheme(scheme, null),
				params -> {
					Dataset<Record> materialized = finalCurrent.materialize(params);
					RecordProjectionFn materializedFn = projectionFn.materialize(params);

					if (!(materialized instanceof LocallySortedDataset<?, Record> locallySortedDataset)) {
						return Datasets.map(materialized, materializedFn);
					}

					boolean needsRepartition = doesNeedRepartition(projections, scheme.size(), locallySortedDataset);

					if (!needsRepartition) {
						return mapAsSorted(projectionFn, locallySortedDataset);
					}

					return repartitionMap(projectionFn, locallySortedDataset);
				}));

		return result;
	}

	@Override
	public RelNode visit(TableScan scan) {
		RelNode result = super.visit(scan);

		UnmaterializedDataset scanned = scan(scan);

		datasetStack.push(scanned);
		return result;
	}

	@Override
	public RelNode visit(LogicalFilter filter) {
		RelNode result = super.visit(filter);

		UnmaterializedDataset current = checkNotNull(datasetStack.pop());

		UnmaterializedDataset newResult = filter(current, filter.getCondition());

		datasetStack.push(newResult);
		return result;
	}

	@Override
	public RelNode visit(LogicalJoin join) {
		RelNode result = super.visit(join);

		UnmaterializedDataset right = checkNotNull(datasetStack.pop());
		UnmaterializedDataset left = checkNotNull(datasetStack.pop());

		UnmaterializedDataset joined = join(join, left, right, getJoinKeyType(join));

		datasetStack.push(joined);
		return result;
	}

	@Override
	public RelNode visit(LogicalAggregate aggregate) {
		RelNode result = super.visit(aggregate);

		UnmaterializedDataset current = checkNotNull(datasetStack.pop());

		List<AggregateCall> callList = aggregate.getAggCallList();
		List<FieldReducer<?, ?, ?>> fieldReducers = new ArrayList<>(callList.size());

		ImmutableBitSet groupSet = aggregate.getGroupSet();
		for (Integer index : groupSet) {
			fieldReducers.add(new KeyReducer<>(index));
		}

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

		RecordReducer recordReducer = RecordReducer.create(current.getScheme(), fieldReducers);
		Function<Record, Record> keyFunction = getKeyFunction(groupSet.toList());

		datasetStack.push(UnmaterializedDataset.of(
				recordReducer.getOutputScheme(),
				params -> Datasets.sortReduceRepartitionReduce(current.materialize(params), recordReducer,
						Record.class, keyFunction, RecordKeyComparator.getInstance())));

		return result;
	}

	private static Function<Record, Record> getKeyFunction(List<Integer> indices) {
		List<FieldProjection> projections = new ArrayList<>(indices.size());
		for (Integer index : indices) {
			projections.add(new FieldProjection(new OperandRecordField(index), String.valueOf(index)));
		}
		return RecordProjectionFn.create(projections);
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
		RelNode result = super.visit(values);

		ImmutableList<ImmutableList<RexLiteral>> tuples = values.getTuples();

		List<Dataset<Record>> singleDatasets = new ArrayList<>(tuples.size());
		RecordScheme scheme = null;
		for (ImmutableList<RexLiteral> tuple : tuples) {
			SortedDataset<Record, Record> singleDummyDataset = Utils.singleDummyDataset();
			List<FieldProjection> projections = new ArrayList<>();
			for (RexLiteral field : tuple) {
				projections.add(new FieldProjection(toOperand(field), null));
			}
			RecordProjectionFn projectionFn = RecordProjectionFn.create(projections);
			singleDatasets.add(Datasets.map(singleDummyDataset, projectionFn));

			if (scheme == null) {
				scheme = projectionFn.getToScheme(RecordScheme.create(classLoader), null);
			}
		}

		if (singleDatasets.isEmpty()) return result;
		assert scheme != null;

		if (singleDatasets.size() == 1) {
			datasetStack.push(UnmaterializedDataset.of(scheme, $ -> singleDatasets.get(0)));

			return result;
		}

		Dataset<Record> union = singleDatasets.get(0);
		for (int i = 1; i < singleDatasets.size(); i++) {
			Dataset<Record> next = singleDatasets.get(i);

			union = Datasets.unionAll(union, next);
		}

		Dataset<Record> finalUnion = union;
		datasetStack.push(UnmaterializedDataset.of(scheme, $ -> finalUnion));

		return result;
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
		RelNode result = super.visit(union);

		UnmaterializedDataset right = checkNotNull(datasetStack.pop());
		UnmaterializedDataset left = checkNotNull(datasetStack.pop());

		datasetStack.push(UnmaterializedDataset.of(left.getScheme(), params -> {
			Dataset<Record> leftDataset = left.materialize(params);
			Dataset<Record> rightDataset = right.materialize(params);

			// Make sure field names match
			rightDataset = Datasets.map(rightDataset, RecordProjectionFn.rename(left.getScheme()));

			if (union.all) {
				return Datasets.unionAll(leftDataset, rightDataset);
			}

			SortedDataset<Record, Record> sortedLeft = sortForUnion(leftDataset, Datasets::repartitionSort);
			SortedDataset<Record, Record> sortedRight = sortForUnion(rightDataset, Datasets::castToSorted);

			return Datasets.union(sortedLeft, sortedRight);
		}));

		return result;
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

		UnmaterializedDataset current = checkNotNull(datasetStack.pop());

		UnmaterializedDataset sorted = sort(sort, current);

		datasetStack.push(sorted);
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

	@SuppressWarnings("ConstantConditions")
	private UnmaterializedDataset sort(LogicalSort sort, UnmaterializedDataset dataset) {
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

		OperandScalar offset = sort.offset == null ?
				new OperandScalar(Value.materializedValue(int.class, StreamSkip.NO_SKIP)) :
				toScalarOperand(sort.offset);

		OperandScalar limit = sort.fetch == null ?
				new OperandScalar(Value.materializedValue(int.class, StreamLimiter.NO_LIMIT)) :
				toScalarOperand(sort.fetch);

		return UnmaterializedDataset.of(
				dataset.getScheme(),
				params -> {
					Dataset<Record> materializedDataset = dataset.materialize(params);

					LocallySortedDataset<Record, Record> result = Datasets.localSort(materializedDataset, Record.class, Function.identity(), new RecordSortComparator(sorts));

					long offsetValue = ((Number) offset.materialize(params).getValue().getValue()).longValue();
					long limitValue = ((Number) limit.materialize(params).getValue().getValue()).longValue();

					if (offsetValue == StreamSkip.NO_SKIP && limitValue == StreamLimiter.NO_LIMIT) {
						return result;
					}

					return Datasets.datasetOffsetLimit(result, offsetValue, limitValue);
				}
		);
	}

	private static <K extends Comparable<K>> UnmaterializedDataset join(LogicalJoin join, UnmaterializedDataset left, UnmaterializedDataset right, Class<K> keyClass) {
		RexInputRef leftInputNode = (RexInputRef) ((RexCall) join.getCondition()).getOperands().get(0);
		RexInputRef rightInputNode = (RexInputRef) ((RexCall) join.getCondition()).getOperands().get(1);

		int leftIndex = leftInputNode.getIndex();
		int rightIndex = rightInputNode.getIndex() - join.getLeft().getRowType().getFieldCount();

		RecordKeyFunction<K> leftKeyFunction = new RecordKeyFunction<>(leftIndex);
		RecordKeyFunction<K> rightKeyFunction = new RecordKeyFunction<>(rightIndex);

		RecordInnerJoiner<K> joiner = RecordInnerJoiner.create(left.getScheme(), right.getScheme());

		return UnmaterializedDataset.of(
				joiner.getScheme(),
				params -> {
					Dataset<Record> leftDataset = left.materialize(params);
					SortedDataset<K, Record> sortedLeft = sortForJoin(keyClass, leftKeyFunction, leftDataset, Datasets::repartitionSort);

					Dataset<Record> rightDataset = right.materialize(params);
					SortedDataset<K, Record> sortedRight = sortForJoin(keyClass, rightKeyFunction, rightDataset, Datasets::castToSorted);

					return Datasets.join(
							sortedLeft,
							sortedRight,
							joiner,
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
		checkState(datasetStack.size() == 1);

		return datasetStack.peek();
	}

	public List<RexDynamicParam> getParameters() {
		checkState(datasetStack.size() == 1);

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
			case EQUALS -> new EqPredicate(toOperand(operands.get(0)), toOperand(operands.get(1)));
			case NOT_EQUALS -> new NotEqPredicate(toOperand(operands.get(0)), toOperand(operands.get(1)));
			case GREATER_THAN -> new GtPredicate(toOperand(operands.get(0)), toOperand(operands.get(1)));
			case GREATER_THAN_OR_EQUAL -> new GePredicate(toOperand(operands.get(0)), toOperand(operands.get(1)));
			case LESS_THAN -> new LtPredicate(toOperand(operands.get(0)), toOperand(operands.get(1)));
			case LESS_THAN_OR_EQUAL -> new LePredicate(toOperand(operands.get(0)), toOperand(operands.get(1)));
			case BETWEEN ->
					new BetweenPredicate(toOperand(operands.get(0)), toOperand(operands.get(1)), toOperand(operands.get(2)));
			case IN -> {
				List<Operand<?>> options = operands.subList(1, operands.size())
						.stream()
						.map(this::toOperand)
						.collect(Collectors.toList());
				yield new InPredicate(toOperand(operands.get(0)), options);
			}
			case LIKE -> new LikePredicate(toOperand(operands.get(0)), toOperand(operands.get(1)));
			case IS_NULL -> new IsNullPredicate(toOperand(operands.get(0)));
			case IS_NOT_NULL -> new IsNotNullPredicate(toOperand(operands.get(0)));

			default -> throw new IllegalArgumentException("Not supported condition:" + conditionNode.getKind());
		};
	}

	private Operand<?> toOperand(RexNode node) {
		Operand<?> operand = Utils.toOperand(node, classLoader);
		params.addAll(operand.getParams());
		return operand;
	}

	private OperandScalar toScalarOperand(RexNode node) {
		Operand<?> operand = toOperand(node);
		checkArgument(operand instanceof OperandScalar, "Not scalar operand");
		return (OperandScalar) operand;
	}

	private static <K extends Comparable<K>> SortedDataset<K, Record> sortForJoin(
			Class<K> keyClass, RecordKeyFunction<K> keyFunction, Dataset<Record> dataset,
			Function<LocallySortedDataset<K, Record>, SortedDataset<K, Record>> toSortedFn
	) {
		//noinspection PointlessBooleanExpression,unchecked

		LocallySortedDataset<K, Record> locallySorted = true &&
				dataset instanceof LocallySortedDataset<?, ?> locallySortedDataset &&
				locallySortedDataset.keyFunction() instanceof RecordKeyFunction<?> recordKeyFunction &&
				recordKeyFunction.getIndex() == keyFunction.getIndex() &&
				locallySortedDataset.keyComparator() == Comparator.naturalOrder() ?
				(LocallySortedDataset<K, Record>) locallySortedDataset :
				Datasets.localSort(dataset, keyClass, keyFunction, Comparator.naturalOrder());

		return toSortedFn.apply(locallySorted);
	}

	private static SortedDataset<Record, Record> sortForUnion(Dataset<Record> dataset,
			Function<LocallySortedDataset<Record, Record>, SortedDataset<Record, Record>> toSortedFn
	) {
		//noinspection PointlessBooleanExpression,unchecked

		LocallySortedDataset<Record, Record> locallySorted = true &&
				dataset instanceof LocallySortedDataset<?, ?> locallySortedDataset &&
				locallySortedDataset.keyFunction() == Function.identity() &&
				locallySortedDataset.keyComparator() == Comparator.naturalOrder() ?
				(LocallySortedDataset<Record, Record>) locallySortedDataset :
				Datasets.localSort(dataset, Record.class, Function.identity(), RecordKeyComparator.getInstance());

		return toSortedFn.apply(locallySorted);
	}

	private static boolean doesNeedRepartition(List<FieldProjection> projections, int schemeSize, LocallySortedDataset<?, Record> locallySortedDataset) {
		Comparator<?> comparator = locallySortedDataset.keyComparator();
		if (comparator instanceof RecordKeyComparator) {
			return checkReSortIndices(projections, IntStream.of(0, schemeSize).boxed().toList());
		}
		if (comparator instanceof RecordSortComparator recordSortComparator) {
			List<FieldSort> sorts = recordSortComparator.getSorts();
			return checkReSortIndices(projections, sorts.stream().map(FieldSort::index).distinct().toList());
		}
		if (comparator == Comparator.naturalOrder()) {
			return !(locallySortedDataset.keyFunction() instanceof RecordKeyFunction<?> recordKeyFunction) ||
					checkReSortIndices(projections, List.of(recordKeyFunction.getIndex()));
		}
		throw new AssertionError();
	}

	private static boolean checkReSortIndices(List<FieldProjection> projections, List<Integer> indices) {
		int size = projections.size();
		for (Integer index : indices) {
			if (index >= size) return true;

			FieldProjection projection = projections.get(index);
			if (!(projection.operand() instanceof OperandRecordField operandField) || operandField.getIndex() != index) {
				return true;
			}
		}
		return false;
	}

	private static <K> SortedDataset<K, Record> mapAsSorted(RecordProjectionFn projectionFn, LocallySortedDataset<K, Record> locallySortedDataset) {
		Dataset<Record> mapped = Datasets.map(locallySortedDataset, projectionFn);
		Class<K> keyType = locallySortedDataset.keyType();
		Function<Record, K> keyFunction = locallySortedDataset.keyFunction();
		return Datasets.castToSorted(mapped, keyType, keyFunction, locallySortedDataset.keyComparator());
	}

	private static <K> Dataset<Record> repartitionMap(RecordProjectionFn projectionFn, LocallySortedDataset<K, Record> locallySortedDataset) {
		Dataset<Record> repartitioned = new RepartitionToSingleDataset<>(locallySortedDataset);
		return Datasets.map(repartitioned, projectionFn);
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

	private static final class RepartitionToSingleDataset<K> extends LocallySortedDataset<K, Record> {
		private final Dataset<Record> input;

		private final int sharderNonce = ThreadLocalRandom.current().nextInt();

		private RepartitionToSingleDataset(LocallySortedDataset<K, Record> input) {
			super(input.valueType(), input.keyComparator(), input.keyType(), input.keyFunction());
			this.input = input;
		}

		@Override
		public List<StreamId> channels(DataflowContext context) {
			DataflowContext next = context.withFixedNonce(sharderNonce);

			List<StreamId> streamIds = input.channels(context);

			DataflowGraph graph = next.getGraph();

			if (streamIds.size() <= 1) {
				return streamIds;
			}

			StreamId randomStreamId = streamIds.get(Math.abs(sharderNonce) % streamIds.size());
			Partition randomPartition = graph.getPartition(randomStreamId);

			List<StreamId> newStreamIds = DatasetUtils.repartitionAndReduce(next, streamIds, valueType(), keyFunction(), keyComparator(), mergeReducer(), List.of(randomPartition));
			assert newStreamIds.size() == 1;

			return newStreamIds;
		}

		@Override
		public Collection<Dataset<?>> getBases() {
			return List.of(input);
		}
	}
}
