package io.activej.dataflow.calcite;

import io.activej.common.Utils;
import io.activej.common.exception.ToDoException;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjection;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionAsIs;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionListGet;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionMapGet;
import io.activej.dataflow.calcite.function.ListGetFunction;
import io.activej.dataflow.calcite.function.MapGetFunction;
import io.activej.dataflow.calcite.join.RecordInnerJoiner;
import io.activej.dataflow.calcite.join.RecordKeyFunction;
import io.activej.dataflow.calcite.sort.RecordComparator;
import io.activej.dataflow.calcite.sort.RecordComparator.FieldSort;
import io.activej.dataflow.calcite.where.*;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.dataset.Datasets;
import io.activej.dataflow.dataset.SortedDataset;
import io.activej.record.Record;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.common.Checks.*;
import static java.util.Objects.requireNonNull;

public class DataflowShuttle extends RelShuttleImpl {
	Queue<Dataset<Record>> current = new ArrayDeque<>();

	@Override
	public RelNode visit(LogicalCalc calc) {
		RelNode result = super.visit(calc);

		RexProgram program = calc.getProgram();

		Dataset<Record> dataset = checkNotNull(current.poll());

		RexLocalRef condition = program.getCondition();
		if (condition != null) {
			RexNode conditionNode = program.expandLocalRef(condition);
			dataset = filter(dataset, conditionNode);
		}

		if (program.getInputRowType().equals(program.getOutputRowType())) {
			current.add(dataset);
			return result;
		}

		List<FieldProjection> projections = new ArrayList<>();
		for (RexNode rexNode : program.expandList(program.getProjectList())) {
			switch (rexNode.getKind()) {
				case INPUT_REF -> projections.add(new FieldProjectionAsIs(((RexInputRef) rexNode).getIndex()));
				case OTHER_FUNCTION -> {
					RexCall rexCall = (RexCall) rexNode;
					SqlOperator operator = rexCall.getOperator();
					if (operator instanceof MapGetFunction) {
						RexInputRef mapInput = (RexInputRef) rexCall.getOperands().get(0);
						RexNode key = rexCall.getOperands().get(1);

						projections.add(new FieldProjectionMapGet(null, mapInput.getIndex(), toJavaType((RexLiteral) key)));
					} else if (operator instanceof ListGetFunction) {
						RexInputRef listInput = (RexInputRef) rexCall.getOperands().get(0);
						RexNode key = rexCall.getOperands().get(1);

						projections.add(new FieldProjectionListGet(null, listInput.getIndex(), (int) toJavaType((RexLiteral) key)));
					} else {
						throw new IllegalArgumentException("Unknown function: " + rexCall.getOperator());
					}
				}
				default -> throw new IllegalArgumentException("Unsupported node kind: " + rexNode.getKind());
			}
		}

		RecordProjectionFn projectionFn = new RecordProjectionFn(projections);

		dataset = Datasets.map(dataset, projectionFn, Record.class);

		current.add(dataset);
		return result;
	}

	@Override
	public RelNode visit(TableScan scan) {
		RelNode result = super.visit(scan);

		Dataset<Record> scanned = scan(scan);

		current.add(scanned);
		return result;
	}

	@Override
	public RelNode visit(LogicalFilter filter) {
		RelNode result = super.visit(filter);
		current.add(filter(current.poll(), filter.getCondition()));
		return result;
	}

	@Override
	public RelNode visit(LogicalJoin join) {
		RelNode result = super.visit(join);

		Dataset<Record> left = checkNotNull(current.poll());
		Dataset<Record> right = checkNotNull(current.poll());

		Dataset<Record> joined = join(join, left, right, getJoinKeyType(join));

		current.add(joined);
		return result;
	}

	@Override
	public RelNode visit(LogicalAggregate aggregate) {
		throw new ToDoException();
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

		Dataset<Record> dataset = checkNotNull(current.poll());

		Dataset<Record> sorted = sort(sort, dataset);

		current.add(sorted);
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

	private static <T> Dataset<Record> scan(TableScan scan) {
		RelOptTable table = scan.getTable();
		List<String> names = table.getQualifiedName();

		//noinspection unchecked
		DataflowTable<T> dataflowTable = table.unwrap(DataflowTable.class);
		assert dataflowTable != null;

		Dataset<T> dataset = Datasets.datasetOfId(Utils.last(names).toLowerCase(), dataflowTable.getType());
		RecordFunction<T> mapper = dataflowTable.getRecordFunction();

		return Datasets.map(dataset, mapper, Record.class);
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

	private static Dataset<Record> sort(LogicalSort sort, Dataset<Record> dataset) {
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

		return Datasets.localSort(dataset, Record.class, Function.identity(), comparator);
	}

	private static <K extends Comparable<K>> Dataset<Record> doSort(Dataset<Record> dataset, int index, Direction direction, Class<K> keyClass) {
		RecordKeyFunction<K> keyFunction = new RecordKeyFunction<>(index);

		Comparator<K> comparator;
		if (direction == Direction.ASCENDING) comparator = Comparator.naturalOrder();
		else if (direction == Direction.DESCENDING) comparator = Comparator.reverseOrder();
		else throw new IllegalArgumentException("Unsupported sort direction: " + direction);

		return Datasets.localSort(dataset, keyClass, keyFunction, comparator);
	}

	private static <K extends Comparable<K>> Dataset<Record> join(LogicalJoin join, Dataset<Record> left, Dataset<Record> right, Class<K> keyClass) {
		RexInputRef leftInputNode = (RexInputRef) ((RexCall) join.getCondition()).getOperands().get(0);
		RexInputRef rightInputNode = (RexInputRef) ((RexCall) join.getCondition()).getOperands().get(1);

		int leftIndex = leftInputNode.getIndex();
		int rightIndex = rightInputNode.getIndex() - join.getLeft().getRowType().getFieldCount();

		RecordKeyFunction<K> leftKeyFunction = new RecordKeyFunction<>(leftIndex);
		RecordKeyFunction<K> rightKeyFunction = new RecordKeyFunction<>(rightIndex);

		SortedDataset<K, Record> sortedLeft = Datasets.castToSorted(left, keyClass, leftKeyFunction, Comparator.naturalOrder());
		SortedDataset<K, Record> sortedRight = Datasets.castToSorted(right, keyClass, rightKeyFunction, Comparator.naturalOrder());

		return Datasets.join(
				sortedLeft,
				sortedRight,
				new RecordInnerJoiner<>(),
				Record.class,
				leftKeyFunction
		);
	}

	@Override
	public RelNode visit(RelNode other) {
		if (other instanceof LogicalCalc calc) {
			// visit() not overridden in LogicalCalc for some reason

			return visit(calc);
		}
		return super.visit(other);
	}

	public Dataset<Record> result() {
		checkState(current.size() == 1);

		return current.peek();
	}

	private static Dataset<Record> filter(Dataset<Record> dataset, RexNode conditionNode) {
		SqlKind kind = conditionNode.getKind();

		if (kind == SqlKind.LITERAL) {
			RexLiteral literal = (RexLiteral) conditionNode;
			if (literal.isAlwaysFalse()) {
				dataset = Datasets.empty(dataset.valueType());
			} else //noinspection StatementWithEmptyBody
				if (literal.isAlwaysTrue()) {
					// Do nothing
				} else {
					throw new IllegalArgumentException("Unknown literal: " + literal.getValueAs(Object.class));
				}
		} else if (conditionNode instanceof RexCall call) {
			WherePredicate wherePredicate = toWherePredicate(call);
			dataset = Datasets.filter(dataset, wherePredicate);
		} else {
			throw new IllegalArgumentException("Unknown condition: " + kind);
		}
		return dataset;
	}

	private static WherePredicate toWherePredicate(RexCall conditionNode) {
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
						.map(DataflowShuttle::toOperand)
						.collect(Collectors.toList());
				yield new InPredicate(toOperand(operands.get(0)), options);
			}
			case LIKE ->
				//noinspection unchecked
					new LikePredicate((Operand<String>) toOperand(operands.get(0)), (String) toJavaType((RexLiteral) operands.get(1)));
			default -> throw new IllegalArgumentException("Not supported condition:" + conditionNode.getKind());
		};
	}

	private static Operand<?> toOperand(RexNode conditionNode) {
		if (conditionNode instanceof RexCall call) {
			if (call.getKind() == SqlKind.CAST) {
				List<RexNode> operands = call.getOperands();
				if (operands.size() == 1) {
					RexInputRef inputRef = (RexInputRef) operands.get(0);
					return new OperandField<>(inputRef.getIndex());
				}
			}
		} else if (conditionNode instanceof RexLiteral literal) {
			return new OperandScalar<>(toJavaType(literal));
		} else if (conditionNode instanceof RexInputRef inputRef) {
			return new OperandField<>(inputRef.getIndex());
		}
		throw new IllegalArgumentException("Unknown node: " + conditionNode);
	}

	private static Object toJavaType(RexLiteral literal) {
		SqlTypeName typeName = literal.getTypeName();

		return switch (requireNonNull(typeName.getFamily())) {
			case CHARACTER -> literal.getValueAs(String.class);
			case NUMERIC, INTEGER -> literal.getValueAs(Integer.class);
			default -> throw new ToDoException();
		};
	}

}
