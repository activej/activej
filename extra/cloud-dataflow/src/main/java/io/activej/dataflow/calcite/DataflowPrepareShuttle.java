package io.activej.dataflow.calcite;

import io.activej.common.exception.ToDoException;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjection;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionAsIs;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionChain;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjectionPojoField;
import io.activej.dataflow.calcite.aggregation.*;
import io.activej.dataflow.calcite.function.ProjectionFunction;
import io.activej.dataflow.calcite.join.RecordInnerJoiner;
import io.activej.record.RecordScheme;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;

import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Checks.checkState;

public class DataflowPrepareShuttle extends RelShuttleImpl {
	private final Queue<RecordScheme> currentQueue = new ArrayDeque<>();
	private final Set<RexDynamicParam> parameters = new TreeSet<>(Comparator.comparingInt(RexDynamicParam::getIndex));

	@Override
	public RelNode visit(LogicalCalc calc) {
		RelNode result = super.visit(calc);

		RexProgram program = calc.getProgram();

		RecordScheme current = checkNotNull(currentQueue.poll());

		RexLocalRef condition = program.getCondition();
		if (condition != null) {
			RexNode conditionNode = program.expandLocalRef(condition);
			scanFilter(conditionNode);
		}

		if (program.getInputRowType().equals(program.getOutputRowType())) {
			currentQueue.add(current);
			return result;
		}

		List<FieldProjection> projections = new ArrayList<>();
		for (RexNode rexNode : program.expandList(program.getProjectList())) {
			projections.add(project(rexNode));
		}

		RecordProjectionFn projectionFn = new RecordProjectionFn(projections);

		current = projectionFn.getToScheme(current, ($1, $2) -> null);

		currentQueue.add(current);
		return result;
	}

	private FieldProjection project(RexNode rexNode) {
		return switch (rexNode.getKind()) {
			case INPUT_REF -> new FieldProjectionAsIs(((RexInputRef) rexNode).getIndex());
			case OTHER_FUNCTION -> {
				RexCall rexCall = (RexCall) rexNode;
				SqlOperator operator = rexCall.getOperator();
				if (operator instanceof ProjectionFunction projectionFunction) {
					List<RexNode> operands = rexCall.getOperands();
					for (RexNode operand : operands) {
						if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
							parameters.add(((RexDynamicParam) operand));
						}
					}
					yield projectionFunction.projectField(null, operands);
				}
				throw new IllegalArgumentException("Unknown function: " + rexCall.getOperator());
			}
			case FIELD_ACCESS -> {
				RexFieldAccess fieldAccess = (RexFieldAccess) rexNode;
				RexNode referenceExpr = fieldAccess.getReferenceExpr();
				String fieldName = fieldAccess.getField().getName();
				Class<?> type = ((JavaType) fieldAccess.getType()).getJavaClass();

				if (referenceExpr instanceof RexInputRef input) {
					yield new FieldProjectionPojoField(null, input.getIndex(), fieldName, type);
				} else if (referenceExpr instanceof RexCall call) {
					yield new FieldProjectionChain(List.of(
							project(call),
							new FieldProjectionPojoField(null, -1, fieldName, type)
					));
				}
				throw new IllegalArgumentException();
			}
			default -> throw new IllegalArgumentException("Unsupported node kind: " + rexNode.getKind());
		};

	}

	@Override
	public RelNode visit(TableScan scan) {
		RelNode result = super.visit(scan);

		RecordScheme scanned = scan(scan);

		currentQueue.add(scanned);
		return result;
	}

	@Override
	public RelNode visit(LogicalFilter filter) {
		RelNode result = super.visit(filter);
		scanFilter(filter.getCondition());
		return result;
	}

	@Override
	public RelNode visit(LogicalJoin join) {
		RelNode result = super.visit(join);

		RecordScheme left = checkNotNull(currentQueue.poll());
		RecordScheme right = checkNotNull(currentQueue.poll());

		currentQueue.add(RecordInnerJoiner.createScheme(left, right));
		return result;
	}

	@Override
	public RelNode visit(LogicalAggregate aggregate) {
		RelNode result = super.visit(aggregate);

		RecordScheme current = checkNotNull(currentQueue.poll());

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

		RecordScheme newResult = recordReducer.createScheme(current);
		currentQueue.add(newResult);

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
		return super.visit(sort);
	}

	@Override
	public RelNode visit(LogicalExchange exchange) {
		throw new ToDoException();
	}

	@Override
	public RelNode visit(LogicalTableModify modify) {
		throw new ToDoException();
	}

	private static <T> RecordScheme scan(TableScan scan) {
		RelOptTable table = scan.getTable();

		//noinspection unchecked
		DataflowTable<T> dataflowTable = table.unwrap(DataflowTable.class);
		assert dataflowTable != null;

		RecordFunction<T> mapper = dataflowTable.getRecordFunction();

		return mapper.getScheme();
	}

	@Override
	public RelNode visit(RelNode other) {
		if (other instanceof LogicalCalc calc) {
			// visit() not overridden in LogicalCalc for some reason

			return visit(calc);
		}
		return super.visit(other);
	}

	public RecordScheme getScheme() {
		checkState(currentQueue.size() == 1);

		return currentQueue.peek();
	}

	public List<RexDynamicParam> getParameters() {
		checkState(currentQueue.size() == 1);

		return new ArrayList<>(parameters);
	}

	private void scanFilter(RexNode conditionNode) {
		if (conditionNode instanceof RexDynamicParam dynamic) {
			boolean unique = parameters.add(dynamic);
			checkState(unique);
		} else if (conditionNode instanceof RexCall call) {
			for (RexNode operand : call.getOperands()) {
				scanFilter(operand);
			}
		} else if (conditionNode instanceof RexFieldAccess fieldAccess){
			scanFilter(fieldAccess.getReferenceExpr());
		}
	}

}
