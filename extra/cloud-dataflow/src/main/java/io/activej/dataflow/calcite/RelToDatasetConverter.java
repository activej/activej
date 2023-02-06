package io.activej.dataflow.calcite;

import com.google.common.collect.ImmutableList;
import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.calcite.RecordProjectionFn.FieldProjection;
import io.activej.dataflow.calcite.aggregation.*;
import io.activej.dataflow.calcite.dataset.SupplierOfPredicateDataset;
import io.activej.dataflow.calcite.join.RecordLeftJoiner;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.impl.RecordField;
import io.activej.dataflow.calcite.operand.impl.Scalar;
import io.activej.dataflow.calcite.rel.DataflowTableScan;
import io.activej.dataflow.calcite.table.AbstractDataflowTable;
import io.activej.dataflow.calcite.table.DataflowPartitionedTable;
import io.activej.dataflow.calcite.utils.*;
import io.activej.dataflow.calcite.utils.RecordSortComparator.FieldSort;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.dataflow.calcite.where.WherePredicates;
import io.activej.dataflow.dataset.*;
import io.activej.dataflow.graph.*;
import io.activej.datastream.processor.reducer.Reducer;
import io.activej.datastream.processor.transformer.impl.Limiter;
import io.activej.datastream.processor.transformer.impl.Skip;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.types.Types;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.last;
import static io.activej.dataflow.calcite.operand.Operands.recordField;
import static io.activej.dataflow.calcite.where.WherePredicates.and;
import static io.activej.datastream.processor.reducer.Reducers.mergeReducer;
import static java.util.Collections.emptyList;

public class RelToDatasetConverter {
	private final DefiningClassLoader classLoader;

	private RelToDatasetConverter(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	public static RelToDatasetConverter create(DefiningClassLoader classLoader) {
		return new RelToDatasetConverter(classLoader);
	}

	public ConversionResult convert(RelNode relNode) {
		ParamsCollector paramsCollector = new ParamsCollector();
		UnmaterializedDataset dataset = handle(relNode, paramsCollector);
		return new ConversionResult(dataset, paramsCollector.getParams());
	}

	private UnmaterializedDataset handle(RelNode relNode, ParamsCollector paramsCollector) {
		if (relNode instanceof LogicalProject logicalProject) {
			return handle(logicalProject, paramsCollector);
		}
		if (relNode instanceof DataflowTableScan tableScan) {
			return handle(tableScan, paramsCollector);
		}
		if (relNode instanceof LogicalFilter logicalFilter) {
			return handle(logicalFilter, paramsCollector);
		}
		if (relNode instanceof LogicalJoin logicalJoin) {
			return handle(logicalJoin, paramsCollector);
		}
		if (relNode instanceof LogicalAggregate logicalAggregate) {
			return handle(logicalAggregate, paramsCollector);
		}
		if (relNode instanceof LogicalValues logicalValues) {
			return handle(logicalValues, paramsCollector);
		}
		if (relNode instanceof LogicalUnion logicalUnion) {
			return handle(logicalUnion, paramsCollector);
		}
		if (relNode instanceof LogicalSort logicalSort) {
			return handle(logicalSort, paramsCollector);
		}
		throw new IllegalArgumentException("Unknown node type: " + relNode.getClass().getName());
	}

	private UnmaterializedDataset handle(LogicalProject project, ParamsCollector paramsCollector) {
		UnmaterializedDataset current = handle(project.getInput(), paramsCollector);
		RecordScheme scheme = current.getScheme();

		List<RexNode> projectNodes = project.getProjects();
		List<String> fieldNames = project.getRowType().getFieldNames();

		List<FieldProjection> projections = new ArrayList<>(projectNodes.size());
		assert projectNodes.size() == fieldNames.size();

		boolean redundantProjection = projectNodes.size() == scheme.size();
		for (int i = 0; i < projectNodes.size(); i++) {
			RexNode projectNode = projectNodes.get(i);
			String fieldName = fieldNames.get(i);

			Operand<?> projectOperand = paramsCollector.toOperand(projectNode);

			if (isSynthetic(fieldName) || projectNode.getKind() == SqlKind.FIELD_ACCESS) {
				fieldName = null;
			}
			FieldProjection fieldProjection = new FieldProjection(projectOperand, fieldName);

			projections.add(fieldProjection);

			if (!(projectOperand instanceof RecordField operandRecordField) ||
					operandRecordField.index != i ||
					!scheme.getField(i).equals(fieldName)) {
				redundantProjection = false;
			}
		}

		if (redundantProjection) {
			return current;
		}

		RecordProjectionFn projectionFn = RecordProjectionFn.create(projections);

		RecordScheme toScheme = projectionFn.getToScheme(scheme, null);
		return UnmaterializedDataset.of(
				toScheme,
				params -> {
					Dataset<Record> materialized = current.materialize(params);
					RecordProjectionFn materializedFn = projectionFn.materialize(params);

					if (!(materialized instanceof LocallySortedDataset<?, Record> locallySortedDataset) ||
							!(locallySortedDataset.keyComparator() instanceof RecordSortComparator)) {
						return Datasets.map(materialized, materializedFn, RecordStreamSchema.create(toScheme));
					}

					boolean needsRepartition = doesNeedRepartition(projections, scheme.size(), locallySortedDataset);

					if (!needsRepartition) {
						return mapAsSorted(toScheme, projectionFn, locallySortedDataset);
					}

					return repartitionMap(toScheme, projectionFn, locallySortedDataset);
				});
	}

	@SuppressWarnings({"unchecked", "ConstantConditions"})
	private UnmaterializedDataset handle(DataflowTableScan scan, ParamsCollector paramsCollector) {
		RelOptTable table = scan.getTable();

		AbstractDataflowTable<?> dataflowTable = table.unwrap(AbstractDataflowTable.class);
		assert dataflowTable != null;

		RecordFunction<Object> mapper = (RecordFunction<Object>) dataflowTable.getRecordFunction();
		String id = last(table.getQualifiedName());

		WherePredicate wherePredicate;
		RexNode predicate = scan.getCondition();
		boolean needsFiltering = predicate instanceof RexCall;
		if (needsFiltering) {
			wherePredicate = paramsCollector.toWherePredicate((RexCall) predicate);
		} else {
			wherePredicate = and(emptyList());
		}

		RexNode offsetNode = scan.getOffset();
		RexNode limitNode = scan.getLimit();
		Scalar offsetOperand = offsetNode == null ?
				new Scalar(Value.materializedValue(int.class, Skip.NO_SKIP)) :
				paramsCollector.toScalarOperand(offsetNode);

		Scalar limitOperand = limitNode == null ?
				new Scalar(Value.materializedValue(int.class, Limiter.NO_LIMIT)) :
				paramsCollector.toScalarOperand(limitNode);

		RecordScheme scheme = mapper.getScheme();
		return UnmaterializedDataset.of(scheme, params -> {
			WherePredicate materializedPredicate = wherePredicate.materialize(params);
			Class<Object> type = (Class<Object>) dataflowTable.getType();
			Dataset<Object> dataset = SupplierOfPredicateDataset.create(id, materializedPredicate, StreamSchemas.simple(type));

			Dataset<Record> mapped = Datasets.map(dataset, new NamedRecordFunction<>(id, mapper), RecordStreamSchema.create(scheme));
			Dataset<Record> filtered = needsFiltering ?
					Datasets.filter(mapped, materializedPredicate) :
					mapped;

			if (!(dataflowTable instanceof DataflowPartitionedTable<?> dataflowPartitionedTable)) return filtered;

			long offset = ((Number) offsetOperand.materialize(params).value.getValue()).longValue();
			long limit = ((Number) limitOperand.materialize(params).value.getValue()).longValue();

			if (limit != Limiter.NO_LIMIT) {
				filtered = Datasets.localLimit(filtered, offset + limit);
			}

			List<Integer> indexes = new ArrayList<>(dataflowPartitionedTable.getPrimaryKeyIndexes());
			if (indexes.isEmpty()) {
				for (int i = 0; i < scheme.size(); i++) {
					indexes.add(i);
				}
			}

			LocallySortedDataset<Record, Record> locallySortedDataset = Datasets.localSort(filtered, Record.class, getKeyFunction(indexes), RecordKeyComparator.getInstance());

			Reducer<Record, Record, Record, ?> providedReducer = dataflowPartitionedTable.getReducer();
			Reducer<Record, Record, Record, ?> reducer = new NamedReducer(id, (Reducer<Record, Record, Record, Object>) providedReducer);

			return Datasets.repartitionReduce(locallySortedDataset, reducer, RecordStreamSchema.create(scheme));
		});
	}

	private UnmaterializedDataset handle(LogicalFilter filter, ParamsCollector paramsCollector) {
		UnmaterializedDataset current = handle(filter.getInput(), paramsCollector);

		return filter(current, filter.getCondition(), paramsCollector);
	}

	private UnmaterializedDataset handle(LogicalJoin join, ParamsCollector paramsCollector) {
		UnmaterializedDataset left = handle(join.getLeft(), paramsCollector);
		UnmaterializedDataset right = handle(join.getRight(), paramsCollector);

		RecordScheme leftScheme = left.getScheme();
		RecordScheme rightScheme = right.getScheme();

		List<String> fieldNames = join.getRowType().getFieldNames();
		RecordLeftJoiner joiner = RecordLeftJoiner.create(join.getJoinType(), leftScheme, rightScheme, fieldNames);

		JoinKeyProjections joinKeyProjections = getJoinKeyProjections(leftScheme, rightScheme, join);

		RecordScheme resultSchema = joiner.getScheme();
		return UnmaterializedDataset.of(
				resultSchema,
				params -> {
					Dataset<Record> leftDataset = left.materialize(params);
					SortedDataset<Record, Record> sortedLeft = sortForJoin(joinKeyProjections.leftKeyProjection, leftDataset, Datasets::repartitionSort);

					Dataset<Record> rightDataset = right.materialize(params);
					SortedDataset<Record, Record> sortedRight = sortForJoin(joinKeyProjections.rightKeyProjection, rightDataset, Datasets::castToSorted);

					return Datasets.join(
							sortedLeft,
							sortedRight,
							joiner,
							RecordStreamSchema.create(resultSchema),
							joinKeyProjections.leftKeyProjection
					);
				});
	}

	private UnmaterializedDataset handle(LogicalAggregate aggregate, ParamsCollector paramsCollector) {
		UnmaterializedDataset current = handle(aggregate.getInput(), paramsCollector);

		List<AggregateCall> callList = aggregate.getAggCallList();
		List<FieldReducer<?, ?, ?>> fieldReducers = new ArrayList<>(callList.size());

		List<RelDataTypeField> fieldList = aggregate.getRowType().getFieldList();
		ImmutableBitSet groupSet = aggregate.getGroupSet();
		for (Integer index : groupSet) {
			String fieldName = fieldList.get(fieldReducers.size()).getName();
			fieldReducers.add(new KeyReducer<>(index, isSynthetic(fieldName) ? null : fieldName));
		}

		for (AggregateCall aggregateCall : callList) {
			SqlAggFunction aggregation = aggregateCall.getAggregation();

			List<Integer> argList = aggregateCall.getArgList();
			int fieldIndex = switch (argList.size()) {
				case 0 -> -1;
				case 1 -> argList.get(0);
				default -> throw new AssertionError();
			};

			String fieldName = fieldList.get(fieldReducers.size()).getName();
			String alias = isSynthetic(fieldName) ? null : fieldName;
			FieldReducer<?, ?, ?> fieldReducer = switch (aggregation.getKind()) {
				case COUNT -> new CountReducer<>(fieldIndex, alias);
				case SUM -> {
					SqlTypeName sqlTypeName = aggregate.getInput().getRowType().getFieldList().get(fieldIndex).getValue().getSqlTypeName();
					if (sqlTypeName == SqlTypeName.TINYINT || sqlTypeName == SqlTypeName.SMALLINT ||
							sqlTypeName == SqlTypeName.INTEGER || sqlTypeName == SqlTypeName.BIGINT)
						yield new SumReducerInteger<>(fieldIndex, alias);
					if (sqlTypeName == SqlTypeName.FLOAT || sqlTypeName == SqlTypeName.DOUBLE ||
							sqlTypeName == SqlTypeName.REAL)
						yield new SumReducerDecimal<>(fieldIndex, alias);
					throw new AssertionError("SUM() is not supported for type: " + sqlTypeName);
				}
				case AVG -> new AvgReducer(fieldIndex, alias);
				case MIN -> new MinReducer<>(fieldIndex, alias);
				case MAX -> new MaxReducer<>(fieldIndex, alias);
				default -> throw new AssertionError();
			};

			fieldReducers.add(fieldReducer);
		}

		RecordReducer recordReducer = RecordReducer.create(current.getScheme(), fieldReducers);
		Function<Record, Record> keyFunction = getKeyFunction(groupSet.toList());

		RecordScheme accumulatorScheme = recordReducer.getAccumulatorScheme();
		RecordScheme outputScheme = recordReducer.getOutputScheme();
		return UnmaterializedDataset.of(
				outputScheme,
				params -> Datasets.sortReduceRepartitionReduce(current.materialize(params), recordReducer,
						Record.class, keyFunction, RecordKeyComparator.getInstance(),
						RecordStreamSchema.create(accumulatorScheme),
						keyFunction,
						RecordStreamSchema.create(outputScheme)));
	}

	private static Function<Record, Record> getKeyFunction(List<Integer> indices) {
		List<FieldProjection> projections = new ArrayList<>(indices.size());
		for (Integer index : indices) {
			projections.add(new FieldProjection(recordField(index), String.valueOf(index)));
		}
		return RecordProjectionFn.create(projections);
	}

	private UnmaterializedDataset handle(LogicalValues values, ParamsCollector paramsCollector) {
		ImmutableList<ImmutableList<RexLiteral>> tuples = values.getTuples();

		List<Dataset<Record>> singleDatasets = new ArrayList<>(tuples.size());
		RecordScheme scheme = null;
		for (ImmutableList<RexLiteral> tuple : tuples) {
			SortedDataset<Record, Record> singleDummyDataset = Utils.singleDummyDataset();
			List<FieldProjection> projections = new ArrayList<>();
			for (RexLiteral field : tuple) {
				projections.add(new FieldProjection(paramsCollector.toOperand(field), null));
			}
			RecordProjectionFn projectionFn = RecordProjectionFn.create(projections);
			if (scheme == null) {
				scheme = projectionFn.getToScheme(RecordScheme.builder(classLoader).build(), null);
			}
			singleDatasets.add(Datasets.map(singleDummyDataset, projectionFn, RecordStreamSchema.create(scheme)));
		}

		assert scheme != null;

		if (singleDatasets.size() == 1) {
			return UnmaterializedDataset.of(scheme, $ -> singleDatasets.get(0));
		}

		Dataset<Record> union = singleDatasets.get(0);
		for (int i = 1; i < singleDatasets.size(); i++) {
			Dataset<Record> next = singleDatasets.get(i);

			union = Datasets.unionAll(union, next);
		}

		Dataset<Record> finalUnion = union;
		return UnmaterializedDataset.of(scheme, $ -> finalUnion);
	}

	private UnmaterializedDataset handle(LogicalUnion union, ParamsCollector paramsCollector) {
		UnmaterializedDataset left = handle(union.getInputs().get(0), paramsCollector);
		UnmaterializedDataset right = handle(union.getInputs().get(1), paramsCollector);

		return UnmaterializedDataset.of(left.getScheme(), params -> {
			Dataset<Record> leftDataset = left.materialize(params);
			Dataset<Record> rightDataset = right.materialize(params);

			// Make sure field names match
			rightDataset = Datasets.map(rightDataset, RecordProjectionFn.rename(left.getScheme()), RecordStreamSchema.create(left.getScheme()));

			if (union.all) {
				return Datasets.unionAll(leftDataset, rightDataset);
			}

			SortedDataset<Record, Record> sortedLeft = sortForUnion(leftDataset, Datasets::repartitionSort);
			SortedDataset<Record, Record> sortedRight = sortForUnion(rightDataset, Datasets::castToSorted);

			return Datasets.union(sortedLeft, sortedRight);
		});
	}

	@SuppressWarnings("ConstantConditions")
	private UnmaterializedDataset handle(LogicalSort sort, ParamsCollector paramsCollector) {
		UnmaterializedDataset current = handle(sort.getInput(), paramsCollector);
		RecordScheme scheme = current.getScheme();

		List<RelDataTypeField> fieldList = sort.getRowType().getFieldList();
		List<FieldSort> sorts = new ArrayList<>(fieldList.size());
		for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
			int fieldIndex = fieldCollation.getFieldIndex();
			Type fieldType = scheme.getFieldType(fieldIndex);

			if (!Utils.isSortable(Types.getRawType(fieldType))) {
				throw new IllegalArgumentException("Field: '" + scheme.getField(fieldIndex) + "' cannot be ordered");
			}

			Direction direction = fieldCollation.getDirection();

			boolean asc;
			if (direction == Direction.ASCENDING) asc = true;
			else if (direction == Direction.DESCENDING) asc = false;
			else throw new IllegalArgumentException("Unsupported sort direction: " + direction);

			sorts.add(new FieldSort(fieldIndex, asc, fieldCollation.nullDirection));
		}

		Scalar offset = sort.offset == null ?
				new Scalar(Value.materializedValue(int.class, Skip.NO_SKIP)) :
				paramsCollector.toScalarOperand(sort.offset);

		Scalar limit = sort.fetch == null ?
				new Scalar(Value.materializedValue(int.class, Limiter.NO_LIMIT)) :
				paramsCollector.toScalarOperand(sort.fetch);

		return UnmaterializedDataset.of(
				scheme,
				params -> {
					Dataset<Record> materializedDataset = current.materialize(params);

					long offsetValue = ((Number) offset.materialize(params).value.getValue()).longValue();
					long limitValue = ((Number) limit.materialize(params).value.getValue()).longValue();


					if (sorts.isEmpty()) {
						return Datasets.offsetLimit(materializedDataset, IdentityFunction.getInstance(), offsetValue, limitValue);
					}

					LocallySortedDataset<Record, Record> sorted = Datasets.localSort(
							materializedDataset,
							Record.class,
							IdentityFunction.getInstance(),
							new RecordSortComparator(sorts)
					);

					if (offsetValue == Skip.NO_SKIP && limitValue == Limiter.NO_LIMIT) {
						return sorted;
					}

					return Datasets.offsetLimit(sorted, offsetValue, limitValue);
				}
		);
	}

	private static JoinKeyProjections getJoinKeyProjections(RecordScheme leftScheme, RecordScheme rightScheme, LogicalJoin join) {
		RexNode condition = join.getCondition();
		List<RexCall> conditions = flattenJoinConditions(condition);

		List<FieldProjection> leftProjections = new ArrayList<>(conditions.size());
		List<FieldProjection> rightProjections = new ArrayList<>(conditions.size());

		for (RexCall eqCondition : conditions) {
			List<RexNode> operands = eqCondition.getOperands();

			RexNode firstOperand = operands.get(0);
			RexNode secondOperand = operands.get(1);

			RexInputRef firstInputNode = (RexInputRef) firstOperand;
			RexInputRef secondInputNode = (RexInputRef) secondOperand;

			int firstIndex = firstInputNode.getIndex();
			int secondIndex = secondInputNode.getIndex();

			int leftIndex, rightIndex;
			if (secondIndex > firstIndex) {
				leftIndex = firstIndex;
				rightIndex = secondIndex - leftScheme.size();
			} else {
				leftIndex = secondIndex;
				rightIndex = firstIndex - leftScheme.size();
			}

			Class<?> leftClass = Types.getRawType(leftScheme.getFieldType(leftIndex));
			Class<?> rightClass = Types.getRawType(rightScheme.getFieldType(rightIndex));

			checkArgument(leftClass == rightClass);
			checkArgument(Utils.isSortable(leftClass), "Column not sortable");

			leftProjections.add(new FieldProjection(recordField(leftIndex), "join_" + leftProjections.size()));
			rightProjections.add(new FieldProjection(recordField(rightIndex), "join_" + rightProjections.size()));
		}

		return new JoinKeyProjections(
				RecordProjectionFn.create(leftProjections),
				RecordProjectionFn.create(rightProjections)
		);
	}

	private static List<RexCall> flattenJoinConditions(RexNode condition) {
		if (condition.getKind() == SqlKind.EQUALS) {
			RexCall eqCondition = (RexCall) condition;
			List<RexNode> operands = eqCondition.getOperands();
			if (operands.size() != 2) {
				throw new IllegalArgumentException("Illegal number of EQ operands");
			}
			for (RexNode operand : operands) {
				if (operand.getKind() != SqlKind.INPUT_REF) {
					throw new IllegalArgumentException("Unsupported join condition: " + operand +
							". Only equi-joins are supported");
				}
			}
			return List.of(eqCondition);
		}

		if (condition.getKind() == SqlKind.AND) {
			List<RexCall> result = new ArrayList<>();
			for (RexNode operand : ((RexCall) condition).getOperands()) {
				result.addAll(flattenJoinConditions(operand));
			}
			return result;
		}

		throw new IllegalArgumentException("Unsupported join condition: " + condition +
				". Only equi-joins are supported");
	}

	public record ConversionResult(UnmaterializedDataset unmaterializedDataset, List<RexDynamicParam> dynamicParams) {
	}

	private UnmaterializedDataset filter(UnmaterializedDataset dataset, RexNode conditionNode, ParamsCollector paramsCollector) {
		SqlKind kind = conditionNode.getKind();

		if (kind == SqlKind.LITERAL) {
			RexLiteral literal = (RexLiteral) conditionNode;
			if (literal.isAlwaysFalse()) {
				return UnmaterializedDataset.of(dataset.getScheme(), $ -> Datasets.empty(RecordStreamSchema.create(dataset.getScheme())));
			} else //noinspection StatementWithEmptyBody
				if (literal.isAlwaysTrue()) {
					// Do nothing
				} else {
					throw new IllegalArgumentException("Unknown literal: " + literal.getValueAs(Object.class));
				}
		} else if (conditionNode instanceof RexCall call) {
			WherePredicate wherePredicate = paramsCollector.toWherePredicate(call);
			return UnmaterializedDataset.of(
					dataset.getScheme(),
					params -> Datasets.filter(dataset.materialize(params), wherePredicate.materialize(params)));
		} else {
			throw new IllegalArgumentException("Unknown condition: " + kind);
		}
		return dataset;
	}

	private boolean isSynthetic(String fieldName) {
		return fieldName.contains("$");
	}

	private static SortedDataset<Record, Record> sortForJoin(
			RecordProjectionFn keyFunction, Dataset<Record> dataset,
			Function<LocallySortedDataset<Record, Record>, SortedDataset<Record, Record>> toSortedFn
	) {
		return toSortedFn.apply(Datasets.localSort(dataset, Record.class, keyFunction, RecordKeyComparator.getInstance()));
	}

	private static SortedDataset<Record, Record> sortForUnion(Dataset<Record> dataset,
			Function<LocallySortedDataset<Record, Record>, SortedDataset<Record, Record>> toSortedFn
	) {
		return toSortedFn.apply(Datasets.localSort(dataset, Record.class, IdentityFunction.getInstance(), RecordKeyComparator.getInstance()));
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
		throw new AssertionError();
	}

	private static boolean checkReSortIndices(List<FieldProjection> projections, List<Integer> indices) {
		int size = projections.size();
		for (Integer index : indices) {
			if (index >= size) return true;

			FieldProjection projection = projections.get(index);
			if (!(projection.operand() instanceof RecordField operandField) || operandField.index != index) {
				return true;
			}
		}
		return false;
	}

	private static <K> SortedDataset<K, Record> mapAsSorted(RecordScheme toScheme, RecordProjectionFn projectionFn, LocallySortedDataset<K, Record> locallySortedDataset) {
		Dataset<Record> mapped = Datasets.map(locallySortedDataset, projectionFn, RecordStreamSchema.create(toScheme));
		Class<K> keyType = locallySortedDataset.keyType();
		Function<Record, K> keyFunction = locallySortedDataset.keyFunction();
		return Datasets.castToSorted(mapped, keyType, keyFunction, locallySortedDataset.keyComparator());
	}

	private static <K> Dataset<Record> repartitionMap(RecordScheme toScheme, RecordProjectionFn projectionFn, LocallySortedDataset<K, Record> locallySortedDataset) {
		Dataset<Record> repartitioned = new RepartitionToSingleDataset<>(locallySortedDataset);
		return Datasets.map(repartitioned, projectionFn, RecordStreamSchema.create(toScheme));
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

	public static final class RepartitionToSingleDataset<K> extends LocallySortedDataset<K, Record> {
		private final Dataset<Record> input;

		private final int sharderNonce = ThreadLocalRandom.current().nextInt();

		private RepartitionToSingleDataset(LocallySortedDataset<K, Record> input) {
			super(input.streamSchema(), input.keyComparator(), input.keyType(), input.keyFunction());
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

			List<StreamId> newStreamIds = DatasetUtils.repartitionAndReduce(next, streamIds, streamSchema(), keyFunction(), keyComparator(), mergeReducer(), List.of(randomPartition));
			assert newStreamIds.size() == 1;

			return newStreamIds;
		}

		@Override
		public Collection<Dataset<?>> getBases() {
			return List.of(input);
		}
	}

	public record JoinKeyProjections(RecordProjectionFn leftKeyProjection, RecordProjectionFn rightKeyProjection) {
	}

	public final class ParamsCollector {
		private final Set<RexDynamicParam> params = new TreeSet<>(Comparator.comparingInt(RexDynamicParam::getIndex));

		public List<RexDynamicParam> getParams() {
			return List.copyOf(params);
		}

		private WherePredicate toWherePredicate(RexCall conditionNode) {
			List<RexNode> operands = conditionNode.getOperands();
			return switch (conditionNode.getKind()) {
				case OR -> WherePredicates.or(operands.stream()
						.map(rexNode -> toWherePredicate((RexCall) rexNode))
						.collect(Collectors.toList()));
				case AND -> WherePredicates.and(operands.stream()
						.map(rexNode -> toWherePredicate((RexCall) rexNode))
						.collect(Collectors.toList()));
				case EQUALS -> WherePredicates.eq(toOperand(operands.get(0)), toOperand(operands.get(1)));
				case NOT_EQUALS -> WherePredicates.notEq(toOperand(operands.get(0)), toOperand(operands.get(1)));
				case GREATER_THAN -> WherePredicates.gt(toOperand(operands.get(0)), toOperand(operands.get(1)));
				case GREATER_THAN_OR_EQUAL -> WherePredicates.ge(toOperand(operands.get(0)), toOperand(operands.get(1)));
				case LESS_THAN -> WherePredicates.lt(toOperand(operands.get(0)), toOperand(operands.get(1)));
				case LESS_THAN_OR_EQUAL -> WherePredicates.le(toOperand(operands.get(0)), toOperand(operands.get(1)));
				case BETWEEN ->
						WherePredicates.between(toOperand(operands.get(0)), toOperand(operands.get(1)), toOperand(operands.get(2)));
				case IN -> {
					List<Operand<?>> options = operands.subList(1, operands.size())
							.stream()
							.map(this::toOperand)
							.collect(Collectors.toList());
					yield WherePredicates.in(toOperand(operands.get(0)), options);
				}
				case LIKE -> WherePredicates.like(toOperand(operands.get(0)), toOperand(operands.get(1)));
				case IS_NULL -> WherePredicates.isNull(toOperand(operands.get(0)));
				case IS_NOT_NULL -> WherePredicates.isNotNull(toOperand(operands.get(0)));

				default -> throw new IllegalArgumentException("Not supported condition:" + conditionNode.getKind());
			};
		}

		private Operand<?> toOperand(RexNode node) {
			Operand<?> operand = Utils.toOperand(node, classLoader);
			params.addAll(operand.getParams());
			return operand;
		}

		private Scalar toScalarOperand(RexNode node) {
			Operand<?> operand = toOperand(node);
			checkArgument(operand instanceof Scalar, "Not scalar operand");
			return (Scalar) operand;
		}
	}
}
