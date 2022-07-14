package io.activej.dataflow.calcite.jdbc;

import io.activej.dataflow.DataflowException;
import io.activej.dataflow.calcite.CalciteSqlDataflow;
import io.activej.dataflow.calcite.CalciteSqlDataflow.PreparedTransformationResult;
import io.activej.dataflow.calcite.CalciteSqlDataflow.TransformationResult;
import io.activej.eventloop.Eventloop;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.types.Types;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public final class DataflowMeta extends LimitedMeta {
	private final Eventloop eventloop;
	private final CalciteSqlDataflow sqlDataflow;
	private final Map<String, Integer> statementIds = new ConcurrentHashMap<>();
	private final Map<StatementKey, FrameConsumer> consumers = new ConcurrentHashMap<>();

	public DataflowMeta(Eventloop eventloop, CalciteSqlDataflow sqlDataflow) {
		this.eventloop = eventloop;
		this.sqlDataflow = sqlDataflow;
	}

	@Override
	public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
		StatementHandle statement = createStatement(ch);
		PreparedTransformationResult transformed = transformPrepared(sql);
		statement.signature = getSignature(sql, transformed.fields(), transformed.parameters(), transformed.scheme());
		return statement;
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
		Signature signature = h.signature;
		String sql = signature.sql;
		for (TypedValue parameterValue : parameterValues) {
			String value = parameterValue.value.toString();
			Class<?> clazz = parameterValue.type.clazz;
			if (clazz == String.class || clazz == Character.class || clazz == char.class) {
				value = '\'' + value + '\'';
			}
			sql = sql.replaceFirst("\\?", value);
		}

		TransformationResult transformed = transform(sql);

		return doExecute(h, transformed, signature);
	}

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback) {
		TransformationResult transformed = transform(sql);
		Signature signature = getSignature(sql, transformed.fields(), Collections.emptyList(), transformed.scheme());

		return doExecute(h, transformed, signature);
	}

	private ExecuteResult doExecute(StatementHandle h, TransformationResult transformed, Signature signature) {
		MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, signature, null);

		FrameConsumer frameConsumer;
		try {
			frameConsumer = eventloop.submit(() -> sqlDataflow.queryDataflow(transformed.dataset())
					.map(supplier -> {
						FrameConsumer consumer = new FrameConsumer(transformed.scheme().size());
						supplier.streamTo(consumer);
						return consumer;
					})).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}

		consumers.put(StatementKey.create(h), frameConsumer);

		return new ExecuteResult(List.of(metaResultSet));
	}

	private Signature getSignature(String sql, List<RelDataTypeField> fields, List<RexDynamicParam> dynamicParams, RecordScheme scheme) {
		int fieldCount = scheme.size();
		List<ColumnMetaData> columns = new ArrayList<>(fieldCount);
		for (int i = 0; i < fieldCount; i++) {
			Type fieldType = scheme.getFieldType(i);
			ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(fieldType);
			ColumnMetaData.ScalarType scalarType = ColumnMetaData.scalar(rep.typeId, rep.name(), rep);
			String fieldName = scheme.getField(i);
			RelDataTypeField field = fields.get(i);

			ColumnMetaData columnMetaData = new ColumnMetaData(i + 1, false, true, false, false, 1, false,
					1, fieldName, fieldName, "dataflow",
					getPrecision(field.getType()),
					getScale(field.getType()),
					"", "dataflow",
					scalarType, true, false, false, Types.getRawType(fieldType).getName());
			columns.add(columnMetaData);
		}

		List<AvaticaParameter> parameters = new ArrayList<>(dynamicParams.size());
		for (RexDynamicParam dynamicParam : dynamicParams) {
			RelDataType type = dynamicParam.getType();
			parameters.add(new AvaticaParameter(false,
					getPrecision(type),
					getScale(type),
					type.getSqlTypeName().getJdbcOrdinal(),
					getTypeName(type),
					Object.class.getName(),
					dynamicParam.getName()
			));
		}

		return Signature.create(
				columns,
				sql,
				parameters,
				CursorFactory.record(Record.class, Collections.emptyList(), Collections.emptyList()), // we already know the fields
				StatementType.SELECT);
	}

	private TransformationResult transform(String sql) {
		RelNode node = convertToNode(sql);

		return sqlDataflow.transform(node);
	}

	private PreparedTransformationResult transformPrepared(String sql) {
		RelNode node = convertToNode(sql);

		return sqlDataflow.transformPrepared(node);
	}

	private RelNode convertToNode(String sql) {
		RelNode node;
		try {
			node = sqlDataflow.convertToNode(sql);
		} catch (DataflowException e) {
			throw new RuntimeException(e);
		}
		return node;
	}

	@Override
	public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
		FrameConsumer frameConsumer = consumers.get(StatementKey.create(h));
		if (frameConsumer == null) {
			throw new NoSuchStatementException(h);
		}
		return frameConsumer.fetch(offset, fetchMaxRowCount);
	}

	@Override
	public StatementHandle createStatement(ConnectionHandle ch) {
		Integer newId = statementIds.computeIfPresent(ch.id, ($, id) -> id + 1);

		if (newId == null) {
			throw new RuntimeException("Unknown connection: " + ch.id);
		}

		return new StatementHandle(ch.id, newId, null);
	}

	@Override
	public void openConnection(ConnectionHandle ch, Map<String, String> info) {
		statementIds.put(ch.id, 0);
	}

	@Override
	public void closeConnection(ConnectionHandle ch) {
		statementIds.remove(ch.id);
	}

	@Override
	public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
		return connProps;
	}

	@Override
	public void closeStatement(StatementHandle h) {
		FrameConsumer frameConsumer = consumers.get(StatementKey.create(h));
		if (frameConsumer == null) return;

		try {
			eventloop.submit(frameConsumer::close).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	private static int getScale(RelDataType type) {
		return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
				? 0
				: type.getScale();
	}

	private static int getPrecision(RelDataType type) {
		return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
				? 0
				: type.getPrecision();
	}

	private static String getTypeName(RelDataType type) {
		final SqlTypeName sqlTypeName = type.getSqlTypeName();
		return switch (sqlTypeName) {
			case ARRAY, MULTISET, MAP, ROW -> type.toString();
			case INTERVAL_YEAR_MONTH -> "INTERVAL_YEAR_TO_MONTH";
			case INTERVAL_DAY_HOUR -> "INTERVAL_DAY_TO_HOUR";
			case INTERVAL_DAY_MINUTE -> "INTERVAL_DAY_TO_MINUTE";
			case INTERVAL_DAY_SECOND -> "INTERVAL_DAY_TO_SECOND";
			case INTERVAL_HOUR_MINUTE -> "INTERVAL_HOUR_TO_MINUTE";
			case INTERVAL_HOUR_SECOND -> "INTERVAL_HOUR_TO_SECOND";
			case INTERVAL_MINUTE_SECOND -> "INTERVAL_MINUTE_TO_SECOND";
			default -> sqlTypeName.getName();
		};
	}

	private record StatementKey(String connectionId, int statementId) {
		static StatementKey create(StatementHandle h) {
			return new StatementKey(h.connectionId, h.id);
		}
	}
}
