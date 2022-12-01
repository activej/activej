package io.activej.dataflow.calcite.jdbc;

import io.activej.async.callback.AsyncComputation;
import io.activej.dataflow.calcite.CalciteSqlDataflow;
import io.activej.dataflow.calcite.RelToDatasetConverter.ConversionResult;
import io.activej.dataflow.calcite.RelToDatasetConverter.UnmaterializedDataset;
import io.activej.dataflow.calcite.utils.JavaRecordType;
import io.activej.dataflow.dataset.Dataset;
import io.activej.dataflow.exception.DataflowException;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.SynchronousStreamConsumer;
import io.activej.eventloop.Eventloop;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.types.Types;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public final class DataflowMeta extends LimitedMeta {
	private static final String TABLE_CAT = "TABLE_CAT";
	private static final String TABLE_SCHEM = "TABLE_SCHEM";
	private static final String TABLE_NAME = "TABLE_NAME";
	private static final String TABLE_CATALOG = "TABLE_CATALOG";
	private static final String TABLE_TYPE = "TABLE_TYPE";
	private static final String REMARKS = "REMARKS";
	private static final String TYPE_CAT = "TYPE_CAT";
	private static final String TYPE_SCHEME = "TYPE_SCHEME";
	private static final String TYPE_NAME = "TYPE_NAME";
	private static final String SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME";
	private static final String REF_GENERATION = "REF_GENERATION";
	private static final String DATA_TYPE = "DATA_TYPE";
	private static final String COLUMN_NAME = "COLUMN_NAME";
	private static final String COLUMN_SIZE = "COLUMN_SIZE";
	private static final String COLUMN_DEF = "COLUMN_DEF";
	private static final String BUFFER_LENGTH = "BUFFER_LENGTH";
	private static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
	private static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
	private static final String NULLABLE = "NULLABLE";
	private static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
	private static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
	private static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
	private static final String ORDINAL_POSITION = "ORDINAL_POSITION";
	private static final String IS_NULLABLE = "IS_NULLABLE";
	private static final String SCOPE_CATALOG = "SCOPE_CATALOG";
	private static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
	private static final String SCOPE_TABLE = "SCOPE_TABLE";
	private static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
	private static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
	private static final String IS_GENERATEDCOLUMN = "IS_GENERATEDCOLUMN";
	private static final String TABLE = "TABLE";
	private static final String NO = "NO";
	private static final String YES = "YES";

	private static final String SCHEMA_NAME = "dataflow";

	private static final String EXPLAIN = "EXPLAIN";
	private static final String EXPLAIN_PLAN = "EXPLAIN PLAN ";
	private static final String EXPLAIN_GRAPH = "EXPLAIN GRAPH ";
	private static final String EXPLAIN_NODES = "EXPLAIN NODES ";

	private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

	private final Eventloop eventloop;
	private final CalciteSqlDataflow sqlDataflow;
	private final Map<String, Integer> statementIds = new ConcurrentHashMap<>();
	private final Map<String, Map<Integer, FrameFetcher>> fetchers = new ConcurrentHashMap<>();
	private final Map<String, Map<Integer, UnmaterializedDataset>> unmaterializedDatasets = new ConcurrentHashMap<>();

	private DataflowMeta(Eventloop eventloop, CalciteSqlDataflow sqlDataflow) {
		this.eventloop = eventloop;
		this.sqlDataflow = sqlDataflow;
	}

	public static DataflowMeta create(Eventloop eventloop, CalciteSqlDataflow sqlDataflow) {
		return new DataflowMeta(eventloop, sqlDataflow);
	}

	@Override
	public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
		Map<Integer, UnmaterializedDataset> connectionDatasets = unmaterializedDatasets.get(ch.id);
		if (connectionDatasets == null) {
			throw new RuntimeException("Unknown connection: " + ch);
		}

		StatementHandle statement = createStatement(ch);
		TransformationResult transformed = transform(sql);
		ConversionResult conversionResult = transformed.conversionResult();
		statement.signature = createSignature(sql, transformed.fields(), conversionResult.dynamicParams(), conversionResult.unmaterializedDataset().getScheme());

		connectionDatasets.put(statement.id, conversionResult.unmaterializedDataset());
		return statement;
	}

	@Override
	public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
		Map<Integer, UnmaterializedDataset> connectionDatasets = unmaterializedDatasets.get(h.connectionId);
		if (connectionDatasets == null) {
			throw new RuntimeException("Unknown connection: " + h.connectionId);
		}

		UnmaterializedDataset unmaterializedDataset = connectionDatasets.get(h.id);
		if (unmaterializedDataset == null) {
			throw new NoSuchStatementException(h);
		}

		List<Object> params = parameterValues.stream()
				.map(this::paramToJdbc)
				.toList();

		Dataset<Record> dataset = unmaterializedDataset.materialize(params);

		FrameFetcher frameFetcher = createFrameFetcher(h, dataset);

		Frame firstFrame = frameFetcher.fetch(0, maxRowsInFirstFrame);

		MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, h.signature, firstFrame);
		return new ExecuteResult(List.of(metaResultSet));
	}

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback) {
		if (sql.toUpperCase().startsWith(EXPLAIN)) {
			return handleExplainQuery(h, sql);
		}

		TransformationResult transformed = transform(sql);
		ConversionResult conversionResult = transformed.conversionResult();
		UnmaterializedDataset unmaterialized = conversionResult.unmaterializedDataset();
		h.signature = createSignature(sql, transformed.fields(), Collections.emptyList(), unmaterialized.getScheme());

		Dataset<Record> dataset = unmaterialized.materialize(Collections.emptyList());

		FrameFetcher frameFetcher = createFrameFetcher(h, dataset);

		Frame firstFrame = frameFetcher.fetch(0, maxRowsInFirstFrame);

		MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, h.signature, firstFrame);
		return new ExecuteResult(List.of(metaResultSet));
	}

	private static final LinkedHashMap<String, Class<?>> EXPLAIN_QUERY_COLUMNS = new LinkedHashMap<>();

	static {
		EXPLAIN_QUERY_COLUMNS.put(EXPLAIN, String.class);
	}

	private ExecuteResult handleExplainQuery(StatementHandle h, String sql) {
		String upperCaseSql = sql.toUpperCase();
		String explainString;
		try {
			if (upperCaseSql.startsWith(EXPLAIN_PLAN)) {
				explainString = sqlDataflow.explainPlan(sql.substring(EXPLAIN_PLAN.length()));
			} else if (upperCaseSql.startsWith(EXPLAIN_GRAPH)) {
				explainString = sqlDataflow.explainGraph(sql.substring(EXPLAIN_GRAPH.length()));
			} else if (upperCaseSql.startsWith(EXPLAIN_NODES)) {
				explainString = eventloop.submit(AsyncComputation.of(() -> sqlDataflow.explainNodes(sql.substring(EXPLAIN_GRAPH.length())))).get();
			} else {
				throw new RuntimeException("Unknown EXPLAIN query, only `EXPLAIN PLAN`, `EXPLAIN GRAPH` and `EXPLAIN NODES` queries are supported");
			}
		} catch (SqlParseException | DataflowException | ExecutionException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}

		MetaResultSet resultSet = createMetaResponse(h, EXPLAIN_QUERY_COLUMNS, List.<Object[]>of(new Object[]{explainString}));
		return new ExecuteResult(List.of(resultSet));
	}

	private FrameFetcher createFrameFetcher(StatementHandle statement, Dataset<Record> dataset) {
		FrameFetcher frameFetcher;
		try {
			frameFetcher = eventloop.submit((AsyncComputation<FrameFetcher>) cb -> {
				StreamSupplier<Record> supplier = sqlDataflow.queryDataflow(dataset);
				SynchronousStreamConsumer<Record> recordConsumer = SynchronousStreamConsumer.create();
				supplier.streamTo(recordConsumer);
				cb.accept(new FrameFetcher(recordConsumer, statement.signature.columns.size()), null);
			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}

		fetchers.get(statement.connectionId).put(statement.id, frameFetcher);

		return frameFetcher;
	}

	private Signature createSignature(String sql, List<RelDataTypeField> fields, List<RexDynamicParam> dynamicParams, RecordScheme scheme) {
		int fieldCount = scheme.size();
		List<ColumnMetaData> columns = new ArrayList<>(fieldCount);
		for (int i = 0; i < fieldCount; i++) {
			Type fieldType = scheme.getFieldType(i);
			String fieldName = scheme.getField(i);
			RelDataType relDataType = fields.get(i).getType();
			ColumnMetaData.AvaticaType avaticaType = getAvaticaType(relDataType, fieldType);

			ColumnMetaData columnMetaData = new ColumnMetaData(i, false, true, false, false, 0, false,
					1, fieldName, fieldName, SCHEMA_NAME,
					getPrecision(relDataType),
					getScale(relDataType),
					"", SCHEMA_NAME,
					avaticaType, true, false, false, Types.getRawType(fieldType).getName());
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
				CursorFactory.LIST,
				StatementType.SELECT);
	}

	private static ColumnMetaData.AvaticaType getAvaticaType(RelDataType relDataType, @Nullable Type type) {
		if (relDataType instanceof MapSqlType mapSqlType) {
			Type keyType = null;
			Type valueType = null;
			if (type instanceof ParameterizedType parameterizedType) {
				Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
				if (actualTypeArguments.length == 2) {
					keyType = actualTypeArguments[0];
					valueType = actualTypeArguments[1];
				}
			}

			ColumnMetaData.AvaticaType keyAvaticaType = getAvaticaType(mapSqlType.getKeyType(), keyType);
			ColumnMetaData.AvaticaType valueAvaticaType = getAvaticaType(mapSqlType.getValueType(), valueType);

			return ColumnMetaData.scalar(SqlType.MAP.id, "MAP(" + keyAvaticaType.name + "," + valueAvaticaType.name + ")", ColumnMetaData.Rep.OBJECT);
		}
		if (relDataType instanceof ArraySqlType arraySqlType) {
			Type componentType = null;
			if (type instanceof ParameterizedType parameterizedType) {
				Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
				if (actualTypeArguments.length == 1) {
					componentType = actualTypeArguments[0];
				}
			}

			ColumnMetaData.AvaticaType componentAvaticaType = getAvaticaType(arraySqlType.getComponentType(), componentType);

			return ColumnMetaData.array(componentAvaticaType, "ARRAY(" + componentAvaticaType.name + ")", ColumnMetaData.Rep.ARRAY);
		}

		if (type != null) {
			ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(type);
			return ColumnMetaData.scalar(rep.typeId, type.getTypeName(), rep);
		}
		int jdbcOrdinal = relDataType.getSqlTypeName().getJdbcOrdinal();
		SqlType sqlType = SqlType.valueOf(jdbcOrdinal);
		ColumnMetaData.Rep rep = ColumnMetaData.Rep.nonPrimitiveRepOf(sqlType);

		String typeName;
		if (relDataType instanceof RelDataTypeFactoryImpl.JavaType javaType) {
			typeName = javaType.getJavaClass().getName();
		} else if (relDataType instanceof JavaRecordType javaRecordType) {
			typeName = javaRecordType.getClazz().getName();
		} else {
			typeName = rep.name();
		}
		return ColumnMetaData.scalar(rep.typeId, typeName, rep);
	}

	private TransformationResult transform(String sql) {
		RelNode node;
		try {
			node = sqlDataflow.convertToNode(sql);
		} catch (DataflowException | SqlParseException e) {
			throw new RuntimeException(e);
		}

		return new TransformationResult(node.getRowType().getFieldList(), sqlDataflow.convert(node));
	}

	@Override
	public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException {
		Map<Integer, FrameFetcher> connectionFetchers = fetchers.get(h.connectionId);
		if (connectionFetchers == null) {
			throw new RuntimeException("Unknown connection: " + h.connectionId);
		}

		FrameFetcher frameFetcher = connectionFetchers.get(h.id);
		if (frameFetcher == null) {
			throw new NoSuchStatementException(h);
		}
		return frameFetcher.fetch(offset, fetchMaxRowCount);
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
		unmaterializedDatasets.put(ch.id, new HashMap<>());
		fetchers.put(ch.id, new HashMap<>());
	}

	@Override
	public void closeConnection(ConnectionHandle ch) {
		statementIds.remove(ch.id);

		unmaterializedDatasets.remove(ch.id);

		Map<Integer, FrameFetcher> connectionFetchers = fetchers.remove(ch.id);
		if (connectionFetchers == null) return;

		try {
			eventloop.submit(() -> {
				for (FrameFetcher frameFetcher : connectionFetchers.values()) {
					frameFetcher.close();
				}
			}).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
		return connProps;
	}

	@Override
	public void closeStatement(StatementHandle h) {
		Map<Integer, UnmaterializedDataset> connectionDatasets = unmaterializedDatasets.get(h.connectionId);
		if (connectionDatasets != null) {
			connectionDatasets.remove(h.id);
		}

		Map<Integer, FrameFetcher> connectionFetchers = fetchers.get(h.connectionId);
		if (connectionFetchers == null) return;

		FrameFetcher frameFetcher = connectionFetchers.remove(h.id);
		if (frameFetcher == null) return;

		try {
			eventloop.submit(frameFetcher::close).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	private static final LinkedHashMap<String, Class<?>> GET_CATALOGS_COLUMNS = new LinkedHashMap<>();

	static {
		GET_CATALOGS_COLUMNS.put(TABLE_CAT, String.class);
	}

	@Override
	public MetaResultSet getCatalogs(ConnectionHandle ch) {
		return createMetaResponse(createStatement(ch), GET_CATALOGS_COLUMNS, List.<Object[]>of(new Object[]{SCHEMA_NAME}));
	}

	private static final LinkedHashMap<String, Class<?>> GET_SCHEMAS_COLUMNS = new LinkedHashMap<>();

	static {
		GET_SCHEMAS_COLUMNS.put(TABLE_SCHEM, String.class);
		GET_SCHEMAS_COLUMNS.put(TABLE_CATALOG, String.class);
	}

	@Override
	public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
		return createMetaResponse(createStatement(ch), GET_SCHEMAS_COLUMNS, Collections.emptyList());
	}

	private static final LinkedHashMap<String, Class<?>> GET_TABLE_TYPES_COLUMNS = new LinkedHashMap<>();

	static {
		GET_TABLE_TYPES_COLUMNS.put(TABLE_TYPE, String.class);
	}

	@Override
	public MetaResultSet getTableTypes(ConnectionHandle ch) {
		return createMetaResponse(createStatement(ch), GET_TABLE_TYPES_COLUMNS, List.<Object[]>of(new Object[]{TABLE}));
	}

	private static final LinkedHashMap<String, Class<?>> GET_TABLES_COLUMNS = new LinkedHashMap<>();

	static {
		GET_TABLES_COLUMNS.put(TABLE_CAT, String.class);
		GET_TABLES_COLUMNS.put(TABLE_SCHEM, String.class);
		GET_TABLES_COLUMNS.put(TABLE_NAME, String.class);
		GET_TABLES_COLUMNS.put(TABLE_TYPE, String.class);
		GET_TABLES_COLUMNS.put(REMARKS, String.class);
		GET_TABLES_COLUMNS.put(TYPE_CAT, String.class);
		GET_TABLES_COLUMNS.put(TYPE_SCHEME, String.class);
		GET_TABLES_COLUMNS.put(TYPE_NAME, String.class);
		GET_TABLES_COLUMNS.put(SELF_REFERENCING_COL_NAME, String.class);
		GET_TABLES_COLUMNS.put(REF_GENERATION, String.class);
	}

	@Override
	public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
		List<Object[]> results = new ArrayList<>();
		//noinspection PointlessBooleanExpression
		if (true &&
				(catalog == null || catalog.equals(SCHEMA_NAME)) &&
				(schemaPattern.s == null || schemaPattern.s.isEmpty()) &&
				(typeList == null || typeList.contains(TABLE))
		) {
			Predicate<String> tableNamePredicate = patternToPredicate(tableNamePattern);
			for (String tableName : sqlDataflow.getSchema().getTableNames()) {
				if (!tableNamePredicate.test(tableName)) continue;

				Object[] row = new Object[10];
				row[0] = SCHEMA_NAME;
				row[2] = tableName;
				row[3] = TABLE;
				results.add(row);
			}
		}

		return createMetaResponse(createStatement(ch), GET_TABLES_COLUMNS, results);
	}

	private static final LinkedHashMap<String, Class<?>> GET_COLUMNS_COLUMNS = new LinkedHashMap<>();

	static {
		GET_COLUMNS_COLUMNS.put(TABLE_CAT, String.class); // 0
		GET_COLUMNS_COLUMNS.put(TABLE_SCHEM, String.class); // 1
		GET_COLUMNS_COLUMNS.put(TABLE_NAME, String.class); // 2
		GET_COLUMNS_COLUMNS.put(COLUMN_NAME, String.class); // 3
		GET_COLUMNS_COLUMNS.put(DATA_TYPE, Integer.class); // 4
		GET_COLUMNS_COLUMNS.put(TYPE_NAME, String.class); // 5
		GET_COLUMNS_COLUMNS.put(COLUMN_SIZE, Integer.class); // 6
		GET_COLUMNS_COLUMNS.put(BUFFER_LENGTH, Integer.class); // 7
		GET_COLUMNS_COLUMNS.put(DECIMAL_DIGITS, Integer.class); // 8
		GET_COLUMNS_COLUMNS.put(NUM_PREC_RADIX, Integer.class); // 9
		GET_COLUMNS_COLUMNS.put(NULLABLE, Integer.class); // 10
		GET_COLUMNS_COLUMNS.put(REMARKS, String.class); // 11
		GET_COLUMNS_COLUMNS.put(COLUMN_DEF, String.class); // 12
		GET_COLUMNS_COLUMNS.put(SQL_DATA_TYPE, Integer.class); // 13
		GET_COLUMNS_COLUMNS.put(SQL_DATETIME_SUB, Integer.class); // 14
		GET_COLUMNS_COLUMNS.put(CHAR_OCTET_LENGTH, Integer.class); // 15
		GET_COLUMNS_COLUMNS.put(ORDINAL_POSITION, Integer.class); // 16
		GET_COLUMNS_COLUMNS.put(IS_NULLABLE, String.class); // 17
		GET_COLUMNS_COLUMNS.put(SCOPE_CATALOG, String.class); // 18
		GET_COLUMNS_COLUMNS.put(SCOPE_SCHEMA, String.class); // 19
		GET_COLUMNS_COLUMNS.put(SCOPE_TABLE, String.class); // 20
		GET_COLUMNS_COLUMNS.put(SOURCE_DATA_TYPE, Integer.class); // 21
		GET_COLUMNS_COLUMNS.put(IS_AUTOINCREMENT, String.class); // 22
		GET_COLUMNS_COLUMNS.put(IS_GENERATEDCOLUMN, String.class); //23
	}

	@Override
	public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
		List<Object[]> results = new ArrayList<>();
		CalciteSchema schema = sqlDataflow.getSchema();
		RelDataTypeFactory typeFactory = sqlDataflow.getTypeFactory();

		if ((catalog == null || catalog.equals(SCHEMA_NAME)) && (schemaPattern.s == null || schemaPattern.s.isEmpty())) {
			Predicate<String> tableNamePredicate = patternToPredicate(tableNamePattern);
			Predicate<String> columnNamePredicate = patternToPredicate(columnNamePattern);
			for (String tableName : schema.getTableNames()) {
				if (!tableNamePredicate.test(tableName)) continue;

				CalciteSchema.TableEntry entry = schema.getTable(tableName, false);
				assert entry != null;
				RelDataType dataType = entry.getTable().getRowType(typeFactory);
				List<RelDataTypeField> fieldList = dataType.getFieldList();

				for (RelDataTypeField field : fieldList) {
					String fieldName = field.getName();
					if (!columnNamePredicate.test(fieldName)) continue;

					Object[] row = new Object[24];
					RelDataType fieldType = field.getType();
					SqlTypeName sqlTypeName = fieldType.getSqlTypeName();

					int precision =
							sqlTypeName.allowsPrec()
									&& !(fieldType
									instanceof RelDataTypeFactoryImpl.JavaType)
									? fieldType.getPrecision()
									: -1;

					row[0] = SCHEMA_NAME;
					row[2] = tableName;
					row[3] = fieldName;
					row[4] = sqlTypeName.getJdbcOrdinal();
					row[5] = sqlTypeName.getName();
					row[6] = precision;
					row[8] = sqlTypeName.allowsScale()
							? fieldType.getScale()
							: null;
					row[9] = 10;

					boolean isNullable = field.getType().isNullable();
					row[10] = isNullable
							? DatabaseMetaData.columnNullable
							: DatabaseMetaData.columnNoNulls;
					row[15] = precision;
					row[16] = field.getIndex() + 1;
					row[17] = isNullable ? YES : NO;
					row[22] = NO;
					row[23] = NO;

					results.add(row);
				}
			}
		}

		return createMetaResponse(createStatement(ch), GET_COLUMNS_COLUMNS, results);
	}

	private MetaResultSet createMetaResponse(StatementHandle h, LinkedHashMap<String, Class<?>> columnNamesToTypes, List<Object[]> frameRows) {
		assert frameRows.stream().allMatch(objects -> objects.length == columnNamesToTypes.size());

		Signature signature = createMetaSignature(columnNamesToTypes);
		Frame frame = Frame.create(0, true, new ArrayList<>(frameRows));

		return MetaResultSet.create(h.connectionId, h.id, true, signature, frame);
	}

	private static Signature createMetaSignature(LinkedHashMap<String, Class<?>> columnNamesToTypes) {
		List<ColumnMetaData> columnMetaDatas = new ArrayList<>(columnNamesToTypes.size());
		int i = 0;
		for (Map.Entry<String, Class<?>> entry : columnNamesToTypes.entrySet()) {
			String columnName = entry.getKey();
			Class<?> type = entry.getValue();
			String typeName = type.getName();
			ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(type);
			ColumnMetaData.ScalarType scalarType = ColumnMetaData.scalar(rep.typeId, rep.name(), rep);
			ColumnMetaData columnMetaData = new ColumnMetaData(i++, false, true, false, false, 0, false,
					1, columnName, columnName, SCHEMA_NAME,
					0,
					0,
					"", SCHEMA_NAME,
					scalarType, true, false, false, typeName);
			columnMetaDatas.add(columnMetaData);
		}
		return Signature.create(columnMetaDatas, null, null, CursorFactory.LIST, null);
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

	private static Predicate<String> patternToPredicate(Pat pattern) {
		String patternString = pattern.s;
		if (patternString == null || patternString.equals("%")) return $ -> true;

		Pattern regexPattern = Pattern.compile("^" + patternString.replaceAll("%", ".*").replaceAll("_", ".") + "$");
		return string -> regexPattern.matcher(string).matches();
	}

	private Object paramToJdbc(TypedValue typedValue) {
		Object result = typedValue.toJdbc(UTC_CALENDAR);
		if (result instanceof Timestamp timestamp) return timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC);
		if (result instanceof Date date) return date.toLocalDate().plusDays(1); // Bug with TypedValue
		if (result instanceof Time time) return time.toLocalTime();
		return result;
	}

	private record TransformationResult(List<RelDataTypeField> fields, ConversionResult conversionResult) {

	}
}
