package io.activej.dataflow.calcite.jdbc.client;

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.calcite.avatica.remote.JsonService.MAPPER;

class DataflowResultSet extends AvaticaResultSet {

	private static final String LIST_CLASS_NAME = List.class.getName();
	private static final String MAP_CLASS_NAME = Map.class.getName();
	private static final Map<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
	private static final Class<?> UNKNOWN_CLASS = DataflowResultSet.class;

	DataflowResultSet(AvaticaStatement statement,
			Meta.Signature signature,
			ResultSetMetaData resultSetMetaData, TimeZone timeZone,
			Meta.Frame firstFrame) throws SQLException {
		super(statement, null, signature, resultSetMetaData, timeZone, firstFrame);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		int column = findColumn(columnLabel);
		return getObject(column);
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		Object result = super.getObject(columnIndex);

		ColumnMetaData columnMetaData = signature.columns.get(columnIndex - 1);
		String className = columnMetaData.columnClassName;
		if (LIST_CLASS_NAME.equals(className) || MAP_CLASS_NAME.equals(className)) {
			return result;
		}

		Class<?> cls = CLASS_CACHE.computeIfAbsent(className, $ -> {
			try {
				return Class.forName(className);
			} catch (ClassNotFoundException e) {
				return UNKNOWN_CLASS;
			}
		});

		if (cls == UNKNOWN_CLASS){
			return result;
		}

		return MAPPER.convertValue(result, cls);
	}
}
