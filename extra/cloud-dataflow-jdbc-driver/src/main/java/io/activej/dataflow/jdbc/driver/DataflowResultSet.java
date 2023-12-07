package io.activej.dataflow.jdbc.driver;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.jetbrains.annotations.Nullable;

import java.sql.Date;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static io.activej.dataflow.jdbc.driver.DataflowJdbc41Factory.notSupported;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static org.apache.calcite.avatica.remote.JsonService.MAPPER;

public class DataflowResultSet extends AvaticaResultSet {

	private static final TypeFactory TYPE_FACTORY = TypeFactory.defaultInstance();
	private static final Map<String, JavaType> JAVA_TYPE_CACHE = new ConcurrentHashMap<>();

	public static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
		.parseCaseInsensitive()
		.append(ISO_LOCAL_DATE)
		.appendLiteral(' ')
		.append(ISO_LOCAL_TIME)
		.toFormatter();

	DataflowResultSet(AvaticaStatement statement,
		Meta.Signature signature,
		ResultSetMetaData resultSetMetaData, TimeZone timeZone,
		Meta.Frame firstFrame) throws SQLException {
		super(statement, null, signature, resultSetMetaData, timeZone, firstFrame);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		int column = findColumn(columnLabel);
		return getObject(column, type);
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		int column = findColumn(columnLabel);
		return getObject(column);
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return doGetObject(columnIndex, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		if (type == Time.class || type == Date.class || type == Timestamp.class) {
			throw notSupported(type);
		}
		Class<?> expectedClass = type == Object.class ? null : type;
		return (T) doGetObject(columnIndex, expectedClass);
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		Object object = getObject(columnIndex);
		if (object == null) return null;
		return Objects.toString(object);
	}

	// region not supported temporal types
	@Override
	public Date getDate(int columnIndex) {
		throw notSupported(Date.class);
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) {
		throw notSupported(Date.class);
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) {
		throw notSupported(Date.class);
	}

	@Override
	public Date getDate(String columnLabel) {
		throw notSupported(Date.class);
	}

	@Override
	public void updateDate(int columnIndex, Date x) {
		throw notSupported(Date.class);
	}

	@Override
	public void updateDate(String columnLabel, Date x) {
		throw notSupported(Date.class);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw notSupported(Time.class);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		throw notSupported(Time.class);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw notSupported(Time.class);
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw notSupported(Time.class);
	}

	@Override
	public void updateTime(int columnIndex, Time x) {
		throw notSupported(Time.class);
	}

	@Override
	public void updateTime(String columnLabel, Time x) {
		throw notSupported(Time.class);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) {
		throw notSupported(Timestamp.class);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) {
		throw notSupported(Timestamp.class);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) {
		throw notSupported(Timestamp.class);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) {
		throw notSupported(Timestamp.class);
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) {
		throw notSupported(Timestamp.class);
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) {
		throw notSupported(Timestamp.class);
	}
	// endregion

	private Object doGetObject(int columnIndex, @Nullable Class<?> expectedClass) throws SQLException {
		Object result = super.getObject(columnIndex);

		JavaType javaType = expectedClass == null ?
			getJavaType(columnIndex) :
			TYPE_FACTORY.constructSimpleType(expectedClass, new JavaType[0]);

		Class<?> internalClass = javaType.getRawClass();
		if (result == null ||
			javaType == TypeFactory.unknownType() ||
			result instanceof Array ||
			internalClass.isAssignableFrom(result.getClass())
		) {
			return result;
		}

		Object converted = MAPPER.convertValue(result, javaType);
		if (expectedClass == null && converted instanceof LocalDateTime localDateTime) {
			return localDateTime.format(DATE_TIME_FORMATTER);
		}
		return converted;
	}

	private JavaType getJavaType(int columnIndex) {
		ColumnMetaData columnMetaData = signature.columns.get(columnIndex - 1);
		ColumnMetaData.AvaticaType type = columnMetaData.type;
		return getJavaType(type);
	}

	private static JavaType getJavaType(ColumnMetaData.AvaticaType avaticaType) {
		String name = avaticaType.getName();
		JavaType javaType = JAVA_TYPE_CACHE.get(name);
		if (javaType != null) {
			return javaType;
		}
		if (avaticaType instanceof ColumnMetaData.ArrayType arrayType) {
			JavaType componentType = getJavaType(arrayType.getComponent());
			CollectionType listType = TYPE_FACTORY.constructCollectionType(List.class, componentType);
			JAVA_TYPE_CACHE.put(name, listType);
			return listType;
		}

		MapTypes mapTypes = extractMapTypes(avaticaType);
		if (mapTypes != null) {
			MapType mapType = TYPE_FACTORY.constructMapType(Map.class, mapTypes.keyType, mapTypes.valueType);
			JAVA_TYPE_CACHE.put(name, mapType);
			return mapType;
		}

		return resolveSimple(name);
	}

	private static JavaType resolveSimple(String name) {
		return JAVA_TYPE_CACHE.computeIfAbsent(name, $ -> {
			try {
				Class<?> aClass = Class.forName(name);
				return TYPE_FACTORY.constructSimpleType(aClass, new JavaType[0]);
			} catch (ClassNotFoundException e) {
				return TypeFactory.unknownType();
			}
		});
	}

	private static @Nullable MapTypes extractMapTypes(ColumnMetaData.AvaticaType avaticaType) {
		String name = avaticaType.getName();
		if (!name.startsWith("MAP(") || !name.endsWith(")")) return null;

		String componentPart = name.substring(4, name.length() - 1);
		String[] components = componentPart.split(",");
		if (components.length != 2) return null;

		return new MapTypes(
			resolveSimple(components[0]),
			resolveSimple(components[1])
		);
	}

	public record MapTypes(JavaType keyType, JavaType valueType) {
	}
}
