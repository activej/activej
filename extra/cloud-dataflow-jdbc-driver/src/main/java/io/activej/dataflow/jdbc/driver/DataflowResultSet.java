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

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.calcite.avatica.remote.JsonService.MAPPER;

class DataflowResultSet extends AvaticaResultSet {

	private static final TypeFactory TYPE_FACTORY = TypeFactory.defaultInstance();
	private static final Map<String, JavaType> JAVA_TYPE_CACHE = new ConcurrentHashMap<>();

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
		ColumnMetaData.AvaticaType type = columnMetaData.type;

		JavaType javaType = getJavaType(type);

		if (javaType == TypeFactory.unknownType() || result instanceof Array) {
			return result;
		}

		if (isTemporalType(javaType)) {
			return getTemporalType((String) result, javaType);
		}

		return MAPPER.convertValue(result, javaType);
	}

	private static Object getTemporalType(String result, JavaType javaType) {
		Class<?> rawClass = javaType.getRawClass();

		if (rawClass == Timestamp.class) return Timestamp.valueOf(result);
		if (rawClass == Date.class) return Date.valueOf(result);
		if (rawClass == Time.class) return Time.valueOf(result);

		throw new AssertionError();
	}

	private static boolean isTemporalType(JavaType javaType) {
		Class<?> rawClass = javaType.getRawClass();
		return rawClass == Timestamp.class || rawClass == Date.class || rawClass == Time.class;
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

	private record MapTypes(JavaType keyType, JavaType valueType) {

	}
}
