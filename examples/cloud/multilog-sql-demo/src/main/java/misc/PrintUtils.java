package misc;

import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.types.Types;
import org.apache.calcite.avatica.SqlType;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;

public final class PrintUtils {

	public static void printRecords(List<Record> records) {
		RecordPrinter recordPrinter = new RecordPrinter(records);
		recordPrinter.printResults();
	}

	public static void printResultSet(ResultSet resultSet) {
		ResultSetPrinter resultSetPrinter = new ResultSetPrinter(resultSet);
		resultSetPrinter.printResults();
	}

	private static Format estimateFormat(String columnName, Class<?> columnCls) {
		Format format = estimateValueWidth(columnCls);
		int columnNameLength = columnName.length();
		if (columnNameLength <= format.columnWidth) {
			return format;
		}
		return new Format(columnNameLength, format.formatFn);
	}

	private static Format estimateValueWidth(Class<?> columnCls) {
		if (columnCls == byte.class || columnCls == Byte.class) {
			return new Format(4, x -> "%" + x + "d");
		}
		if (columnCls == short.class || columnCls == Short.class) {
			return new Format(6, x -> "%" + x + "d");
		}
		if (columnCls == int.class || columnCls == Integer.class) {
			return new Format(11, x -> "%" + x + "d");
		}
		if (columnCls == long.class || columnCls == Long.class) {
			return new Format(20, x -> "%" + x + "d");
		}
		if (columnCls == float.class || columnCls == Float.class || columnCls == double.class || columnCls == Double.class) {
			return new Format(0, x -> "%" + x + ".3f");
		}
		if (columnCls == String.class) {
			return new Format(37, x -> "%" + x + "s");
		}
		throw new AssertionError();
	}

	private record Format(int columnWidth, IntFunction<String> formatFn) {
	}

	private static abstract class ResultPrinter {
		final void printResults() {
			try {
				if (isEmpty()) {
					System.out.println("[EMPTY]");
					return;
				}

				StringBuilder recordPatternBuilder = new StringBuilder("|");
				StringBuilder headerPatternBuilder = new StringBuilder("|");
				StringBuilder horizontalBorderBuilder = new StringBuilder("+");

				int columnCount = getColumnCount();
				String[] columnNames = new String[columnCount];

				for (int i = 0; i < columnCount; i++) {
					String columnName = getColumnName(i);
					columnNames[i] = columnName;

					Class<?> columnType = getColumnType(i);

					Format format = estimateFormat(columnName, columnType);
					recordPatternBuilder.append(format.formatFn.apply(format.columnWidth)).append("|");
					headerPatternBuilder.append("%").append(format.columnWidth).append("s|");

					horizontalBorderBuilder.append(String.join("", Collections.nCopies(format.columnWidth, "-"))).append("+");
				}

				String valuesPattern = recordPatternBuilder.append("%n").toString();
				String headerPattern = headerPatternBuilder.append("%n").toString();
				String horizontalBorder = horizontalBorderBuilder.toString();

				System.out.println(horizontalBorder);
				System.out.printf(headerPattern, (Object[]) columnNames);

				System.out.println(horizontalBorder);

				while (hasNextValues()) {
					Object[] values = getNextValues();
					if (values == null) break;
					System.out.printf(valuesPattern, values);
				}

				System.out.println(horizontalBorder);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		abstract boolean isEmpty() throws Exception;

		abstract int getColumnCount() throws Exception;

		abstract String getColumnName(int index) throws Exception;

		abstract Class<?> getColumnType(int index) throws Exception;

		abstract boolean hasNextValues() throws Exception;

		abstract Object[] getNextValues() throws Exception;
	}

	private static final class RecordPrinter extends ResultPrinter {
		private final Iterator<Record> recordIterator;
		private final @Nullable RecordScheme scheme;

		private RecordPrinter(List<Record> records) {
			this.recordIterator = records.iterator();
			this.scheme = !records.isEmpty() ? records.get(0).getScheme() : null;
		}

		@Override
		boolean isEmpty() {
			return scheme == null;
		}

		@Override
		int getColumnCount() {
			assert scheme != null;
			return scheme.size();
		}

		@Override
		String getColumnName(int index) {
			assert scheme != null;
			return scheme.getField(index);
		}

		@Override
		Class<?> getColumnType(int index) {
			assert scheme != null;
			return Types.getRawType(scheme.getFieldType(index));
		}

		@Override
		boolean hasNextValues() {
			return recordIterator.hasNext();
		}

		@Override
		Object[] getNextValues() {
			return recordIterator.next().toMap().values().toArray();
		}
	}

	private static final class ResultSetPrinter extends ResultPrinter {
		private final ResultSet resultSet;

		private ResultSetPrinter(ResultSet resultSet) {
			this.resultSet = resultSet;
		}

		@Override
		boolean isEmpty() throws SQLException {
			return !resultSet.isBeforeFirst();
		}

		@Override
		int getColumnCount() throws SQLException {
			return resultSet.getMetaData().getColumnCount();
		}

		@Override
		String getColumnName(int index) throws SQLException {
			return resultSet.getMetaData().getColumnName(index + 1);
		}

		@Override
		Class<?> getColumnType(int index) throws SQLException {
			int columnType = resultSet.getMetaData().getColumnType(index + 1);
			return SqlType.valueOf(columnType).clazz;
		}

		@Override
		boolean hasNextValues() throws SQLException {
			return resultSet.next();
		}

		@Override
		Object[] getNextValues() throws SQLException {
			Object[] values = new Object[getColumnCount()];
			for (int i = 0; i < values.length; i++) {
				values[i] = resultSet.getObject(i + 1);
			}
			return values;
		}
	}
}
