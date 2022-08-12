package misc;

import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.types.Types;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;

public final class PrintUtils {

	public static void printRecords(List<Record> records) {
		if (records.isEmpty()) {
			System.out.println("[EMPTY]");
		}

		StringBuilder recordPatternBuilder = new StringBuilder("|");
		StringBuilder headerPatternBuilder = new StringBuilder("|");
		StringBuilder horizontalBorderBuilder = new StringBuilder("+");
		RecordScheme scheme = records.get(0).getScheme();
		List<String> fields = scheme.getFields();
		List<Type> types = scheme.getTypes();
		assert fields.size() == types.size();

		for (int i = 0; i < fields.size(); i++) {
			String fieldName = fields.get(i);
			Type type = types.get(i);
			Class<?> fieldCls = Types.getRawType(type);

			Format format = estimateFormat(fieldName, fieldCls);
			recordPatternBuilder.append(format.formatFn.apply(format.columnWidth)).append("|");
			headerPatternBuilder.append("%").append(format.columnWidth).append("s|");

			horizontalBorderBuilder.append(String.join("", Collections.nCopies(format.columnWidth, "-"))).append("+");
		}

		String recordPattern = recordPatternBuilder.append("%n").toString();
		String headerPattern = headerPatternBuilder.append("%n").toString();
		String horizontalBorder = horizontalBorderBuilder.toString();

		System.out.println(horizontalBorder);
		System.out.printf(headerPattern, fields.toArray());

		System.out.println(horizontalBorder);
		for (Record record : records) {
			System.out.printf(recordPattern, record.toMap().values().toArray());
		}

		System.out.println(horizontalBorder);
	}

	public static void main(String[] args) {
		System.out.printf("|%5.3f|", 0.0352352);
	}

	private static Format estimateFormat(String fieldName, Class<?> fieldCls) {
		Format format = estimateValueWidth(fieldCls);
		int fieldNameLength = fieldName.length();
		if (fieldNameLength <= format.columnWidth) {
			return format;
		}
		return new Format(fieldNameLength, format.formatFn);
	}

	private static Format estimateValueWidth(Class<?> fieldCls) {
		if (fieldCls == byte.class || fieldCls == Byte.class) {
			return new Format(4, x -> "%" + x + "d");
		}
		if (fieldCls == short.class || fieldCls == Short.class) {
			return new Format(6, x -> "%" + x + "d");
		}
		if (fieldCls == int.class || fieldCls == Integer.class) {
			return new Format(11, x -> "%" + x + "d");
		}
		if (fieldCls == long.class || fieldCls == Long.class) {
			return new Format(20, x -> "%" + x + "d");
		}
		if (fieldCls == float.class || fieldCls == Float.class || fieldCls == double.class || fieldCls == Double.class) {
			return new Format(0, x -> "%" + x + ".3f");
		}
		if (fieldCls == String.class) {
			return new Format(37, x -> "%" + x + "s");
		}
		throw new AssertionError();
	}

	private record Format(int columnWidth, IntFunction<String> formatFn) {
	}
}
