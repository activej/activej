package io.activej.dataflow.calcite.join;

import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamJoin.InnerJoiner;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import java.lang.reflect.Type;
import java.util.List;

public final class RecordInnerJoiner<K extends Comparable<K>> extends InnerJoiner<K, Record, Record, Record> {

	private final RecordScheme scheme;

	private RecordInnerJoiner(RecordScheme scheme) {
		this.scheme = scheme;
	}

	public static <K extends Comparable<K>> RecordInnerJoiner<K> create(RecordScheme left, RecordScheme right, List<String> fieldNames) {
		return new RecordInnerJoiner<>(createScheme(left, right, fieldNames));
	}

	public static <K extends Comparable<K>> RecordInnerJoiner<K> create(RecordScheme scheme) {
		return new RecordInnerJoiner<>(scheme);
	}

	public RecordScheme getScheme() {
		return scheme;
	}

	@Override
	public void onInnerJoin(K key, Record left, Record right, StreamDataAcceptor<Record> output) {
		RecordScheme leftScheme = left.getScheme();
		RecordScheme rightScheme = right.getScheme();

		Record result = scheme.record();

		int idx = 0;
		for (int i = 0; i < leftScheme.size(); i++) {
			result.set(idx++, left.get(i));
		}
		for (int i = 0; i < rightScheme.size(); i++) {
			result.set(idx++, right.get(i));
		}

		output.accept(result);
	}

	private static RecordScheme createScheme(RecordScheme leftScheme, RecordScheme rightScheme, List<String> fieldNames) {
		assert leftScheme.getClassLoader() == rightScheme.getClassLoader();
		assert fieldNames.size() == leftScheme.size() + rightScheme.size();

		RecordScheme scheme = RecordScheme.create(leftScheme.getClassLoader());

		addFields(scheme, leftScheme, fieldNames, 0);
		addFields(scheme, rightScheme, fieldNames, leftScheme.size());

		return scheme
				.withComparator(scheme.getFields())
				.build();
	}

	private static void addFields(RecordScheme toScheme, RecordScheme fromScheme, List<String> fieldNames, int offset) {
		List<Type> types = fromScheme.getTypes();
		for (int i = 0; i < types.size(); i++) {
			toScheme.addField(fieldNames.get(i + offset), types.get(i));
		}
	}
}
