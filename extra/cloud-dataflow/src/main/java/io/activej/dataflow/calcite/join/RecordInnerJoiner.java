package io.activej.dataflow.calcite.join;

import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamJoin.InnerJoiner;
import io.activej.record.Record;
import io.activej.record.RecordScheme;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.List;

public final class RecordInnerJoiner<K extends Comparable<K>> extends InnerJoiner<K, Record, Record, Record> {

	@Nullable
	RecordScheme scheme;

	@Override
	public void onInnerJoin(K key, Record left, Record right, StreamDataAcceptor<Record> output) {
		RecordScheme leftScheme = left.getScheme();
		RecordScheme rightScheme = right.getScheme();

		if (scheme == null) {
			scheme = RecordScheme.create();

			addFields("left", leftScheme);
			addFields("right", rightScheme);
			scheme.build();
		}

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

	private void addFields(String prefix, RecordScheme leftScheme) {
		assert scheme != null;

		List<String> fields = leftScheme.getFields();
		List<Type> types = leftScheme.getTypes();
		for (int i = 0; i < fields.size(); i++) {
			String fieldName = fields.get(i);
			Type type = types.get(i);
			scheme.addField(prefix + '.' + fieldName, type);
		}
	}
}
