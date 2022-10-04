package io.activej.dataflow.calcite.join;

import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamJoin.InnerJoiner;
import io.activej.record.Record;
import io.activej.record.RecordGetter;
import io.activej.record.RecordScheme;
import io.activej.record.RecordSetter;

import java.lang.reflect.Type;
import java.util.List;

public final class RecordInnerJoiner<K extends Comparable<K>> extends InnerJoiner<K, Record, Record, Record> {

	private final RecordScheme scheme;
	private final RecordScheme left;
	private final RecordScheme right;

	private final RecordSetter<?>[] setters;
	private final RecordGetter<?>[] leftGetters;
	private final RecordGetter<?>[] rightGetters;

	private RecordInnerJoiner(
			RecordScheme scheme,
			RecordScheme leftScheme,
			RecordScheme rightScheme,
			RecordSetter<?>[] setters,
			RecordGetter<?>[] leftGetters,
			RecordGetter<?>[] rightGetters) {
		this.scheme = scheme;
		this.left = leftScheme;
		this.right = rightScheme;
		this.setters = setters;
		this.leftGetters = leftGetters;
		this.rightGetters = rightGetters;
	}

	public static <K extends Comparable<K>> RecordInnerJoiner<K> create(RecordScheme left, RecordScheme right, List<String> fieldNames) {
		RecordScheme scheme = createScheme(left, right, fieldNames);
		return create(scheme, left, right);
	}

	public static <K extends Comparable<K>> RecordInnerJoiner<K> create(RecordScheme scheme, RecordScheme left, RecordScheme right) {
		RecordSetter<?>[] setters = getSetters(scheme);
		RecordGetter<?>[] leftGetters = getGetters(left);
		RecordGetter<?>[] rightGetters = getGetters(right);

		return new RecordInnerJoiner<>(scheme, left, right, setters, leftGetters, rightGetters);
	}

	public RecordScheme getScheme() {
		return scheme;
	}

	public RecordScheme getLeft() {
		return left;
	}

	public RecordScheme getRight() {
		return right;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onInnerJoin(K key, Record left, Record right, StreamDataAcceptor<Record> output) {
		Record result = scheme.record();

		int idx = 0;
		for (RecordGetter<?> leftGetter : leftGetters) {
			RecordSetter<Object> setter = (RecordSetter<Object>) setters[idx++];
			setter.set(result, leftGetter.get(left));
		}
		for (RecordGetter<?> rightGetter : rightGetters) {
			RecordSetter<Object> setter = (RecordSetter<Object>) setters[idx++];
			setter.set(result, rightGetter.get(right));
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

	private static RecordGetter<?>[] getGetters(RecordScheme scheme) {
		RecordGetter<?>[] getters = new RecordGetter[scheme.size()];
		for (int i = 0; i < getters.length; i++) {
			getters[i] = scheme.getter(i);
		}
		return getters;
	}

	private static RecordSetter<?>[] getSetters(RecordScheme scheme) {
		RecordSetter<?>[] setters = new RecordSetter[scheme.size()];
		for (int i = 0; i < setters.length; i++) {
			setters[i] = scheme.setter(i);
		}
		return setters;
	}
}
