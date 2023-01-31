package io.activej.dataflow.calcite.join;

import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamLeftJoin.LeftJoiner;
import io.activej.record.Record;
import io.activej.record.RecordGetter;
import io.activej.record.RecordScheme;
import io.activej.record.RecordSetter;
import io.activej.types.Primitives;
import org.apache.calcite.rel.core.JoinRelType;

import java.lang.reflect.Type;
import java.util.List;

import static io.activej.common.Checks.checkArgument;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;

public final class RecordLeftJoiner implements LeftJoiner<Record, Record, Record, Record> {
	private final JoinRelType joinType;

	private final RecordScheme scheme;
	private final RecordScheme left;
	private final RecordScheme right;

	private final RecordSetter<?>[] setters;
	private final RecordGetter<?>[] leftGetters;
	private final RecordGetter<?>[] rightGetters;

	private RecordLeftJoiner(
			JoinRelType joinType,
			RecordScheme scheme,
			RecordScheme leftScheme,
			RecordScheme rightScheme,
			RecordSetter<?>[] setters,
			RecordGetter<?>[] leftGetters,
			RecordGetter<?>[] rightGetters) {
		this.joinType = joinType;
		this.scheme = scheme;
		this.left = leftScheme;
		this.right = rightScheme;
		this.setters = setters;
		this.leftGetters = leftGetters;
		this.rightGetters = rightGetters;
	}

	public static RecordLeftJoiner create(JoinRelType joinType, RecordScheme left, RecordScheme right, List<String> fieldNames) {
		RecordScheme scheme = createScheme(joinType, left, right, fieldNames);
		return create(joinType, scheme, left, right);
	}

	public static RecordLeftJoiner create(JoinRelType joinType, RecordScheme scheme, RecordScheme left, RecordScheme right) {
		checkArgument(joinType == INNER || joinType == LEFT, "Only INNER and LEFT joins are supported");

		RecordSetter<?>[] setters = getSetters(scheme);
		RecordGetter<?>[] leftGetters = getGetters(left);
		RecordGetter<?>[] rightGetters = getGetters(right);

		return new RecordLeftJoiner(joinType, scheme, left, right, setters, leftGetters, rightGetters);
	}

	public JoinRelType getJoinRelType() {
		return joinType;
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
	public void onOuterJoin(Record key, Record left, StreamDataAcceptor<Record> output) {
		if (joinType != LEFT) return;

		Record result = scheme.record();

		for (int i = 0; i < leftGetters.length; i++) {
			RecordGetter<?> leftGetter = leftGetters[i];
			//noinspection unchecked
			RecordSetter<Object> setter = (RecordSetter<Object>) setters[i];
			setter.set(result, leftGetter.get(left));
		}

		output.accept(result);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onInnerJoin(Record key, Record left, Record right, StreamDataAcceptor<Record> output) {
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

	private static RecordScheme createScheme(JoinRelType joinType, RecordScheme leftScheme, RecordScheme rightScheme, List<String> fieldNames) {
		assert leftScheme.getClassLoader() == rightScheme.getClassLoader();
		assert fieldNames.size() == leftScheme.size() + rightScheme.size();

		RecordScheme.Builder schemeBuilder = RecordScheme.builder(leftScheme.getClassLoader());

		addFields(schemeBuilder, leftScheme, fieldNames, 0, false);
		addFields(schemeBuilder, rightScheme, fieldNames, leftScheme.size(), joinType == LEFT);

		return schemeBuilder
				.withComparatorFields(fieldNames)
				.build();
	}

	private static void addFields(RecordScheme.Builder toSchemeBuilder, RecordScheme fromScheme, List<String> fieldNames, int offset, boolean forceNullability) {
		List<Type> types = fromScheme.getTypes();
		for (int i = 0; i < types.size(); i++) {
			Type type = types.get(i);
			if (forceNullability && type instanceof Class<?> cls && cls.isPrimitive()) {
				type = Primitives.wrap(cls);
			}
			toSchemeBuilder.withField(fieldNames.get(i + offset), type);
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
