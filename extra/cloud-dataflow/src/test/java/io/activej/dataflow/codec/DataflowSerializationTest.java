package io.activej.dataflow.codec;

import io.activej.common.exception.MalformedDataException;
import io.activej.dataflow.codec.module.DataflowStreamCodecsModule;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamLeftJoin.LeftJoiner;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.codec.SubtypeImpl.subtype;

public class DataflowSerializationTest {

	public static class TestComparator implements Comparator<Integer> {
		@Override
		public int compare(Integer o1, Integer o2) {
			return o1.compareTo(o2);
		}
	}

	private static class TestReducer implements Reducer<Integer, Integer, Integer, Integer> {
		@Override
		public Integer onFirstItem(StreamDataAcceptor<Integer> stream, Integer key, Integer firstValue) {
			return null;
		}

		@Override
		public Integer onNextItem(StreamDataAcceptor<Integer> stream, Integer key, Integer nextValue, Integer accumulator) {
			return null;
		}

		@Override
		public void onComplete(StreamDataAcceptor<Integer> stream, Integer key, Integer accumulator) {}
	}

	private static class TestJoiner implements LeftJoiner<Integer, Integer, Integer, Integer> {

		@Override
		public void onInnerJoin(Integer key, Integer left, Integer right, StreamDataAcceptor<Integer> output) {
		}

		@Override
		public void onOuterJoin(Integer key, Integer left, StreamDataAcceptor<Integer> output) {
		}
	}

	private static class TestPredicate implements Predicate<Integer> {
		@Override
		public boolean test(Integer integer) {
			return false;
		}
	}

	private static class TestFunction implements Function<String, String> {
		@Override
		public String apply(String input) {
			return "<" + input + ">";
		}
	}

	private static class TestIdentityFunction<T> implements Function<T, T> {
		@Override
		public T apply(T value) {
			return value;
		}
	}

	@Test
	public void test() throws MalformedDataException {
		Module serialization = ModuleBuilder.create()
				.install(DataflowStreamCodecsModule.create())
				.bind(new Key<StreamCodec<Comparator<?>>>() {}).toInstance(StreamCodecs.singleton(new TestComparator()))
				.bind(new Key<StreamCodec<Reducer<?, ?, ?, ?>>>() {}).toInstance(StreamCodecs.singleton(new TestReducer()))
				.bind(new Key<StreamCodec<LeftJoiner<?, ?, ?, ?>>>() {}).toInstance(StreamCodecs.singleton(new TestJoiner()))
				.bind(new Key<StreamCodec<Predicate<?>>>() {}).toInstance(StreamCodecs.singleton(new TestPredicate()))

				.bind(new Key<StreamCodec<TestFunction>>(subtype(0)) {}).toInstance(StreamCodecs.singleton(new TestFunction()))
				.bind(new Key<StreamCodec<TestIdentityFunction<?>>>(subtype(1)) {}).toInstance(StreamCodecs.singleton(new TestIdentityFunction<>()))
				.build();

		StreamCodec<Comparator<?>> comparatorCodec = Injector.of(serialization).getInstance(new Key<>() {});
		StreamCodec<Function<?, ?>> functionCodec = Injector.of(serialization).getInstance(new Key<>() {});
		StreamCodec<Reducer<?, ?, ?, ?>> reducerCodec = Injector.of(serialization).getInstance(new Key<>() {});
		StreamCodec<LeftJoiner<?, ?, ?, ?>> leftJoinerCodec = Injector.of(serialization).getInstance(new Key<>() {});
		StreamCodec<Predicate<?>> predicateCodec = Injector.of(serialization).getInstance(new Key<>() {});

		byte[] comparatorBytes = comparatorCodec.toByteArray(new TestComparator());
		byte[] functionBytes = functionCodec.toByteArray(new TestFunction());
		byte[] identityFunctionBytes = functionCodec.toByteArray(new TestIdentityFunction<>());
		byte[] reducerBytes = reducerCodec.toByteArray(new TestReducer());
		byte[] joinerBytes = leftJoinerCodec.toByteArray(new TestJoiner());
		byte[] predicateBytes = predicateCodec.toByteArray(new TestPredicate());

		System.out.println(Arrays.toString(comparatorBytes));
		System.out.println(Arrays.toString(functionBytes));
		System.out.println(Arrays.toString(identityFunctionBytes));
		System.out.println(Arrays.toString(reducerBytes));
		System.out.println(Arrays.toString(joinerBytes));
		System.out.println(Arrays.toString(predicateBytes));

		System.out.println(comparatorCodec.fromByteArray(comparatorBytes));
		System.out.println(functionCodec.fromByteArray(functionBytes));
		System.out.println(functionCodec.fromByteArray(identityFunctionBytes));
		System.out.println(reducerCodec.fromByteArray(reducerBytes));
		System.out.println(leftJoinerCodec.fromByteArray(joinerBytes));
		System.out.println(predicateCodec.fromByteArray(predicateBytes));
	}
}
