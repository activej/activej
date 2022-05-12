package io.activej.dataflow.collector;

import com.google.protobuf.ByteString;
import io.activej.common.exception.MalformedDataException;
import io.activej.dataflow.proto.FunctionSerializer;
import io.activej.dataflow.proto.FunctionSubtypeSerializer;
import io.activej.dataflow.proto.ProtobufFunctionModule;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamJoin.Joiner;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.serializer.BinarySerializer;
import org.junit.Test;

import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.activej.dataflow.proto.ProtobufUtils.ofObject;

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

	private static class TestJoiner implements Joiner<Integer, Integer, Integer, Integer> {

		@Override
		public void onInnerJoin(Integer key, Integer left, Integer right, StreamDataAcceptor<Integer> output) {
		}

		@Override
		public void onLeftJoin(Integer key, Integer left, StreamDataAcceptor<Integer> output) {
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

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void test() throws MalformedDataException {
		Key<BinarySerializer<TestFunction>> testFunctionSerializerKey = new Key<>() {};
		Key<BinarySerializer<TestIdentityFunction>> testIdentityFunctionSerializerKey = new Key<>() {};
		Module serialization = ModuleBuilder.create()
				.install(ProtobufFunctionModule.create())
				.bind(new Key<BinarySerializer<Comparator<?>>>() {}).toInstance(ofObject(TestComparator::new))
				.bind(new Key<BinarySerializer<Reducer<?, ?, ?, ?>>>() {}).toInstance(ofObject(TestReducer::new))
				.bind(new Key<BinarySerializer<Joiner<?, ?, ?, ?>>>() {}).toInstance(ofObject(TestJoiner::new))
				.bind(new Key<BinarySerializer<Predicate<?>>>() {}).toInstance(ofObject(TestPredicate::new))
				.bind(testFunctionSerializerKey).toInstance(ofObject(TestFunction::new))
				.bind(testIdentityFunctionSerializerKey).toInstance(ofObject(TestIdentityFunction::new))
				.bind(new Key<BinarySerializer<Function<?, ?>>>() {}).to((testFunctionSerializer, testIdentityFunctionSerializer) -> {
					FunctionSubtypeSerializer<Function> functionSubtypeSerializer = FunctionSubtypeSerializer.create();
					functionSubtypeSerializer.setSubtypeCodec(TestFunction.class, testFunctionSerializer);
					functionSubtypeSerializer.setSubtypeCodec(TestIdentityFunction.class, testIdentityFunctionSerializer);
					return ((BinarySerializer<Function<?,?>>) ((BinarySerializer) functionSubtypeSerializer));
				}, testFunctionSerializerKey, testIdentityFunctionSerializerKey)
				.build();

		FunctionSerializer functionSerializer = Injector.of(serialization).getInstance(FunctionSerializer.class);

		ByteString comparatorByteString = functionSerializer.serializeComparator(new TestComparator());
		ByteString functionByteString = functionSerializer.serializeFunction(new TestFunction());
		ByteString identityFunctionByteString = functionSerializer.serializeFunction(new TestIdentityFunction<>());
		ByteString reducerByteString = functionSerializer.serializeReducer(new TestReducer());
		ByteString joinerByteString = functionSerializer.serializeJoiner(new TestJoiner());
		ByteString predicateByteString = functionSerializer.serializePredicate(new TestPredicate());

		System.out.println(comparatorByteString);
		System.out.println(functionByteString);
		System.out.println(identityFunctionByteString);
		System.out.println(reducerByteString);
		System.out.println(joinerByteString);
		System.out.println(predicateByteString);

		System.out.println(functionSerializer.deserializeComparator(comparatorByteString));
		System.out.println(functionSerializer.deserializeFunction(functionByteString));
		System.out.println(functionSerializer.deserializeFunction(identityFunctionByteString));
		System.out.println(functionSerializer.deserializeReducer(reducerByteString));
		System.out.println(functionSerializer.deserializeJoiner(joinerByteString));
		System.out.println(functionSerializer.deserializePredicate(predicateByteString));
	}
}
