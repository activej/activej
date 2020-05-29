package io.activej.dataflow.collector;

import io.activej.codec.StructuredCodec;
import io.activej.common.exception.parse.ParseException;
import io.activej.dataflow.command.DataflowCommand;
import io.activej.dataflow.command.DataflowCommandExecute;
import io.activej.dataflow.graph.StreamId;
import io.activej.dataflow.inject.CodecsModule.Subtypes;
import io.activej.dataflow.inject.DataflowCodecs;
import io.activej.dataflow.node.*;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.datastream.processor.StreamReducers.Reducer;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static io.activej.codec.StructuredCodec.ofObject;
import static io.activej.codec.json.JsonUtils.fromJson;
import static io.activej.codec.json.JsonUtils.toJson;

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

	@SuppressWarnings("rawtypes")
	@Test
	public void test2() throws UnknownHostException, ParseException {

		Module serialization = ModuleBuilder.create()
				.install(DataflowCodecs.create())
				.bind(new Key<StructuredCodec<TestComparator>>() {}).toInstance(ofObject(TestComparator::new))
				.bind(new Key<StructuredCodec<TestFunction>>() {}).toInstance(ofObject(TestFunction::new))
				.bind(new Key<StructuredCodec<TestIdentityFunction>>() {}).toInstance(ofObject(TestIdentityFunction::new))
				.bind(new Key<StructuredCodec<TestReducer>>() {}).toInstance(ofObject(TestReducer::new))
				.build();

		NodeReduce<Integer, Integer, Integer> reducer = new NodeReduce<>(0, new TestComparator());
		reducer.addInput(new StreamId(), new TestIdentityFunction<>(), new TestReducer());
		List<Node> nodes = Arrays.asList(
				reducer,
				new NodeMap<>(1, new TestFunction(), new StreamId(1)),
				new NodeUpload<>(2, Integer.class, new StreamId(Long.MAX_VALUE)),
				new NodeDownload<>(3, Integer.class, new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 1571), new StreamId(Long.MAX_VALUE))
		);

		StructuredCodec<DataflowCommand> commandCodec = Injector.of(serialization).getInstance(new Key<StructuredCodec<DataflowCommand>>() {}.qualified(Subtypes.class));

		String str = toJson(commandCodec, new DataflowCommandExecute(123, nodes));
		System.out.println(str);

		System.out.println(fromJson(commandCodec, str));
	}
}
