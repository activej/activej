package io.activej.ot.utils;

import com.dslplatform.json.JsonConverter;
import com.dslplatform.json.JsonReader.ReadObject;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.ParsingException;
import com.dslplatform.json.runtime.ExplicitDescription;
import io.activej.ot.IOTCommitFactory.DiffsWithLevel;
import io.activej.ot.OTCommit;
import io.activej.ot.system.IOTSystem;
import io.activej.ot.system.OTSystem;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.dslplatform.json.JsonWriter.*;
import static com.dslplatform.json.NumberConverter.deserializeInt;
import static com.dslplatform.json.NumberConverter.serialize;
import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.difference;
import static io.activej.common.Utils.first;
import static io.activej.ot.TransformResult.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class Utils {

	private static final Object INVALID_KEY = new Object();

	public static TestAdd add(int delta) {
		return new TestAdd(delta);
	}

	public static TestSet set(int prev, int next) {
		return new TestSet(prev, next);
	}

	public static IOTSystem<TestOp> createTestOp() {
		return OTSystem.<TestOp>builder()
				.withTransformFunction(TestAdd.class, TestAdd.class, (left, right) -> of(add(right.getDelta()), add(left.getDelta())))
				.withTransformFunction(TestAdd.class, TestSet.class, (left, right) -> left(set(right.getPrev() + left.getDelta(), right.getNext())))
				.withTransformFunction(TestSet.class, TestSet.class, (left, right) -> {
					checkArgument(left.getPrev() == right.getPrev(), "Previous values of left and right set operation should be equal");
					if (left.getNext() > right.getNext()) return left(set(left.getNext(), right.getNext()));
					if (left.getNext() < right.getNext()) return right(set(right.getNext(), left.getNext()));
					return empty();
				})
				.withSquashFunction(TestAdd.class, TestAdd.class, (op1, op2) -> add(op1.getDelta() + op2.getDelta()))
				.withSquashFunction(TestSet.class, TestSet.class, (op1, op2) -> set(op1.getPrev(), op2.getNext()))
				.withSquashFunction(TestAdd.class, TestSet.class, (op1, op2) -> set(op1.inverse().apply(op2.getPrev()), op2.getNext()))
				.withSquashFunction(TestSet.class, TestAdd.class, (op1, op2) -> set(op1.getPrev(), op1.getNext() + op2.getDelta()))
				.withEmptyPredicate(TestAdd.class, add -> add.getDelta() == 0)
				.withEmptyPredicate(TestSet.class, set -> set.getPrev() == set.getNext())
				.withInvertFunction(TestAdd.class, op -> List.of(op.inverse()))
				.withInvertFunction(TestSet.class, op -> List.of(set(op.getNext(), op.getPrev())))
				.build();
	}

	static final class JsonConverters {
		@JsonConverter(target = TestOp.class)
		public static class TestOpConverter {
			public static final ReadObject<TestOp> JSON_READER = reader -> {
				if (reader.last() != OBJECT_START) throw reader.newParseError("Expected '{'");
				reader.getNextToken();
				String key = reader.readString();
				if (reader.getNextToken() != SEMI) {
					throw ParsingException.create("':' expected after object key", true);
				}

				TestOp result;
				switch (key) {
					case "add" -> {
						reader.getNextToken();
						result = new TestAdd(deserializeInt(reader));
					}
					case "set" -> {
						reader.startArray();
						reader.getNextToken();
						int prev = deserializeInt(reader);
						if (reader.getNextToken() != COMMA) {
							throw reader.newParseError("Comma expected");
						}
						reader.getNextToken();
						int next = deserializeInt(reader);
						reader.endArray();
						result = new TestSet(prev, next);
					}
					default -> throw reader.newParseError("Invalid TestOp key: " + key);
				}
				reader.endObject();
				return result;
			};
			public static final WriteObject<TestOp> JSON_WRITER = new TestOpWriteObject();

			private static class TestOpWriteObject implements WriteObject<TestOp>, ExplicitDescription {
				@Override
				public void write(JsonWriter writer, TestOp value) {
					if (value instanceof TestAdd) {
						writer.writeByte(OBJECT_START);
						writer.writeString("add");
						writer.writeByte(SEMI);
						serialize(((TestAdd) value).getDelta(), writer);
						writer.writeByte(OBJECT_END);
					} else if (value instanceof TestSet set) {
						writer.writeByte(OBJECT_START);
						writer.writeString("set");
						writer.writeByte(SEMI);
						writer.writeByte(ARRAY_START);
						serialize(set.getPrev(), writer);
						writer.writeByte(COMMA);
						serialize(set.getNext(), writer);
						writer.writeByte(ARRAY_END);
						writer.writeByte(OBJECT_END);
					} else {
						throw new IllegalArgumentException("Unknown type: " + value);
					}
				}
			}
		}
	}

	public static <K> long calcLevels(K commitId, Map<K, Long> levels, Function<K, Collection<K>> getParents) {
		if (!levels.containsKey(commitId)) {
			levels.put(commitId, 1L + getParents.apply(commitId).stream()
					.mapToLong(parentId -> calcLevels(parentId, levels, getParents))
					.max()
					.orElse(0L));
		}
		return levels.get(commitId);
	}

	public static <D> Consumer<OTGraphBuilder<Long, D>> asLong(Consumer<OTGraphBuilder<Integer, D>> intGraphConsumer) {
		return longGraphBuilder ->
				intGraphConsumer.accept((parent, child, diffs) ->
						longGraphBuilder.add((long) parent, (long) child, diffs));
	}

	public static <K, D> List<OTCommit<K, D>> commits(Consumer<OTGraphBuilder<K, D>> graphBuilder) {
		return commits(graphBuilder, true, 1L);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> List<OTCommit<K, D>> commits(Consumer<OTGraphBuilder<K, D>> graphBuilder, boolean withRoots, long initialLevel) {
		Map<K, Map<K, List<D>>> graph = new HashMap<>();
		graphBuilder.accept((parent, child, diffs) ->
				graph.computeIfAbsent(child, $ -> new HashMap<>()).computeIfAbsent(parent, $ -> new ArrayList<>()).addAll(diffs));
		Set<K> heads = difference(
				graph.keySet(),
				graph.values()
						.stream()
						.flatMap(parents -> parents.keySet().stream())
						.collect(toSet()));
		Set<K> roots = difference(
				graph.values()
						.stream()
						.flatMap(parents -> parents.keySet().stream())
						.collect(toSet()),
				graph.keySet());
		HashMap<K, Long> levels = new HashMap<>();
		for (K head : heads) {
			calcLevels(head, levels, id -> graph.getOrDefault(id, Map.of()).keySet());
		}
		if (withRoots) {
			if (roots.size() == 1) {
				graph.put(first(roots), Map.of()); // true root
			} else {
				roots.forEach(root -> graph.put(root, Map.of((K) INVALID_KEY, List.of()))); // intermediate node
			}
		}
		return graph.entrySet()
				.stream()
				.map(entry -> OTCommit.builder(
								0,
								entry.getKey(),
								entry.getValue().entrySet().stream()
										.collect(Collectors.toMap(
												Map.Entry::getKey,
												e -> new DiffsWithLevel<>(
														initialLevel + levels.getOrDefault(e.getKey(), 0L) - 1L,
														e.getValue()
												)
										)))
						.withTimestamp(initialLevel - 1L + levels.get(entry.getKey()))
						.build())
				.collect(toList());
	}

}
