package io.activej.ot.utils;

import com.mysql.cj.jdbc.MysqlDataSource;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecs;
import io.activej.json.SubclassJsonCodec;
import io.activej.ot.AsyncOTCommitFactory.DiffsWithLevel;
import io.activej.ot.OTCommit;
import io.activej.ot.system.OTSystem;
import io.activej.ot.system.OTSystemImpl;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.collection.CollectionUtils.difference;
import static io.activej.common.collection.CollectionUtils.first;
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

	public static OTSystem<TestOp> createTestOp() {
		return OTSystemImpl.<TestOp>builder()
			.withTransformFunction(TestAdd.class, TestAdd.class, (left, right) -> of(add(right.getDelta()), add(left.getDelta())))
			.withTransformFunction(TestAdd.class, TestSet.class, (left, right) -> left(set(right.getPrev() + left.getDelta(), right.getNext())))
			.withTransformFunction(TestSet.class, TestSet.class, (left, right) -> {
				checkArgument(left.getPrev() == right.getPrev(),
					"Previous values of left and right set operation should be equal");
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

	public static final JsonCodec<TestOp> TEST_OP_CODEC = SubclassJsonCodec.<TestOp>builder()
		.with(TestAdd.class, JsonCodecs.ofObject(TestAdd::new,
			"delta", TestAdd::getDelta, JsonCodecs.ofInteger()))
		.with(TestSet.class, JsonCodecs.ofObject(TestSet::new,
			"prev", TestSet::getPrev, JsonCodecs.ofInteger(),
			"next", TestSet::getNext, JsonCodecs.ofInteger()))
		.build();

	public static <K> long calcLevels(K commitId, Map<K, Long> levels, Function<K, Collection<K>> getParents) {
		if (!levels.containsKey(commitId)) {
			levels.put(commitId,
				1L +
				getParents.apply(commitId).stream()
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

	public static DataSource dataSource(String databasePropertiesPath) throws IOException, SQLException {
		Properties properties = new Properties();
		try (FileInputStream fis = new FileInputStream(databasePropertiesPath)) {
			properties.load(fis);
		}

		MysqlDataSource dataSource = new MysqlDataSource();
		dataSource.setUrl("jdbc:mysql://" + properties.getProperty("dataSource.serverName") + '/' + properties.getProperty("dataSource.databaseName"));
		dataSource.setUser(properties.getProperty("dataSource.user"));
		dataSource.setPassword(properties.getProperty("dataSource.password"));
		dataSource.setServerTimezone(properties.getProperty("dataSource.timeZone"));
		dataSource.setAllowMultiQueries(true);
		return dataSource;
	}
}
