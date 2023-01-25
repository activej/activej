package io.activej.ot;

import io.activej.ot.system.OTSystem;
import io.activej.ot.utils.OTGraphBuilder;
import io.activej.ot.utils.OTRepository_Stub;
import io.activej.ot.utils.TestOp;
import io.activej.ot.utils.Utils;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.nonNullElse;
import static io.activej.ot.OTAlgorithms.loadForMerge;
import static io.activej.ot.utils.Utils.add;
import static io.activej.ot.utils.Utils.createTestOp;
import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("CodeBlock2Expr")
public class OTMergeAlgorithmTest {
	private static final OTSystem<TestOp> system = createTestOp();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	static OTLoadedGraph<String, TestOp> buildGraph(Consumer<OTGraphBuilder<String, TestOp>> consumer) {
		OTLoadedGraph<String, TestOp> graph = new OTLoadedGraph<>(OTMergeAlgorithmTest.system);
		consumer.accept((parent, child, diffs) -> {
			checkArgument(graph.getParents(child) == null || graph.getParents(child).get(parent) == null, "Invalid graph");
			graph.addEdge(parent, child, diffs);
		});
		Map<String, Long> levels = new HashMap<>();
		for (String commitId : graph.getTips()) {
			Utils.calcLevels(commitId, levels,
					parentId -> nonNullElse(graph.getParents(parentId), Map.<String, List<TestOp>>of()).keySet());
		}
		levels.forEach(graph::setLevel);

		return graph;
	}

	@FunctionalInterface
	private interface TestAcceptor {
		void accept(OTLoadedGraph<String, TestOp> graph, Map<String, List<TestOp>> merge);
	}

	private void doTest(Set<String> heads, Consumer<OTGraphBuilder<String, TestOp>> graphBuilder, TestAcceptor testAcceptor) throws Exception {
		doTestMerge(heads, graphBuilder, testAcceptor);
		doTestLoadAndMerge(heads, graphBuilder, testAcceptor);
	}

	private void doTestMerge(Set<String> heads, Consumer<OTGraphBuilder<String, TestOp>> graphBuilder, TestAcceptor testAcceptor) throws Exception {
		OTLoadedGraph<String, TestOp> graph = buildGraph(graphBuilder);
		Map<String, List<TestOp>> merge;
		try {
			merge = graph.merge(heads);
		} finally {
			System.out.println(graph.toGraphViz());
		}
		testAcceptor.accept(graph, merge);
	}

	private void doTestLoadAndMerge(Set<String> heads, Consumer<OTGraphBuilder<String, TestOp>> graphBuilder, TestAcceptor testAcceptor) throws Exception {
		OTRepository_Stub<String, TestOp> repository = OTRepository_Stub.create();
		repository.setGraph(graphBuilder);

		OTLoadedGraph<String, TestOp> graph = await(loadForMerge(repository, system, heads));
		Map<String, List<TestOp>> merge;
		try {
			merge = graph.merge(heads);
		} finally {
			System.out.println(graph.toGraphViz());
		}
		testAcceptor.accept(graph, merge);
	}

	@Test
	// Merge one node should return empty merge
	public void test1() throws Exception {
		doTest(Set.of("A", "B"), g -> {
			g.add("A", "B", add(1));
		}, (graph, merge) -> {
			assertEquals(List.of(add(1)), merge.get("A"));
			assertEquals(List.of(), merge.get("B"));
		});
	}

	@Test
	// Merge already merged line
	public void test2() throws Exception {
		doTest(Set.of("A", "B"), g -> {
			g.add("A", "T", add(10));
			g.add("T", "B", add(1));
		}, (graph, merge) -> {
			assertEquals(List.of(add(11)), merge.get("A"));
			assertEquals(List.of(), merge.get("B"));
		});
	}

	@Test
	// Merge V form tree
	public void test3() throws Exception {
		doTest(Set.of("D", "E"), g -> {
			g.add("A", "B", add(1));
			g.add("A", "C", add(100));
			g.add("B", "D", add(10));
			g.add("C", "E", add(1000));
		}, (graph, merge) -> {
			assertEquals(List.of(add(1100)), merge.get("D"));
			assertEquals(List.of(add(11)), merge.get("E"));
		});
	}

	@Test
	// Merge A, B nodes and D, E subnodes
	public void test4() throws Exception {
		doTest(Set.of("A", "B", "D", "E"), g -> {
			g.add("A", "C", add(1));
			g.add("B", "C", add(3));
			g.add("B", "D", add(-5));
			g.add("C", "E", add(10));
		}, (graph, merge) -> {
			assertEquals(List.of(add(-5)), merge.get("E"));
			assertEquals(List.of(add(13)), merge.get("D"));
			assertEquals(List.of(add(6)), merge.get("A"));
			assertEquals(List.of(add(8)), merge.get("B"));
		});
	}

	@Test
	// Merge triple form tree
	public void test5() throws Exception {
		doTest(Set.of("A", "B", "C"), g -> {
			g.add("*", "A", add(1));
			g.add("*", "B", add(10));
			g.add("*", "C", add(100));
		}, (graph, merge) -> {
			assertEquals(List.of(add(110)), merge.get("A"));
			assertEquals(List.of(add(101)), merge.get("B"));
			assertEquals(List.of(add(11)), merge.get("C"));
		});
	}

	@Test
	// Merge W form graph
	public void test6() throws Exception {
		doTest(Set.of("C", "D", "E"), g -> {
			g.add("A", "C", add(3));
			g.add("A", "D", add(10));
			g.add("B", "D", add(1));
			g.add("B", "E", add(30));
		}, (graph, merge) -> {
			assertEquals(List.of(add(40)), merge.get("C"));
			assertEquals(List.of(add(33)), merge.get("D"));
			assertEquals(List.of(add(4)), merge.get("E"));
		});
	}

	@Test
	// Merge equal merges of two nodes
	public void test7() throws Exception {
		doTest(Set.of("C", "D"), g -> {
			g.add("A", "C", add(2));
			g.add("A", "D", add(2));
			g.add("B", "C", add(1));
			g.add("B", "D", add(1));
		}, (graph, merge) -> {
			assertEquals(List.of(), merge.get("C"));
			assertEquals(List.of(), merge.get("D"));
		});
	}

	@Test
	// Merge three equal merges on three nodes
	public void test7a() throws Exception {
		doTest(Set.of("D", "E", "F"), g -> {
			g.add("A", "D", add(5));
			g.add("A", "E", add(5));
			g.add("A", "F", add(5));
			g.add("B", "D", add(4));
			g.add("B", "E", add(4));
			g.add("B", "F", add(4));
			g.add("C", "D", add(3));
			g.add("C", "E", add(3));
			g.add("C", "F", add(3));
		}, (graph, merge) -> {
			assertEquals(List.of(), merge.get("D"));
			assertEquals(List.of(), merge.get("E"));
			assertEquals(List.of(), merge.get("F"));
		});
	}

	@Test
	// Merge full merge and submerge
	public void test8() throws Exception {
		doTest(Set.of("E", "F"), g -> {
			g.add("A", "C", add(10));
			g.add("A", "D", add(100));
			g.add("B", "E", add(10));
			g.add("B", "F", add(110));
			g.add("C", "E", add(1));
			g.add("C", "F", add(101));
			g.add("D", "F", add(11));
		}, (graph, merge) -> {
			assertEquals(List.of(add(100)), merge.get("E"));
			assertEquals(List.of(), merge.get("F"));
		});
	}

	@Test
	// Merge two submerges
	public void test9() throws Exception {
		doTest(Set.of("G", "J"), g -> {
			g.add("A", "C", add(1));
			g.add("A", "D", add(10));
			g.add("B", "E", add(100));
			g.add("B", "F", add(1000));
			g.add("C", "G", add(112));
			g.add("D", "G", add(103));
			g.add("D", "J", add(1102));
			g.add("E", "G", add(14));
			g.add("E", "J", add(1013));
			g.add("F", "J", add(113));
		}, (graph, merge) -> {
			assertEquals(List.of(add(1000)), merge.get("G"));
			assertEquals(List.of(add(1)), merge.get("J"));
		});
	}

	@Test
	// Merge having equal merges parents
	public void test10() throws Exception {
		doTest(Set.of("E", "F", "G"), g -> {
			g.add("A", "C", add(3));
			g.add("A", "D", add(3));
			g.add("B", "C", add(2));
			g.add("B", "D", add(2));
			g.add("C", "E", add(1));
			g.add("C", "F", add(10));
			g.add("D", "G", add(100));
		}, (graph, merge) -> {
			assertEquals(List.of(add(110)), merge.get("E"));
			assertEquals(List.of(add(101)), merge.get("F"));
			assertEquals(List.of(add(11)), merge.get("G"));
		});
	}

	@Test
	// Merge having equal merges parents
	public void test11() throws Exception {
		doTest(Set.of("I", "J"), g -> {
			g.add("A", "C", add(1));
			g.add("A", "D", add(10));
			g.add("B", "E", add(100));
			g.add("B", "F", add(1000));
			g.add("C", "G", add(112));
			g.add("D", "G", add(103));
			g.add("D", "H", add(1102));
			g.add("E", "G", add(14));
			g.add("E", "H", add(1013));
			g.add("F", "H", add(113));
			g.add("G", "I", add(-10));
			g.add("H", "J", add(-100));
		}, (graph, merge) -> {
			assertEquals(List.of(add(900)), merge.get("I"));
			assertEquals(List.of(add(-9)), merge.get("J"));
		});
	}

	@Test
	// Merge of merges should check operations
	public void test12() throws Exception {
		doTest(Set.of("F", "G"), g -> {
			g.add("A", "D", add(1));
			g.add("A", "B", add(100));
			g.add("B", "C", add(2));
			g.add("B", "E", add(3));
			g.add("C", "F", add(1));
			g.add("D", "F", add(102));
			g.add("D", "G", add(103));
			g.add("E", "G", add(1));
		}, (graph, merge) -> {
			assertEquals(List.of(add(3)), merge.get("F"));
			assertEquals(List.of(add(2)), merge.get("G"));
		});
	}

	@Test
	// Should merge in different order
	public void test13() throws Exception {
		doTest(Set.of("F", "C", "E"), g -> {
			g.add("A", "C", add(3));
			g.add("A", "D", add(10));
			g.add("B", "D", add(1));
			g.add("B", "E", add(30));
			g.add("D", "F", add(5));
		}, (graph, merge) -> {
			assertEquals(List.of(add(45)), merge.get("C"));
			assertEquals(List.of(add(33)), merge.get("F"));
			assertEquals(List.of(add(9)), merge.get("E"));
		});
	}

	@Test
	public void test14() throws Exception {
		doTest(Set.of("X", "Y", "Z"), g -> {
			g.add("A", "X", add(1));
			g.add("A", "Z", add(2));
			g.add("B", "X", add(1));
			g.add("B", "Y", add(1));
			g.add("C", "Y", add(1));
			g.add("C", "Z", add(2));
			g.add("B", "Z", add(2));
		}, (graph, merge) -> {
			System.out.println(merge);
		});
	}

	@Test
	public void test15() throws Exception {
		doTest(Set.of("X", "Z"), g -> {
			g.add("A", "U", add(10));
			g.add("A", "Z", add(2));
			g.add("B", "X", add(1));
			g.add("B", "Z", add(2));
			g.add("C", "V", add(100));
			g.add("U", "X", add(-9));
			g.add("V", "Z", add(-98));
		}, (graph, merge) -> {
			System.out.println(merge);
		});
	}

	@Test
	public void test16() throws Exception {
		doTest(Set.of("X", "Y"), g -> {
			g.add("A", "U", add(10));
			g.add("A", "Y", add(2));
			g.add("B", "X", add(1));
			g.add("B", "V", add(100));
			g.add("U", "X", add(-9));
			g.add("V", "Y", add(-98));
		}, (graph, merge) -> {
			System.out.println(merge);
		});
	}

}
