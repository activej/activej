package io.activej.serializer;

import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeReference;
import io.activej.test.rules.ClassBuilderConstantsRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.serializer.Utils.doTest;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class SerializerReferenceTest {

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	public static class TestDataReferences {
		@Serialize
		@SerializeNullable
		@SerializeReference
		public String string;

		@Serialize
		public List<@SerializeReference @SerializeNullable String> list;

		@Serialize
		public Map<@SerializeReference String, @SerializeReference @SerializeNullable String> map;
	}

	@Test
	public void testNullables() {
		TestDataReferences testData = new TestDataReferences();

		testData.string = "string";

		testData.list = new ArrayList<>();
		testData.list.add("string");
		testData.list.add(null);
		testData.map = new LinkedHashMap<>();
		testData.map.put("1", "string");
		testData.map.put("2", null);
		testData.map.put("3", "string");

		TestDataReferences testData1 = doTest(TestDataReferences.class, testData);

		assertEquals(testData.string, testData1.string);
		assertEquals(testData.list, testData1.list);
		assertEquals(testData.map, testData1.map);
		assertSame(testData1.string, testData1.list.get(0));
		assertSame(testData1.string, testData1.map.get("1"));
		assertSame(testData1.string, testData1.map.get("3"));
	}

	public static class Container {
		@Serialize
		public SelfReference self1;

		@Serialize
		public SelfReference self2;

		@Serialize
		@SerializeReference
		public CyclicReferenceA cyclicReferenceA;

		@Serialize
		@SerializeReference
		public Node node;
	}

	@SerializeReference
	public static class SelfReference {
		@Serialize
		@SerializeNullable
		public SelfReference selfReference;
	}

	public static class CyclicReferenceA {
		@Serialize
		@SerializeReference
		public CyclicReferenceB cyclicReferenceB;
	}

	public static class CyclicReferenceB {
		@Serialize
		@SerializeReference
		public CyclicReferenceA cyclicReferenceA;
	}

	public static class Node {
		@Serialize
		public List<@SerializeReference Node> nodes;
	}

	@Test
	public void testCyclicReferences() {
		Container container = new Container();
		container.self1 = new SelfReference();
		container.self2 = new SelfReference();
		container.self2.selfReference = container.self2;
		container.cyclicReferenceA = new CyclicReferenceA();
		container.cyclicReferenceA.cyclicReferenceB = new CyclicReferenceB();
		container.cyclicReferenceA.cyclicReferenceB.cyclicReferenceA = container.cyclicReferenceA;

		Node node1 = new Node();
		Node node2 = new Node();
		node1.nodes = singletonList(node2);
		node2.nodes = singletonList(node1);
		container.node = node1;

		Container container1 = doTest(Container.class, container);

		assertNull(container1.self1.selfReference);
		assertNotNull(container1.self2);
		assertNotNull(container1.cyclicReferenceA);
		assertNotNull(container1.node);
		assertSame(container1.self2, container1.self2.selfReference);
		assertSame(container1.cyclicReferenceA, container1.cyclicReferenceA.cyclicReferenceB.cyclicReferenceA);
		assertSame(container1.node, container1.node.nodes.get(0).nodes.get(0));
		assertSame(container1.node.nodes.get(0), container1.node.nodes.get(0).nodes.get(0).nodes.get(0));
	}

	@SerializeReference
	public static class ContainerCyclicReference {
		@Serialize
		@SerializeNullable
		public ContainerCyclicReference ref;
	}

	@Test
	public void testContainerCyclicReference() {
		ContainerCyclicReference container = new ContainerCyclicReference();

		ContainerCyclicReference container1 = doTest(ContainerCyclicReference.class, container);
		assertNull(container1.ref);

		container.ref = container;
		ContainerCyclicReference container2 = doTest(ContainerCyclicReference.class, container);
		assertSame(container2, container2.ref);
	}

}
