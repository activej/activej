package io.activej.serializer;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeReference;
import io.activej.serializer.impl.SerializerDefReference;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class SerializerReferenceTest {
	private static final DefiningClassLoader definingClassLoader = DefiningClassLoader.create();

	private static <T> T doTest(Class<T> type, T testData1) {
		BinarySerializer<T> serializer = SerializerBuilder.create(definingClassLoader)
//				.withGeneratedBytecodePath(Paths.get("tmp").toAbsolutePath())
				.build(type);
		return doTest(testData1, serializer, serializer);
	}

	private static <T> T doTest(T testData1, BinarySerializer<T> serializer, BinarySerializer<T> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData1);
		return deserializer.decode(array, 0);
	}

	public static class TestDataReferences {
		@Serialize(order = 0)
		@SerializeNullable
		@SerializeReference
		public String string;

		@Serialize(order = 1)
		@SerializeReference(path = {0})
		@SerializeNullable(path = {0})
		public List<String> list;

		@Serialize(order = 2)
		@SerializeReference(path = {0})
		@SerializeReference(path = {1})
		@SerializeNullable(path = {1})
		public Map<String, String> map;
	}

	@Test
	public void testNullables() {
		SerializerDefReference.reset();

		TestDataReferences testData = new TestDataReferences();

		testData.string = "string";

		testData.list = new ArrayList<>();
		testData.list.add("listString1");
		testData.list.add(null);
		testData.map = new LinkedHashMap<>();
		testData.map.put("1", "mapString1");
		testData.map.put("2", null);
		testData.map.put("3", "mapString3");

		TestDataReferences testData1 = doTest(TestDataReferences.class, testData);
		TestDataReferences testData2 = doTest(TestDataReferences.class, testData);

		assertEquals(testData.string, testData1.string);

		assertSame(testData1.string, testData2.string);
		for (int i = 0; i < testData.list.size(); i++) {
			assertEquals(testData.list.get(i), testData1.list.get(i));
			assertSame(testData1.list.get(i), testData2.list.get(i));
		}

		for (String k : testData.map.keySet()) {
			assertSame(
					testData1.map.keySet().stream().filter(k::equals).findAny().get(),
					testData2.map.keySet().stream().filter(k::equals).findAny().get());
			assertEquals(testData.map.get(k), testData1.map.get(k));
			assertSame(testData1.map.get(k), testData2.map.get(k));
		}
	}

	public static class Container {
		@Serialize(order = 0)
		@SerializeReference
		public SelfReference self1;

		@Serialize(order = 1)
		@SerializeReference
		public SelfReference self2;

		@Serialize(order = 2)
		@SerializeReference
		public CyclicReferenceA cyclicReferenceA;

		@Serialize(order = 3)
		@SerializeReference
		public Node node;
	}

	public static class SelfReference {
		@Serialize(order = 0)
		@SerializeReference
		@SerializeNullable
		public SelfReference selfReference;
	}

	public static class CyclicReferenceA {
		@Serialize(order = 0)
		@SerializeReference
		public CyclicReferenceB cyclicReferenceB;
	}

	public static class CyclicReferenceB {
		@Serialize(order = 0)
		@SerializeReference
		public CyclicReferenceA cyclicReferenceA;
	}

	public static class Node {
		@Serialize(order = 0)
		@SerializeReference(path = {0})
		public List<Node> nodes;
	}

	@Test
	public void testCyclicReferences() {
		SerializerDefReference.reset();

		Container container = new Container();
		container.self1 = new SelfReference();
		container.self2 = new SelfReference();
		container.self2.selfReference = container.self2;
		container.cyclicReferenceA = new CyclicReferenceA();
		container.cyclicReferenceA.cyclicReferenceB = new CyclicReferenceB();
		container.cyclicReferenceA.cyclicReferenceB.cyclicReferenceA = container.cyclicReferenceA;

		Node node1 = new Node();
		Node node2 = new Node();
		node1.nodes = Arrays.asList(node2);
		node2.nodes = Arrays.asList(node1);
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

}
