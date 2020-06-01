package io.activej.common;

import io.activej.common.collection.IntrusiveLinkedList;
import io.activej.common.collection.IntrusiveLinkedList.Node;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class IntrusiveLinkedListTest {
	private IntrusiveLinkedList<String> list;

	@Before
	public void before() {
		list = new IntrusiveLinkedList<>();
	}

	@Test
	public void testAdd() {
		assertTrue(list.isEmpty());
		assertEquals(0, list.size());
		assertNull(list.getFirstNode());
		assertNull(list.getLastNode());

		list.addFirstValue("2");
		assertEquals("2", list.getFirstNode().getValue());
		assertEquals("2", list.getLastNode().getValue());

		list.addFirstValue("1");
		assertEquals("1", list.getFirstNode().getValue());
		assertEquals("2", list.getLastNode().getValue());

		list.addLastValue("3");
		assertEquals("3", list.getLastNode().getValue());

		list.addLastValue("4");
		assertEquals("4", list.getLastNode().getValue());

		assertFalse(list.isEmpty());
		assertEquals(4, list.size());
		assertList("1", "2", "3", "4");

		list.clear();
		list.addLastValue("2");
		assertList("2");
	}

	@Test
	public void testRemove() {
		assertNull(list.removeFirstNode());
		assertNull(list.removeLastNode());

		list.addLastValue("1");
		Node<String> node = list.addLastValue("2");
		list.addLastValue("3");
		list.addLastValue("4");

		list.removeNode(node);
		assertList("1", "3", "4");

		assertNotNull(list.removeFirstNode());
		assertList("3", "4");

		assertNotNull(list.removeLastNode());
		assertList("3");

		list.clear();
		assertTrue(list.isEmpty());

		list.addLastValue("new");
		assertList("new");
	}

	@Test
	public void testMoveToFirst() {
		list.addLastValue("1");
		Node<String> el1 = list.addLastValue("2");
		list.addLastValue("3");
		Node<String> el2 = list.addLastValue("4");
		assertEquals("1", list.getFirstNode().getValue());
		assertEquals("4", list.getLastNode().getValue());

		list.moveNodeToFirst(el1);
		assertEquals("2", list.getFirstNode().getValue());
		assertEquals("4", list.getLastNode().getValue());
		assertList("2", "1", "3", "4");

		list.moveNodeToFirst(el1);
		assertEquals("2", list.getFirstNode().getValue());
		assertEquals("4", list.getLastNode().getValue());
		assertList("2", "1", "3", "4");

		list.moveNodeToFirst(el2);
		assertEquals("4", list.getFirstNode().getValue());
		assertEquals("3", list.getLastNode().getValue());
		assertList("4", "2", "1", "3");
	}

	@Test
	public void testMoveToLast() {
		list.addLastValue("1");
		Node<String> el1 = list.addLastValue("2");
		list.addLastValue("3");
		Node<String> el2 = list.addLastValue("4");

		assertEquals("1", list.getFirstNode().getValue());
		assertEquals("4", list.getLastNode().getValue());

		list.moveNodeToLast(el1);
		assertEquals("1", list.getFirstNode().getValue());
		assertEquals("2", list.getLastNode().getValue());
		assertList("1", "3", "4", "2");

		list.moveNodeToLast(el1);
		assertEquals("1", list.getFirstNode().getValue());
		assertEquals("2", list.getLastNode().getValue());
		assertList("1", "3", "4", "2");

		list.moveNodeToLast(el2);
		assertEquals("1", list.getFirstNode().getValue());
		assertEquals("4", list.getLastNode().getValue());
		assertList("1", "3", "2", "4");
	}

	private void assertList(String... expected) {
		Node<String> node = list.getFirstNode();
		for (String e : expected) {
			assertNotNull(node);
			assertEquals(e, node.getValue());
			node = node.getNext();
		}
	}

}
