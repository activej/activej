/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.datastream.stats;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;

final class IntrusiveLinkedList<T> implements Iterable<T> {

	@Override
	public @NotNull Iterator<T> iterator() {
		return new Iterator<>() {
			@Nullable Node<T> node = getFirstNode();

			@Override
			public boolean hasNext() {
				return node != null;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				T result = node.value;
				node = node.next;
				return result;
			}
		};
	}

	public static final class Node<T> {
		final T value;
		@Nullable Node<T> prev;
		@Nullable Node<T> next;

		public @Nullable Node<T> getPrev() {
			return prev;
		}

		public @Nullable Node<T> getNext() {
			return next;
		}

		public T getValue() {
			return value;
		}

		Node(T value) {
			this.value = value;
		}
	}

	private int size;
	private @Nullable Node<T> first;
	private @Nullable Node<T> last;

	public @Nullable Node<T> getFirstNode() {
		return first;
	}

	public @Nullable Node<T> getLastNode() {
		return last;
	}

	public @Nullable T getFirstValue() {
		return first != null ? first.value : null;
	}

	public @Nullable T getLastValue() {
		return last != null ? last.value : null;
	}

	public boolean isEmpty() {
		return first == null;
	}

	public void clear() {
		first = null;
		last = null;
	}

	public int size() {
		return size;
	}

	public @Nullable Node<T> removeFirstNode() {
		if (first == null)
			return null;
		Node<T> node = first;
		first = node.next;
		if (node.next != null) {
			node.next.prev = node.prev;
		} else {
			assert last == node;
			last = node.prev;
		}
		size--;
		node.next = node.prev = null;
		return node;
	}

	public @Nullable Node<T> removeLastNode() {
		if (last == null)
			return null;
		Node<T> node = last;
		last = node.prev;
		if (node.prev != null) {
			node.prev.next = node.next;
		} else {
			assert first == node;
			first = node.next;
		}
		size--;
		node.next = node.prev = null;
		return node;
	}

	public @Nullable T removeFirstValue() {
		Node<T> node = removeFirstNode();
		if (node == null) {
			return null;
		}
		return node.getValue();
	}

	public @Nullable T removeLastValue() {
		Node<T> node = removeLastNode();
		if (node == null) {
			return null;
		}
		return node.getValue();
	}

	public Node<T> addFirstValue(T value) {
		Node<T> node = new Node<>(value);
		addFirstNode(node);
		return node;
	}

	public Node<T> addLastValue(T value) {
		Node<T> node = new Node<>(value);
		addLastNode(node);
		return node;
	}

	public void addFirstNode(Node<T> node) {
		checkArgument(node.prev == null && node.next == null, "Node should not have 'next' or 'prev' pointers");
		if (first != null) {
			assert first.prev == null;
			first.prev = node;
			node.next = first;
		} else {
			assert last == null;
			last = node;
		}
		first = node;
		size++;
	}

	public void addLastNode(Node<T> node) {
		checkArgument(node.prev == null && node.next == null, "Node should not have 'next' or 'prev' pointers");
		if (last != null) {
			assert last.next == null;
			last.next = node;
			node.prev = last;
		} else {
			assert first == null;
			first = node;
		}
		last = node;
		size++;
	}

	public void moveNodeToLast(Node<T> node) {
		if (node.next == null)
			return;
		removeNode(node);
		addLastNode(node);
	}

	public void moveNodeToFirst(Node<T> node) {
		if (node.prev == null)
			return;
		removeNode(node);
		addFirstNode(node);
	}

	public Node<T> addAfterNode(@Nullable Node<T> node, T value) {
		checkState(first != null && last != null, "Node is assumed to be in this list");
		// null node in add-after context is "node before the head"
		if (node == null) {
			return addFirstValue(value);
		}
		if (node.next == null) {
			return addLastValue(value);
		}
		Node<T> newNode = new Node<>(value);
		Node<T> next = node.next;
		node.next = newNode;
		next.prev = newNode;
		newNode.prev = node;
		newNode.next = next;
		size++;
		return newNode;
	}

	public Node<T> addBeforeNode(@Nullable Node<T> node, T value) {
		checkState(first != null && last != null); // node is assumed to be in this list
		// null node in add-before context is "node after the tail"
		if (node == null) {
			return addLastValue(value);
		}
		if (node.prev == null) {
			return addFirstValue(value);
		}
		Node<T> newNode = new Node<>(value);
		Node<T> prev = node.prev;
		prev.next = newNode;
		node.prev = newNode;
		newNode.prev = prev;
		newNode.next = node;
		size++;
		return newNode;
	}

	public void removeNode(Node<T> node) {
		if (node.prev != null) {
			node.prev.next = node.next;
		} else {
			assert first == node;
			first = node.next;
		}
		if (node.next != null) {
			node.next.prev = node.prev;
		} else {
			assert last == node;
			last = node.prev;
		}
		node.next = node.prev = null;
		size--;
	}

}
