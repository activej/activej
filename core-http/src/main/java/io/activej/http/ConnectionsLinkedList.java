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

package io.activej.http;

import org.jetbrains.annotations.Nullable;

final class ConnectionsLinkedList {
	private final PoolLabel label;

	private int size;

	@Nullable
	private AbstractHttpConnection first;
	@Nullable
	private AbstractHttpConnection last;

	ConnectionsLinkedList(PoolLabel label) {
		assert label != PoolLabel.NONE;
		this.label = label;
	}

	public boolean isEmpty() {
		return first == null;
	}

	public int size() {
		return size;
	}

	public PoolLabel getLabel() {
		return label;
	}

	public void addLastNode(AbstractHttpConnection node) {
		size++;
		if (last != null) {
			last.next = node;
			node.prev = last;
		} else {
			first = node;
		}
		last = node;
	}

	public void removeNode(AbstractHttpConnection node) {
		size--;
		if (node.prev != null) {
			node.prev.next = node.next;
		} else {
			first = node.next;
		}
		if (node.next != null) {
			node.next.prev = node.prev;
		} else {
			last = node.prev;
		}
		node.next = node.prev = null;
	}

	public int closeExpiredConnections(long expiration) {
		return closeExpiredConnections(expiration, null);
	}

	public int closeExpiredConnections(long expiration, @Nullable Exception e) {
		int count = 0;
		AbstractHttpConnection connection = first;
		while (connection != null) {
			AbstractHttpConnection next = connection.next;
			if (connection.poolTimestamp > expiration)
				break; // connections must back ordered by activity
			if (e == null)
				connection.close();
			else
				connection.closeWithError(e);
			assert connection.prev == null && connection.next == null;
			connection = next;
			count++;
		}
		return count;
	}

	public void closeAllConnections() {
		AbstractHttpConnection connection = first;
		while (connection != null) {
			AbstractHttpConnection next = connection.next;
			connection.close();
			assert connection.prev == null && connection.next == null;
			connection = next;
		}
	}

	@Override
	public String toString() {
		return "ConnectionsLinkedList[" + label + "]{" + "size=" + size + '}';
	}
}
