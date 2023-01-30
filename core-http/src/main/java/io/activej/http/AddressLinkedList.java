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

import io.activej.common.Checks;

import static io.activej.common.Checks.checkArgument;

public final class AddressLinkedList {
	private static final boolean CHECKS = Checks.isEnabled(AddressLinkedList.class);

	private HttpClientConnection first;
	private HttpClientConnection last;

	public boolean isEmpty() {
		return first == null;
	}

	public HttpClientConnection removeLastNode() {
		if (last == null)
			return null;
		HttpClientConnection node = last;
		last = node.addressPrev;
		if (node.addressPrev != null) {
			node.addressPrev.addressNext = node.addressNext;
		} else {
			assert first == node;
			first = node.addressNext;
		}
		node.addressNext = node.addressPrev = null;
		return node;
	}

	public void addLastNode(HttpClientConnection node) {
		if (CHECKS) checkArgument(node.addressPrev == null && node.addressNext == null);
		if (last != null) {
			assert last.addressNext == null;
			last.addressNext = node;
			node.addressPrev = last;
		} else {
			assert first == null;
			first = node;
		}
		last = node;
	}

	public void removeNode(HttpClientConnection node) {
		if (node.addressPrev != null) {
			node.addressPrev.addressNext = node.addressNext;
		} else {
			assert first == node;
			first = node.addressNext;
		}
		if (node.addressNext != null) {
			node.addressNext.addressPrev = node.addressPrev;
		} else {
			assert last == node;
			last = node.addressPrev;
		}
		node.addressNext = node.addressPrev = null;
	}

	public int size() {
		int count = 0;
		for (HttpClientConnection connection = first; connection != null; connection = connection.addressNext) {
			count++;
		}
		return count;
	}
}
