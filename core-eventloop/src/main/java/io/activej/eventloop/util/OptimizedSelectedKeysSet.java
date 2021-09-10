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

package io.activej.eventloop.util;

import org.jetbrains.annotations.NotNull;

import java.nio.channels.SelectionKey;
import java.util.AbstractSet;
import java.util.Iterator;

/**
 * The class is used to avoid the overhead in {@link sun.nio.ch.SelectorImpl}
 * This implementation is set instead of {@link java.util.HashSet}
 * It allows to avoid redundant work of GC
 * Should use it as a simple array instead of a set
 */
public final class OptimizedSelectedKeysSet extends AbstractSet<SelectionKey> {
	private static final int INITIAL_SIZE = 1 << 4;
	private int size;
	private SelectionKey[] selectionKeys;

	public OptimizedSelectedKeysSet() {
		selectionKeys = new SelectionKey[INITIAL_SIZE];
	}

	public OptimizedSelectedKeysSet(int initialSize) {
		selectionKeys = new SelectionKey[initialSize];
	}

	@Override
	public boolean add(SelectionKey selectionKey) {
		ensureCapacity();
		selectionKeys[size++] = selectionKey;
		return true;
	}

	/**
	 * Multiply the size of array twice
	 */
	private void ensureCapacity() {
		if (size < selectionKeys.length) {
			return;
		}
		SelectionKey[] newArray = new SelectionKey[selectionKeys.length * 2];
		System.arraycopy(selectionKeys, 0, newArray, 0, size);
		selectionKeys = newArray;
	}

	/**
	 * @param index the pointer to the Selection key from the array,
	 *              must be in range of {@param size}
	 * @return the {@link SelectionKey} from the array by index
	 * @throws ArrayIndexOutOfBoundsException if index is less than 0 or greater than {@code size-1}
	 */
	public SelectionKey get(int index) {
		return selectionKeys[index];
	}

	@Override
	public @NotNull Iterator<SelectionKey> iterator() {
		return new Iterator<SelectionKey>() {
			int step;

			@Override
			public boolean hasNext() {
				return step < size;
			}

			@Override
			public SelectionKey next() {
				return selectionKeys[step++];
			}
		};
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public void clear() {
		size = 0;
	}
}
