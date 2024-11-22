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

package io.activej.common.collection;

import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public final class IteratorUtils {
	private static final Iterator<Object> EMPTY_ITERATOR = new Iterator<>() {
		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Object next() {
			throw new NoSuchElementException();
		}
	};

	@SuppressWarnings("unchecked")
	public static <T> Iterator<T> iteratorOf() {
		return (Iterator<T>) EMPTY_ITERATOR;
	}

	public static <T> Iterator<T> iteratorOf(T item) {
		return new Iterator<>() {
			boolean hasNext = true;

			@Override
			public boolean hasNext() {
				return hasNext;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				hasNext = false;
				return item;
			}
		};
	}

	public static <T> Iterator<T> iteratorOf(T item1, T item2) {
		return new Iterator<>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < 2;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				return i++ == 0 ? item1 : item2;
			}
		};
	}

	@SafeVarargs
	public static <T> Iterator<T> iteratorOf(T... items) {
		return new Iterator<>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				return i < items.length;
			}

			@Override
			public T next() {
				if (!hasNext()) throw new NoSuchElementException();
				return items[i++];
			}
		};
	}

	public static <T, R> Iterator<R> transformIterator(Iterator<? extends T> iterator, Function<? super T, ? extends R> fn) {
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public R next() {
				return fn.apply(iterator.next());
			}
		};
	}

	public static <T> Iterator<T> concat(Iterator<? extends T> iterator1, Iterator<? extends T> iterator2) {
		return concat(iteratorOf(iterator1, iterator2));
	}

	public static <T> Iterator<T> append(Iterator<? extends T> iterator, T value) {
		return new Iterator<>() {
			boolean hasNext = true;

			@Override
			public boolean hasNext() {
				return hasNext;
			}

			@Override
			public T next() {
				if (iterator.hasNext()) {
					return iterator.next();
				}
				if (!hasNext) throw new NoSuchElementException();
				hasNext = false;
				return value;
			}
		};
	}

	public static <T> Iterator<T> prepend(T value, Iterator<? extends T> iterator) {
		return new Iterator<>() {
			@Nullable
			Iterator<? extends T> it;

			@Override
			public boolean hasNext() {
				return it == null || it.hasNext();
			}

			@Override
			public T next() {
				if (it != null) {
					return it.next();
				}
				this.it = iterator;
				return value;
			}
		};
	}

	public static <T> Iterator<T> concat(Iterator<? extends Iterator<? extends T>> iterators) {
		return new Iterator<>() {
			@Nullable Iterator<? extends T> it = findNextIterator(iterators);

			@Override
			public boolean hasNext() {
				return it != null;
			}

			@Override
			public T next() {
				if (it == null) throw new NoSuchElementException();
				T next = it.next();
				if (!it.hasNext()) {
					it = findNextIterator(iterators);
				}
				return next;
			}

			private static <T> @Nullable Iterator<? extends T> findNextIterator(Iterator<? extends Iterator<? extends T>> iterators) {
				while (iterators.hasNext()) {
					Iterator<? extends T> it = iterators.next();
					if (it.hasNext()) return it;
				}
				return null;
			}
		};
	}
}
