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

package io.activej.crdt.wal;

import io.activej.promise.Promise;

import static io.activej.common.Checks.checkArgument;

public final class WriteAheadLogAdapters {

	public static <K extends Comparable<K>, S> WriteAheadLog<K, S> flushOnUpdatesCount(WriteAheadLog<K, S> original, int updatesCount) {
		checkArgument(updatesCount > 0);

		return new ForwardingWriteAheadLog<K, S>(original) {
			private int count;

			@Override
			public Promise<Void> put(K key, S value) {
				return super.put(key, value)
						.whenResult($ -> ++count == updatesCount, this::flush);
			}

			@Override
			public Promise<Void> flush() {
				count = 0;
				return super.flush();
			}
		};
	}
}
