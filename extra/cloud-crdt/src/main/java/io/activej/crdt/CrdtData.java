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

package io.activej.crdt;

public final class CrdtData<K extends Comparable<K>, S> extends CrdtEntity<K> {
	private final long timestamp;
	private final S state;

	public CrdtData(K key, long timestamp, S state) {
		super(key);
		this.timestamp = timestamp;
		this.state = state;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public S getState() {
		return state;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CrdtData<?, ?> crdtData = (CrdtData<?, ?>) o;

		return key.equals(crdtData.key) && state.equals(crdtData.state) && timestamp == crdtData.timestamp;
	}

	@Override
	public int hashCode() {
		int result = key.hashCode();
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + state.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "CrdtData{key=" + key + ", timestamp=" + timestamp + ", state=" + state + '}';
	}
}
