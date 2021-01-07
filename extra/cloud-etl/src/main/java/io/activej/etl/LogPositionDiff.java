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

package io.activej.etl;

import io.activej.multilog.LogPosition;

import java.util.Objects;

public final class LogPositionDiff implements Comparable<LogPositionDiff> {
	public final LogPosition from;
	public final LogPosition to;

	public LogPositionDiff(LogPosition from, LogPosition to) {
		this.from = from;
		this.to = to;
	}

	public LogPositionDiff inverse() {
		return new LogPositionDiff(to, from);
	}

	public boolean isEmpty() {
		return from.equals(to);
	}

	@Override
	public int compareTo(LogPositionDiff o) {
		return this.to.compareTo(o.to);
	}

	@Override
	public String toString() {
		return "LogPositionDiff{" +
				"from=" + from +
				", to=" + to +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LogPositionDiff that = (LogPositionDiff) o;

		if (!Objects.equals(from, that.from)) return false;
		return Objects.equals(to, that.to);
	}

	@Override
	public int hashCode() {
		int result = from != null ? from.hashCode() : 0;
		result = 31 * result + (to != null ? to.hashCode() : 0);
		return result;
	}
}
