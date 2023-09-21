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

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class LogDiff<D> {
	private final Map<String, LogPositionDiff> positions;
	private final List<D> diffs;

	private LogDiff(Map<String, LogPositionDiff> positions, List<D> diffs) {
		this.positions = positions;
		this.diffs = diffs;
	}

	public static <D> LogDiff<D> of(Map<String, LogPositionDiff> positions, List<D> diffs) {
		return new LogDiff<>(positions, diffs);
	}

	public static <D> LogDiff<D> of(Map<String, LogPositionDiff> positions, D diff) {
		return of(positions, List.of(diff));
	}

	public static <D> LogDiff<D> empty() {
		return of(emptyMap(), emptyList());
	}

	public static <D> LogDiff<D> reduce(List<LogDiff<D>> logDiffs, Function<List<D>, D> diffsReducer) {
		Map<String, LogPositionDiff> positions = new LinkedHashMap<>();
		List<D> diffs = new ArrayList<>();
		for (var logDiff : logDiffs) {
			for (var entry : logDiff.getPositions().entrySet()) {
				positions.compute(entry.getKey(),
					(partitionId, logPositionDiff) ->
						logPositionDiff == null ?
							entry.getValue() :
							new LogPositionDiff(logPositionDiff.from(), entry.getValue().to())
				);
			}
			diffs.add(diffsReducer.apply(logDiff.getDiffs()));
		}
		return LogDiff.of(positions, diffsReducer.apply(diffs));
	}


	public static <D> LogDiff<D> forCurrentPosition(List<D> diffs) {
		return of(Map.of(), diffs);
	}

	public static <D> LogDiff<D> forCurrentPosition(D diff) {
		return forCurrentPosition(List.of(diff));
	}

	public Map<String, LogPositionDiff> getPositions() {
		return positions;
	}

	public List<D> getDiffs() {
		return diffs;
	}

	public Stream<D> diffs() {
		return diffs.stream();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LogDiff<?> logDiff = (LogDiff<?>) o;

		if (!Objects.equals(positions, logDiff.positions)) return false;
		return Objects.equals(diffs, logDiff.diffs);
	}

	@Override
	public int hashCode() {
		int result = positions != null ? positions.hashCode() : 0;
		result = 31 * result + (diffs != null ? diffs.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "{positions:" + positions.keySet() + ", diffs:" + diffs.size() + '}';
	}
}
