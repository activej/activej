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

package io.activej.cube.ot;

import io.activej.etl.LogDiff;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public interface CubeDiffScheme<D> {
	D wrap(CubeDiff cubeDiff);

	default List<CubeDiff> unwrap(D diff) {
		return unwrapToStream(diff).collect(toList());
	}

	default Stream<CubeDiff> unwrapToStream(D diff) {
		return unwrap(diff).stream();
	}

	static CubeDiffScheme<LogDiff<CubeDiff>> ofLogDiffs() {
		return new CubeDiffScheme<>() {
			@Override
			public LogDiff<CubeDiff> wrap(CubeDiff cubeDiff) {
				return LogDiff.forCurrentPosition(cubeDiff);
			}

			@Override
			public List<CubeDiff> unwrap(LogDiff<CubeDiff> diff) {
				return diff.getDiffs();
			}
		};
	}

	static CubeDiffScheme<CubeDiff> ofCubeDiffs() {
		return new CubeDiffScheme<>() {
			@Override
			public CubeDiff wrap(CubeDiff cubeDiff) {
				return cubeDiff;
			}

			@Override
			public List<CubeDiff> unwrap(CubeDiff diff) {
				return List.of(diff);
			}
		};
	}
}
