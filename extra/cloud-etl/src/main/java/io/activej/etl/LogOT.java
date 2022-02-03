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

import io.activej.ot.TransformResult;
import io.activej.ot.TransformResult.ConflictResolution;
import io.activej.ot.system.OTSystem;
import io.activej.ot.system.OTSystemImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.Utils.*;

public class LogOT {
	public static <T> OTSystem<LogDiff<T>> createLogOT(OTSystem<T> otSystem) {
		return OTSystemImpl.<LogDiff<T>>create()
				.withTransformFunction(LogDiff.class, LogDiff.class, (left, right) -> {
					Set<String> intersection = intersection(left.getPositions().keySet(), right.getPositions().keySet());
					if (intersection.isEmpty()) {
						TransformResult<T> transformed = otSystem.transform(left.getDiffs(), right.getDiffs());
						if (transformed.hasConflict()) {
							return TransformResult.conflict(transformed.resolution);
						}
						return TransformResult.of(
								LogDiff.of(right.getPositions(), transformed.left),
								LogDiff.of(left.getPositions(), transformed.right));
					}

					int comparison = 0;
					for (String log : intersection) {
						LogPositionDiff leftPosition = left.getPositions().get(log);
						LogPositionDiff rightPosition = right.getPositions().get(log);
						checkArgument(leftPosition.from().equals(rightPosition.from()),
								"'From' values should be equal for left and right log positions");
						comparison += leftPosition.compareTo(rightPosition);
					}

					return TransformResult.conflict(comparison > 0 ? ConflictResolution.LEFT : ConflictResolution.RIGHT);
				})
				.withEmptyPredicate(LogDiff.class, commit -> commit.getPositions().isEmpty() && commit.getDiffs().stream().allMatch(otSystem::isEmpty))
				.withSquashFunction(LogDiff.class, LogDiff.class, (commit1, commit2) -> {
					Map<String, LogPositionDiff> positions = new HashMap<>(commit1.getPositions());
					for (Entry<String, LogPositionDiff> entry : commit2.getPositions().entrySet()) {
						String log = entry.getKey();
						LogPositionDiff positionDiff1 = positions.get(log);
						LogPositionDiff positionDiff2 = entry.getValue();
						if (positionDiff1 != null) {
							checkState(positionDiff1.to().equals(positionDiff2.from()),
									"'To' value of the first log position should be equal to 'From' value of the second log position");
							positionDiff2 = new LogPositionDiff(positionDiff1.from(), positionDiff2.to());
						}
						if (!positionDiff2.isEmpty()) {
							positions.put(log, positionDiff2);
						} else {
							positions.remove(log);
						}
					}
					List<T> ops = concat(commit1.getDiffs(), commit2.getDiffs());
					return LogDiff.of(positions, otSystem.squash(ops));
				})
				.withInvertFunction(LogDiff.class, commit -> List.of(LogDiff.of(
						transformMap(commit.getPositions(), LogPositionDiff::inverse),
						otSystem.invert(commit.getDiffs()))));

	}

}
