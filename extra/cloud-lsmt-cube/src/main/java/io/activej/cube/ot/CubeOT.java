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

import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationOT;
import io.activej.ot.TransformResult;
import io.activej.ot.exception.TransformException;
import io.activej.ot.system.OTSystem;
import io.activej.ot.system.OTSystemImpl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.activej.common.Utils.union;

public class CubeOT {
	public static OTSystem<CubeDiff> createCubeOT() {
		OTSystem<AggregationDiff> aggregationOTSystem = AggregationOT.createAggregationOT();
		return OTSystemImpl.<CubeDiff>builder()
				.withTransformFunction(CubeDiff.class, CubeDiff.class, (left, right) -> {
					Map<String, AggregationDiff> newOpsLeft = new LinkedHashMap<>();
					Map<String, AggregationDiff> newOpsRight = new LinkedHashMap<>();

					for (String aggregation : union(left.keySet(), right.keySet())) {
						AggregationDiff leftOps = left.get(aggregation);
						AggregationDiff rightOps = right.get(aggregation);
						if (leftOps == null) leftOps = AggregationDiff.empty();
						if (rightOps == null) rightOps = AggregationDiff.empty();

						TransformResult<AggregationDiff> transformed = aggregationOTSystem.transform(leftOps, rightOps);
						if (transformed.hasConflict())
							return TransformResult.conflict(transformed.resolution);

						if (transformed.left.size() > 1)
							throw new TransformException("Left transformation result has more than one aggregation diff");
						if (transformed.right.size() > 1)
							throw new TransformException("Right transformation result has more than one aggregation diff");

						if (!transformed.left.isEmpty())
							newOpsLeft.put(aggregation, transformed.left.get(0));
						if (!transformed.right.isEmpty())
							newOpsRight.put(aggregation, transformed.right.get(0));
					}
					return TransformResult.of(CubeDiff.of(newOpsLeft), CubeDiff.of(newOpsRight));
				})
				.withEmptyPredicate(CubeDiff.class, CubeDiff::isEmpty)
				.withInvertFunction(CubeDiff.class, commit -> List.of(commit.inverse()))
				.withSquashFunction(CubeDiff.class, CubeDiff.class, (commit1, commit2) -> {
					Map<String, AggregationDiff> newOps = new LinkedHashMap<>();
					for (String aggregation : union(commit1.keySet(), commit2.keySet())) {
						AggregationDiff ops1 = commit1.get(aggregation);
						AggregationDiff ops2 = commit2.get(aggregation);
						if (ops1 == null) {
							newOps.put(aggregation, ops2);
						} else if (ops2 == null) {
							newOps.put(aggregation, ops1);
						} else {
							List<AggregationDiff> ops = new ArrayList<>();
							ops.add(ops1);
							ops.add(ops2);
							List<AggregationDiff> simplified = aggregationOTSystem.squash(ops);
							if (!simplified.isEmpty()) {
								if (simplified.size() > 1)
									throw new AssertionError();
								newOps.put(aggregation, simplified.get(0));
							}
						}
					}
					return CubeDiff.of(newOps);
				})
				.build();
	}

}
