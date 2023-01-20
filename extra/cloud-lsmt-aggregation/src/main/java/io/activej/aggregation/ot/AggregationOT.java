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

package io.activej.aggregation.ot;

import io.activej.ot.TransformResult;
import io.activej.ot.TransformResult.ConflictResolution;
import io.activej.ot.system.OTSystem;
import io.activej.ot.system.OTSystemImpl;

import java.util.List;

import static io.activej.common.Utils.hasIntersection;

public class AggregationOT {
	public static OTSystem<AggregationDiff> createAggregationOT() {
		return OTSystemImpl.<AggregationDiff>builder()
				.withTransformFunction(AggregationDiff.class, AggregationDiff.class, (left, right) -> {
					if (!hasIntersection(left.getAddedChunks(), right.getAddedChunks()) && !hasIntersection(left.getRemovedChunks(), right.getRemovedChunks())) {
						return TransformResult.of(right, left);
					}

					return left.getRemovedChunks().size() > right.getRemovedChunks().size() ?
							TransformResult.conflict(ConflictResolution.LEFT) :
							TransformResult.conflict(ConflictResolution.RIGHT);
				})
				.withInvertFunction(AggregationDiff.class, op -> List.of(op.inverse()))
				.withEmptyPredicate(AggregationDiff.class, AggregationDiff::isEmpty)
				.withSquashFunction(AggregationDiff.class, AggregationDiff.class, AggregationDiff::squash)
				.build();
	}

}
