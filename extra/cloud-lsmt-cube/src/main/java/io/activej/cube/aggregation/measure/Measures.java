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

package io.activej.cube.aggregation.measure;

import io.activej.common.annotation.StaticFactories;
import io.activej.common.time.CurrentTimeProvider;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.fieldtype.FieldTypes;
import io.activej.cube.aggregation.measure.impl.*;
import io.activej.cube.aggregation.util.Utils;

import static io.activej.cube.aggregation.util.Utils.valueWithTimestampFieldType;

@StaticFactories(Measure.class)
public class Measures {
	public static Measure sum(FieldType<?> ofType) {
		return new Sum(ofType);
	}

	public static Measure min(FieldType<?> ofType) {
		return new Min(ofType);
	}

	public static Measure max(FieldType<?> ofType) {
		return new Max(ofType);
	}

	public static Measure count(FieldType<?> ofType) {
		return new Count(ofType);
	}

	public static Measure hyperLogLog(int registers) {
		return new HyperLogLog(registers);
	}

	public static Measure union(FieldType<?> fieldType) {
		return new Union(FieldTypes.ofSet(fieldType));
	}

	public static Measure lastNotNull(FieldType<?> fieldType, CurrentTimeProvider timeProvider) {
		return new LastNotNull(valueWithTimestampFieldType(fieldType), timeProvider);
	}

	public static Measure lastNotNull(FieldType<?> fieldType) {
		return new LastNotNull(valueWithTimestampFieldType(fieldType), null);
	}

	public static Measure last(FieldType<?> fieldType, CurrentTimeProvider timeProvider) {
		return new Last(valueWithTimestampFieldType(fieldType), timeProvider);
	}

	public static Measure last(FieldType<?> fieldType) {
		return new Last(valueWithTimestampFieldType(fieldType), null);
	}
}
