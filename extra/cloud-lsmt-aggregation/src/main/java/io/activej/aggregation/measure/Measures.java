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

package io.activej.aggregation.measure;

import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.fieldtype.FieldTypes;

public class Measures {
	public static Measure sum(FieldType<?> ofType) {
		return new Measure_Sum(ofType);
	}

	public static Measure min(FieldType<?> ofType) {
		return new Measure_Min(ofType);
	}

	public static Measure max(FieldType<?> ofType) {
		return new Measure_Max(ofType);
	}

	public static Measure count(FieldType<?> ofType) {
		return new Measure_Count(ofType);
	}

	public static Measure hyperLogLog(int registers) {
		return new Measure_HyperLogLog(registers);
	}

	public static Measure union(FieldType<?> fieldType) {
		return new Measure_Union(FieldTypes.ofSet(fieldType));
	}
}
