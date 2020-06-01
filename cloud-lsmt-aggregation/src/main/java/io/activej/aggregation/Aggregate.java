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

package io.activej.aggregation;

/**
 * Accumulates records using arithmetic summation.
 */
public interface Aggregate<T, A> {
	/**
	 * Creates an object which holds arithmetic sum of accumulated records.
	 *
	 * @param record record whose fields are to be accumulated
	 * @return accumulator object
	 */
	A createAccumulator(T record);

	/**
	 * Performs the following operation: accumulator = accumulator + record.
	 *
	 * @param accumulator accumulator which holds the current sum of records
	 * @param record      record to add to the accumulator
	 */
	void accumulate(A accumulator, T record);
}
