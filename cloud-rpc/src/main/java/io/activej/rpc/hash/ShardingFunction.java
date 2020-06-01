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

package io.activej.rpc.hash;

/**
 * Sharder interface determines to which shard item will be sent
 */
@FunctionalInterface
public interface ShardingFunction<T> {
	/**
	 * Determines to which shard {@code item} will be sent
	 *
	 * @param item data to be sent
	 * @return shard number
	 */
	int getShard(T item);
}
