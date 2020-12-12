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

package io.activej.redis;

import io.activej.bytebuf.ByteBufQueue;
import io.activej.common.exception.parse.ParseException;
import org.jetbrains.annotations.Nullable;

public interface RedisProtocol {
	int encode(byte[] array, int offset, RedisCommand item) throws ArrayIndexOutOfBoundsException;

	@Nullable
	RedisResponse tryDecode(ByteBufQueue queue) throws ParseException;
}
