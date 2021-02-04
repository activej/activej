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

import io.activej.common.exception.MalformedDataException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * A base exception for any 'expected' Redis exceptions.
 * <p>
 * Expected exceptions are the ones returned from Redis server or caused by actions of a user.
 * <p>
 * Any other exception (e.g. {@link IOException}, {@link MalformedDataException} etc.) are wrapped into {@link RedisException}
 */
public class ExpectedRedisException extends RedisException {
	public ExpectedRedisException(@NotNull String message) {
		super(message);
	}
}
