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

package io.activej.serializer.stream;

import java.lang.reflect.Type;

/**
 * This is an interface for something that can create or retrieve a codec for a given type.
 */
public interface StreamCodecFactory {
	default <T> StreamCodec<T> get(Class<T> type) {
		return get((Type) type);
	}

	default <T> StreamCodec<T> get(StreamCodecT<T> type) {
		return get(type.getType());
	}

	<T> StreamCodec<T> get(Type type);

}
