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

package io.activej.csp.binary.decoder;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.DecoderFunction;
import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface ByteBufsDecoder<T> {

	@Nullable T tryDecode(ByteBufs bufs) throws MalformedDataException;

	default <V> ByteBufsDecoder<V> andThen(DecoderFunction<? super T, ? extends V> after) {
		return bufs -> {
			T maybeResult = tryDecode(bufs);
			if (maybeResult == null) return null;
			return after.decode(maybeResult);
		};
	}
}
