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

package io.activej.csp.binary;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.function.UnaryOperator;

public interface ByteBufsCodec<I, O> {
	ByteBuf encode(O item);

	@Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException;

	default <I1, O1> @NotNull ByteBufsCodec<I1, O1> andThen(DecoderFunction<? super I, ? extends I1> decoder, Function<? super O1, ? extends O> encoder) {
		return new ByteBufsCodec<>() {
			@Override
			public ByteBuf encode(O1 item) {
				return ByteBufsCodec.this.encode(encoder.apply(item));
			}

			@Override
			public @Nullable I1 tryDecode(ByteBufs bufs) throws MalformedDataException {
				I maybeResult = ByteBufsCodec.this.tryDecode(bufs);
				if (maybeResult == null) return null;
				return decoder.decode(maybeResult);
			}
		};
	}

	static @NotNull ByteBufsCodec<ByteBuf, ByteBuf> ofDelimiter(ByteBufsDecoder<ByteBuf> delimiterIn, UnaryOperator<ByteBuf> delimiterOut) {
		return new ByteBufsCodec<>() {
			@Override
			public ByteBuf encode(ByteBuf buf) {
				return delimiterOut.apply(buf);
			}

			@Override
			public @Nullable ByteBuf tryDecode(ByteBufs bufs) throws MalformedDataException {
				return delimiterIn.tryDecode(bufs);
			}
		};
	}

}
