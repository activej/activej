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

package io.activej.fs.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.binary.ByteBufsDecoder;
import org.jetbrains.annotations.Nullable;

public final class ProtobufUtils {

	public static <I extends Message, O extends Message> ByteBufsCodec<I, O> codec(Parser<I> inputParser) {
		return new ByteBufsCodec<>() {
			@Override
			public ByteBuf encode(O item) {
				byte[] bytes = item.toByteArray();

				int length = bytes.length;
				ByteBuf buf = ByteBufPool.allocate(length + 5);

				buf.writeVarInt(length);
				buf.put(bytes);
				return buf;
			}

			@Override
			public @Nullable I tryDecode(ByteBufs bufs) throws MalformedDataException {
				return ByteBufsDecoder.ofVarIntSizePrefixedBytes()
						.andThen(buf -> {
							try {
								return inputParser.parseFrom(buf.asArray());
							} catch (InvalidProtocolBufferException e) {
								throw new MalformedDataException(e);
							}
						})
						.tryDecode(bufs);
			}
		};
	}
}
