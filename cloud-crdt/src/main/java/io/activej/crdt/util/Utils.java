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

package io.activej.crdt.util;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.codec.StructuredCodec;
import io.activej.codec.json.JsonUtils;
import io.activej.crdt.CrdtException;
import io.activej.csp.binary.ByteBufsCodec;
import io.activej.csp.binary.ByteBufsDecoder;
import io.activej.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Utils {

	public static <I, O> ByteBufsCodec<I, O> nullTerminatedJson(StructuredCodec<I> in, StructuredCodec<O> out) {
		return ByteBufsCodec
				.ofDelimiter(
						ByteBufsDecoder.ofNullTerminatedBytes(),
						buf -> {
							ByteBuf buf1 = ByteBufPool.ensureWriteRemaining(buf, 1);
							buf1.put((byte) 0);
							return buf1;
						})
				.andThen(
						buf -> JsonUtils.fromJson(in, buf.asString(UTF_8)),
						item -> JsonUtils.toJsonBuf(out, item));
	}

	public static <T> BiFunction<T, @Nullable Throwable, Promise<? extends T>> wrapException(Supplier<String> errorMessageSupplier) {
		return (v, e) -> e == null ?
				Promise.of(v) :
				e instanceof CrdtException ?
						Promise.ofException(e) :
						Promise.ofException(new CrdtException(errorMessageSupplier.get(), e));
	}
}
