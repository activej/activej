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

package io.activej.csp.process.transformer.impl;

import io.activej.bytebuf.ByteBuf;
import io.activej.csp.process.transformer.AbstractChannelTransformer;
import io.activej.promise.Promise;

/**
 * Drops exactly N bytes from a csp stream of byte buffers and limits that stream to exactly M bytes in length
 */
public final class ByteRanger extends AbstractChannelTransformer<ByteRanger, ByteBuf, ByteBuf> {
	public final long offset;
	public final long endOffset;

	public long position;

	public ByteRanger(long offset, long length) {
		this.offset = offset;
		this.endOffset = length;
	}

	@Override
	protected Promise<Void> onItem(ByteBuf item) {
		int size = item.readRemaining();
		long oldPos = position;
		position += size;
		if (oldPos > endOffset || position <= offset) {
			item.recycle();
			return Promise.complete();
		}
		if (oldPos < offset) {
			item.moveHead((int) (offset - oldPos));
		}
		if (position > endOffset) {
			item.moveTail((int) (endOffset - position));
		}
		return send(item);
	}
}
