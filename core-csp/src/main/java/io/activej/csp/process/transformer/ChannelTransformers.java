package io.activej.csp.process.transformer;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.MemSize;
import io.activej.common.annotation.StaticFactories;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.process.transformer.impl.ByteChunker;
import io.activej.csp.process.transformer.impl.ByteRanger;
import io.activej.csp.supplier.ChannelSupplier;

import static io.activej.common.Checks.checkArgument;

@StaticFactories(ChannelTransformer.class)
public class ChannelTransformers {

	public static <T> ChannelTransformer<T, T> identity() {
		return new ChannelTransformer<>() {
			@Override
			public ChannelConsumer<T> transform(ChannelConsumer<T> consumer) {
				return consumer;
			}

			@Override
			public ChannelSupplier<T> transform(ChannelSupplier<T> supplier) {
				return supplier;
			}
		};
	}

	public static ChannelTransformer<ByteBuf, ByteBuf> chunkBytes(MemSize minChunkSize, MemSize maxChunkSize) {
		checkArgument(minChunkSize.toInt(), minSize -> minSize > 0, "Minimal chunk size should be greater than 0");
		checkArgument(maxChunkSize.toInt(), maxSize -> maxSize >= minChunkSize.toInt(), "Maximal chunk size cannot be less than minimal chunk size");

		return new ByteChunker(minChunkSize.toInt(), maxChunkSize.toInt());
	}

	public static ChannelTransformer<ByteBuf, ByteBuf> rangeBytes(long offset, long length) {
		if (offset == 0 && length == Long.MAX_VALUE) {
			return identity();
		}
		return new ByteRanger(offset, length);
	}

	public static ChannelTransformer<ByteBuf, ByteBuf> dropBytes(long toDrop) {
		return rangeBytes(toDrop, Long.MAX_VALUE);
	}

	public static ChannelTransformer<ByteBuf, ByteBuf> limitBytes(long limit) {
		return rangeBytes(0, limit);
	}
}
