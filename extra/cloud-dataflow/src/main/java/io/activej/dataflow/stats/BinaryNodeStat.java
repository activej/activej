package io.activej.dataflow.stats;

import com.dslplatform.json.CompiledJson;
import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.dsl.ChannelTransformer;

import java.util.Objects;

@CompiledJson
public class BinaryNodeStat extends NodeStat implements ChannelTransformer<ByteBuf, ByteBuf> {
	public static final StatReducer<BinaryNodeStat> REDUCER =
			stats -> {
				long sum = stats.stream()
						.filter(Objects::nonNull)
						.mapToLong(BinaryNodeStat::getBytes)
						.reduce(0, Long::sum);
				BinaryNodeStat stat = new BinaryNodeStat();
				stat.record(sum);
				return stat;
			};

	private long bytes = 0;

	public BinaryNodeStat() {
	}

	public BinaryNodeStat(long bytes) {
		this.bytes = bytes;
	}

	public void record(long bytes) {
		this.bytes += bytes;
	}

	@Override
	public ChannelConsumer<ByteBuf> transform(ChannelConsumer<ByteBuf> consumer) {
		return consumer.peek(buf -> record(buf.readRemaining()));
	}

	@Override
	public ChannelSupplier<ByteBuf> transform(ChannelSupplier<ByteBuf> supplier) {
		return supplier.peek(buf -> record(buf.readRemaining()));
	}

	public long getBytes() {
		return bytes;
	}

	@Override
	public String toString() {
		return Long.toString(bytes);
	}
}
