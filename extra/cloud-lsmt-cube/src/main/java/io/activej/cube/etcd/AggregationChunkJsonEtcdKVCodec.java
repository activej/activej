package io.activej.cube.etcd;

import io.activej.aggregation.AggregationChunk;
import io.activej.aggregation.PrimaryKey;
import io.activej.common.exception.MalformedDataException;
import io.activej.etcd.codec.EtcdKVCodec;
import io.activej.etcd.codec.KeyValue;
import io.activej.json.JsonCodec;
import io.activej.json.JsonCodecs;
import io.etcd.jetcd.ByteSequence;

import java.util.List;

import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.json.JsonUtils.fromJson;
import static io.activej.json.JsonUtils.toJson;

public final class AggregationChunkJsonEtcdKVCodec implements EtcdKVCodec<Long, AggregationChunk> {
	private record Value(List<String> measures, int count, PrimaryKey min, PrimaryKey max) {}

	private final JsonCodec<Value> valueJsonCodec;

	public AggregationChunkJsonEtcdKVCodec(JsonCodec<PrimaryKey> primaryKeyCodec) {
		this.valueJsonCodec = JsonCodecs.ofObject(Value::new,
			"measures", Value::measures, JsonCodecs.ofList(JsonCodecs.ofString()),
			"count", Value::count, JsonCodecs.ofInteger(),
			"min", Value::min, primaryKeyCodec,
			"max", Value::max, primaryKeyCodec
		);
	}

	@Override
	public KeyValue encodeKV(AggregationChunk chunk) {
		return new KeyValue(
			encodeKey((Long) chunk.getChunkId()),
			byteSequenceFrom(toJson(valueJsonCodec, new Value(chunk.getMeasures(), chunk.getCount(), chunk.getMinPrimaryKey(), chunk.getMaxPrimaryKey()))));
	}

	@Override
	public ByteSequence encodeKey(Long key) {
		return byteSequenceFrom(key.toString());
	}

	@Override
	public AggregationChunk decodeKV(KeyValue kv) throws MalformedDataException {
		long chunkId = decodeKey(kv.key());
		Value value = fromJson(valueJsonCodec, kv.value().toString());
		return AggregationChunk.create(chunkId, value.measures(), value.min(), value.max(), value.count());
	}

	@Override
	public Long decodeKey(ByteSequence byteSequence) throws MalformedDataException {
		long chunkId;
		try {
			chunkId = Long.parseLong(byteSequence.toString());
		} catch (NumberFormatException e) {
			throw new MalformedDataException(e);
		}
		return chunkId;
	}

}
