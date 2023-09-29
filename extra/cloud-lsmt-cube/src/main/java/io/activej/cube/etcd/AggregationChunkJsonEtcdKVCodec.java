package io.activej.cube.etcd;

import io.activej.common.exception.MalformedDataException;
import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.PrimaryKey;
import io.activej.etcd.codec.kv.EtcdKVCodec;
import io.activej.etcd.codec.kv.KeyValue;
import io.activej.etcd.exception.MalformedEtcdDataException;
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
	public AggregationChunk decodeKV(KeyValue kv) throws MalformedEtcdDataException {
		long chunkId = decodeKey(kv.key());
		Value value;
		try {
			value = fromJson(valueJsonCodec, kv.value().toString());
		} catch (MalformedDataException e) {
			throw new MalformedEtcdDataException(e.getMessage());
		}
		return AggregationChunk.create(chunkId, value.measures(), value.min(), value.max(), value.count());
	}

	@Override
	public Long decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException {
		long chunkId;
		try {
			chunkId = Long.parseLong(byteSequence.toString());
		} catch (NumberFormatException e) {
			throw new MalformedEtcdDataException(e.getMessage());
		}
		return chunkId;
	}

}
