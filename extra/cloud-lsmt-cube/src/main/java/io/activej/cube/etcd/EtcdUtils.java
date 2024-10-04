package io.activej.cube.etcd;

import io.activej.cube.aggregation.AggregationChunk;
import io.activej.cube.aggregation.ot.AggregationDiff;
import io.activej.cube.ot.CubeDiff;
import io.activej.etcd.TxnOps;
import io.activej.etcd.codec.key.EtcdKeyCodec;
import io.activej.etcd.codec.key.EtcdKeyCodecs;
import io.activej.etcd.codec.kv.EtcdKVCodec;
import io.activej.etcd.codec.kv.EtcdKVCodecs;
import io.activej.etcd.codec.prefix.EtcdPrefixCodec;
import io.activej.etcd.codec.prefix.EtcdPrefixCodecs;
import io.activej.etcd.codec.prefix.Prefix;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.activej.etl.LogDiff;
import io.activej.etl.LogPositionDiff;
import io.etcd.jetcd.ByteSequence;

import java.util.Map;
import java.util.function.Function;

import static io.activej.common.Utils.entriesToLinkedHashMap;
import static io.activej.cube.etcd.CubeEtcdOTUplink.logPositionEtcdCodec;
import static io.activej.etcd.EtcdUtils.*;

public final class EtcdUtils {
	public static final ByteSequence POS = byteSequenceFrom("pos.");
	public static final ByteSequence CHUNK = byteSequenceFrom("chunk.");
	public static final ByteSequence TIMESTAMP = byteSequenceFrom("timestamp");
	public static final EtcdPrefixCodec<String> AGGREGATION_ID_CODEC = EtcdPrefixCodecs.ofTerminatingString('.');

	static void saveCubeLogDiff(ByteSequence prefixPos, ByteSequence prefixChunk, EtcdPrefixCodec<String> aggregationIdCodec, Function<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory, TxnOps txn, LogDiff<CubeDiff> logDiff) {
		savePositions(txn.child(prefixPos), logDiff.getPositions());
		for (CubeDiff diff : logDiff.getDiffs()) {
			saveCubeDiff(aggregationIdCodec, chunkCodecsFactory, txn.child(prefixChunk), diff);
		}
	}

	private static void savePositions(TxnOps txn, Map<String, LogPositionDiff> positions) {
		checkAndInsert(txn,
			EtcdKVCodecs.ofMapEntry(EtcdKeyCodecs.ofString(), logPositionEtcdCodec()),
			positions.entrySet().stream().filter(diff -> diff.getValue().from().isInitial()).collect(entriesToLinkedHashMap(LogPositionDiff::to)));
		checkAndUpdate(txn,
			EtcdKVCodecs.ofMapEntry(EtcdKeyCodecs.ofString(), logPositionEtcdCodec()),
			positions.entrySet().stream().filter(diff -> !diff.getValue().from().isInitial()).collect(entriesToLinkedHashMap(LogPositionDiff::from)),
			positions.entrySet().stream().filter(diff -> !diff.getValue().from().isInitial()).collect(entriesToLinkedHashMap(LogPositionDiff::to)));
	}

	private static void saveCubeDiff(
		EtcdPrefixCodec<String> aggregationIdCodec,
		Function<String, EtcdKVCodec<Long, AggregationChunk>> chunkCodecsFactory, TxnOps txn,
		CubeDiff cubeDiff
	) {
		for (var entry : cubeDiff.getDiffs().entrySet().stream().collect(entriesToLinkedHashMap(AggregationDiff::getRemovedChunks)).entrySet()) {
			String aggregationId = entry.getKey();
			checkAndDelete(
				txn.child(aggregationIdCodec.encodePrefix(new Prefix<>(aggregationId, ByteSequence.EMPTY))),
				chunkCodecsFactory.apply(aggregationId),
				entry.getValue().stream().map(AggregationChunk::getChunkId).toList());
		}
		for (var entry : cubeDiff.getDiffs().entrySet().stream().collect(entriesToLinkedHashMap(AggregationDiff::getAddedChunks)).entrySet()) {
			String aggregationId = entry.getKey();
			checkAndInsert(
				txn.child(aggregationIdCodec.encodePrefix(new Prefix<>(aggregationId, ByteSequence.EMPTY))),
				chunkCodecsFactory.apply(aggregationId),
				entry.getValue());
		}
	}

	static final EtcdKeyCodec<Long> CHUNK_ID_CODEC = new EtcdKeyCodec<>() {
		@Override
		public Long decodeKey(ByteSequence byteSequence) throws MalformedEtcdDataException {
			try {
				return Long.parseLong(byteSequence.toString());
			} catch (NumberFormatException e) {
				throw new MalformedEtcdDataException(e.getMessage());
			}
		}

		@Override
		public ByteSequence encodeKey(Long key) {
			return byteSequenceFrom(key.toString());
		}
	};

}
