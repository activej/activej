package io.activej.crdt.json;

import io.activej.common.StringFormatUtils;
import io.activej.common.exception.MalformedDataException;
import io.activej.crdt.CrdtData;
import io.activej.crdt.CrdtEntity;
import io.activej.crdt.storage.cluster.PartitionId;
import io.activej.crdt.storage.cluster.RendezvousPartitionGroup;
import io.activej.json.JsonCodec;
import io.activej.json.JsonValidationException;

import java.net.InetSocketAddress;

import static io.activej.json.JsonCodecs.*;

public class JsonCodecs {
	public static <K extends Comparable<K>, S> JsonCodec<CrdtData<K, S>> ofCrdtData(JsonCodec<K> keyCodec, JsonCodec<S> stateCodec) {
		return ofObject(CrdtData::new,
			"key", CrdtEntity::getKey, keyCodec,
			"timestamp", CrdtData::getTimestamp, ofLong(),
			"state", CrdtData::getState, stateCodec);
	}

	public static JsonCodec<InetSocketAddress> ofInetSocketAddress() {
		return transform(ofString(),
			value -> value.getAddress().getHostAddress() + ":" + value.getPort(),
			string -> {
				try {
					return StringFormatUtils.parseInetSocketAddressResolving(string);
				} catch (MalformedDataException e) {
					throw new JsonValidationException(e.getMessage());
				}
			});
	}

	public static JsonCodec<PartitionId> ofPartitionId() {
		return ofObject(PartitionId::of,
			"id", PartitionId::getId, ofString(),
			"crdtAddress", PartitionId::getCrdtAddress, ofInetSocketAddress(),
			"rpcAddress", PartitionId::getRpcAddress, ofInetSocketAddress()
		);
	}

	public static <P> JsonCodec<RendezvousPartitionGroup<P>> ofRendezvousPartitionGroup(JsonCodec<P> idCodec) {
		return ofObject(RendezvousPartitionGroup::create,
			"ids", RendezvousPartitionGroup::getPartitionIds, ofSet(idCodec),
			"replicaCount", RendezvousPartitionGroup::getReplicaCount, ofInteger(),
			"repartition", RendezvousPartitionGroup::isRepartition, ofBoolean(),
			"active", RendezvousPartitionGroup::isActive, ofBoolean());
	}
}
