package io.activej.etcd.codec.kv;

import io.etcd.jetcd.ByteSequence;

public record KeyValue(ByteSequence key, ByteSequence value) {
}
