package io.activej.etcd.codec;

import io.etcd.jetcd.ByteSequence;

public record KeyValue(ByteSequence key, ByteSequence value) {
}
