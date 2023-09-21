package io.activej.etcd.codec;

import io.etcd.jetcd.ByteSequence;

public record Prefix<K>(K key, ByteSequence suffix) {}
