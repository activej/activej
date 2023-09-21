package io.activej.etcd.codec.prefix;

import io.etcd.jetcd.ByteSequence;

public record Prefix<K>(K key, ByteSequence suffix) {}
