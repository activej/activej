package io.activej.etcd.codec;

import io.activej.common.exception.MalformedDataException;

public interface EtcdKVDecoder<K, KV> extends EtcdKeyDecoder<K> {
	KV decodeKV(KeyValue kv) throws MalformedDataException;
}
