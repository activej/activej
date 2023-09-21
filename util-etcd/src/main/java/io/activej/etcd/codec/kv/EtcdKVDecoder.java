package io.activej.etcd.codec.kv;

import io.activej.common.exception.MalformedDataException;
import io.activej.etcd.codec.key.EtcdKeyDecoder;

public interface EtcdKVDecoder<K, KV> extends EtcdKeyDecoder<K> {
	KV decodeKV(KeyValue kv) throws MalformedDataException;
}
