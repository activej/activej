package io.activej.etcd.codec.kv;

import io.activej.etcd.codec.key.EtcdKeyDecoder;
import io.activej.etcd.exception.MalformedEtcdDataException;

public interface EtcdKVDecoder<K, KV> extends EtcdKeyDecoder<K> {
	KV decodeKV(KeyValue kv) throws MalformedEtcdDataException;
}
