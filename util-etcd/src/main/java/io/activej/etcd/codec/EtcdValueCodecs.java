package io.activej.etcd.codec;

import io.activej.common.exception.MalformedDataException;
import io.activej.common.function.DecoderFunction;
import io.etcd.jetcd.ByteSequence;

import java.util.function.Function;

import static io.activej.etcd.EtcdUtils.byteSequenceFrom;

public class EtcdValueCodecs {

	public static EtcdValueCodec<String> ofString() {
		return new EtcdValueCodec<>() {
			@Override
			public ByteSequence encodeValue(String key) {
				return byteSequenceFrom(key);
			}

			@Override
			public String decodeValue(ByteSequence byteSequence) throws MalformedDataException {
				return byteSequence.toString();
			}
		};
	}

	public static <T, R> EtcdValueCodec<R> transform(EtcdValueCodec<T> codec, Function<R, T> encodeFn, DecoderFunction<T, R> decodeFn) {
		return new EtcdValueCodec<>() {
			@Override
			public R decodeValue(ByteSequence byteSequence) throws MalformedDataException {
				return decodeFn.decode(codec.decodeValue(byteSequence));
			}

			@Override
			public ByteSequence encodeValue(R item) {
				return codec.encodeValue(encodeFn.apply(item));
			}
		};
	}

}
