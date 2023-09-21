package io.activej.etcd.codec.value;

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
			public String decodeValue(ByteSequence byteSequence) {
				return byteSequence.toString();
			}
		};
	}

	public static EtcdValueCodec<Integer> ofIntegerString() {
		return new EtcdValueCodec<>() {
			@Override
			public ByteSequence encodeValue(Integer value) {
				return byteSequenceFrom(Integer.toString(value));
			}

			@Override
			public Integer decodeValue(ByteSequence byteSequence) throws MalformedDataException {
				try {
					return Integer.parseInt(byteSequence.toString());
				} catch (NumberFormatException e) {
					throw new MalformedDataException(e);
				}
			}
		};
	}

	public static EtcdValueCodec<Long> ofLongString() {
		return new EtcdValueCodec<>() {
			@Override
			public ByteSequence encodeValue(Long value) {
				return byteSequenceFrom(Long.toString(value));
			}

			@Override
			public Long decodeValue(ByteSequence byteSequence) throws MalformedDataException {
				try {
					return Long.parseLong(byteSequence.toString());
				} catch (NumberFormatException e) {
					throw new MalformedDataException(e);
				}
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
