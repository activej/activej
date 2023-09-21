package io.activej.etcd.codec;

import io.activej.common.exception.MalformedDataException;
import io.activej.common.function.DecoderFunction;
import io.etcd.jetcd.ByteSequence;

import java.util.function.Function;

import static io.activej.etcd.EtcdUtils.byteSequenceFrom;

public class EtcdPrefixCodecs {

	public static EtcdPrefixCodec<String> ofTerminatingString(char terminator) {
		return new EtcdPrefixCodec<>() {
			final ByteSequence terminatorByte = byteSequenceFrom(terminator);

			@Override
			public ByteSequence encodePrefix(String key, ByteSequence suffix) {
				return byteSequenceFrom(key).concat(terminatorByte).concat(suffix);
			}

			@Override
			public Prefix<String> decodePrefix(ByteSequence byteSequence) throws MalformedDataException {
				byte[] bytes = byteSequence.getBytes();
				int i;
				for (i = 0; i < bytes.length; i++) {
					if (bytes[i] == terminator) break;
				}
				if (i >= bytes.length) throw new MalformedDataException();
				return new Prefix<>(byteSequence.substring(0, i).toString(), byteSequence.substring(i + 1));
			}
		};
	}

	public static <T, R> EtcdPrefixCodec<R> transform(EtcdPrefixCodec<T> codec, Function<R, T> encodeFn, DecoderFunction<T, R> decodeFn) {
		return new EtcdPrefixCodec<>() {
			@Override
			public ByteSequence encodePrefix(R key, ByteSequence suffix) {
				return codec.encodePrefix(encodeFn.apply(key), suffix);
			}

			@Override
			public Prefix<R> decodePrefix(ByteSequence byteSequence) throws MalformedDataException {
				Prefix<T> prefix = codec.decodePrefix(byteSequence);
				return new Prefix<>(decodeFn.decode(prefix.key()), prefix.suffix());
			}
		};
	}
}
