package io.activej.etcd.codec.prefix;

import io.activej.common.exception.MalformedDataException;
import io.activej.common.function.DecoderFunction;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.etcd.jetcd.ByteSequence;

import java.util.function.Function;

import static io.activej.etcd.EtcdUtils.byteSequenceFrom;

public class EtcdPrefixCodecs {

	public static EtcdPrefixCodec<String> ofTerminatingString(char terminator) {
		return new EtcdPrefixCodec<>() {
			final ByteSequence terminatorByte = byteSequenceFrom(terminator);

			@Override
			public ByteSequence encodePrefix(Prefix<String> prefix) {
				return byteSequenceFrom(prefix.key()).concat(terminatorByte).concat(prefix.suffix());
			}

			@Override
			public Prefix<String> decodePrefix(ByteSequence byteSequence) throws MalformedEtcdDataException {
				byte[] bytes = byteSequence.getBytes();
				int i;
				for (i = 0; i < bytes.length; i++) {
					if (bytes[i] == terminator) break;
				}
				if (i >= bytes.length) throw new MalformedEtcdDataException("No terminator '" + terminator + "' found in key '" + byteSequence + '\'');
				return new Prefix<>(byteSequence.substring(0, i).toString(), byteSequence.substring(i + 1));
			}
		};
	}

	public static <T, R> EtcdPrefixCodec<R> transform(EtcdPrefixCodec<T> codec, Function<R, T> encodeFn, DecoderFunction<T, R> decodeFn) {
		return new EtcdPrefixCodec<>() {
			@Override
			public ByteSequence encodePrefix(Prefix<R> prefix) {
				Prefix<T> transformedPrefix = new Prefix<>(encodeFn.apply(prefix.key()), prefix.suffix());
				return codec.encodePrefix(transformedPrefix);
			}

			@Override
			public Prefix<R> decodePrefix(ByteSequence byteSequence) throws MalformedEtcdDataException {
				Prefix<T> prefix = codec.decodePrefix(byteSequence);
				try {
					return new Prefix<>(decodeFn.decode(prefix.key()), prefix.suffix());
				} catch (MalformedDataException e) {
					throw new MalformedEtcdDataException("Failed to decode prefix '" + byteSequence + '\'', e);
				}
			}
		};
	}
}
