package io.activej.serializer.stream;

import io.activej.common.tuple.*;
import io.activej.serializer.BinarySerializer;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public interface StreamCodec<T> extends StreamEncoder<T>, StreamDecoder<T> {

	static <T> StreamCodec<T> of(StreamEncoder<? super T> encoder, StreamDecoder<? extends T> decoder) {
		return new StreamCodec<>() {
			@Override
			public void encode(StreamOutput output, T item) throws IOException {
				encoder.encode(output, item);
			}

			@Override
			public T decode(StreamInput input) throws IOException {
				return decoder.decode(input);
			}
		};
	}

	static <T> StreamCodec<T> ofBinarySerializer(BinarySerializer<T> binarySerializer) {
		return ofBinarySerializer(binarySerializer, 1);
	}

	static <T> StreamCodec<T> ofBinarySerializer(BinarySerializer<T> binarySerializer, int estimatedSize) {
		return new StreamCodecs.OfBinarySerializer<>(binarySerializer, estimatedSize);
	}

	static <T> StreamCodec<T> create(TupleConstructor0<T> constructor0) {
		return StreamCodec.of((output, item) -> {}, $ -> constructor0.create());
	}

	static <T1, T> StreamCodec<T> create(TupleConstructor1<T1, T> constructor1,
			Function<T, T1> getter1, StreamCodec<T1> codec1
	) {
		return StreamCodec.of(
				(output, item) -> codec1.encode(output, getter1.apply(item)),
				input -> constructor1.create(codec1.decode(input))
		);
	}

	static <T1, T2, T> StreamCodec<T> create(TupleConstructor2<T1, T2, T> constructor2,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2
	) {
		return StreamCodec.of(
				(output, item) -> {
					codec1.encode(output, getter1.apply(item));
					codec2.encode(output, getter2.apply(item));
				},
				input -> constructor2.create(
						codec1.decode(input),
						codec2.decode(input)
				)
		);
	}

	static <T1, T2, T3, T> StreamCodec<T> create(TupleConstructor3<T1, T2, T3, T> constructor3,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2,
			Function<T, T3> getter3, StreamCodec<T3> codec3
	) {
		return StreamCodec.of(
				(output, item) -> {
					codec1.encode(output, getter1.apply(item));
					codec2.encode(output, getter2.apply(item));
					codec3.encode(output, getter3.apply(item));
				},
				input -> constructor3.create(
						codec1.decode(input),
						codec2.decode(input),
						codec3.decode(input)
				)
		);
	}

	static <T1, T2, T3, T4, T> StreamCodec<T> create(TupleConstructor4<T1, T2, T3, T4, T> constructor4,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2,
			Function<T, T3> getter3, StreamCodec<T3> codec3,
			Function<T, T4> getter4, StreamCodec<T4> codec4
	) {
		return StreamCodec.of(
				(output, item) -> {
					codec1.encode(output, getter1.apply(item));
					codec2.encode(output, getter2.apply(item));
					codec3.encode(output, getter3.apply(item));
					codec4.encode(output, getter4.apply(item));
				},
				input -> constructor4.create(
						codec1.decode(input),
						codec2.decode(input),
						codec3.decode(input),
						codec4.decode(input)
				)
		);
	}

	static <T1, T2, T3, T4, T5, T> StreamCodec<T> create(TupleConstructor5<T1, T2, T3, T4, T5, T> constructor5,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2,
			Function<T, T3> getter3, StreamCodec<T3> codec3,
			Function<T, T4> getter4, StreamCodec<T4> codec4,
			Function<T, T5> getter5, StreamCodec<T5> codec5
	) {
		return StreamCodec.of(
				(output, item) -> {
					codec1.encode(output, getter1.apply(item));
					codec2.encode(output, getter2.apply(item));
					codec3.encode(output, getter3.apply(item));
					codec4.encode(output, getter4.apply(item));
					codec5.encode(output, getter5.apply(item));
				},
				input -> constructor5.create(
						codec1.decode(input),
						codec2.decode(input),
						codec3.decode(input),
						codec4.decode(input),
						codec5.decode(input)
				)
		);
	}

	static <T1, T2, T3, T4, T5, T6, T> StreamCodec<T> create(TupleConstructor6<T1, T2, T3, T4, T5, T6, T> constructor6,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2,
			Function<T, T3> getter3, StreamCodec<T3> codec3,
			Function<T, T4> getter4, StreamCodec<T4> codec4,
			Function<T, T5> getter5, StreamCodec<T5> codec5,
			Function<T, T6> getter6, StreamCodec<T6> codec6
	) {
		return StreamCodec.of(
				(output, item) -> {
					codec1.encode(output, getter1.apply(item));
					codec2.encode(output, getter2.apply(item));
					codec3.encode(output, getter3.apply(item));
					codec4.encode(output, getter4.apply(item));
					codec5.encode(output, getter5.apply(item));
					codec6.encode(output, getter6.apply(item));
				},
				input -> constructor6.create(
						codec1.decode(input),
						codec2.decode(input),
						codec3.decode(input),
						codec4.decode(input),
						codec5.decode(input),
						codec6.decode(input)
				)
		);
	}

	static <T> StreamCodec<T> create(TupleConstructorN<T> constructorN,
			List<CodecAndGetter<T, ?>> codecsAndGetters
	) {
		//noinspection unchecked
		CodecAndGetter<T, Object>[] codecsAndGettersArray = codecsAndGetters.toArray(CodecAndGetter[]::new);
		return StreamCodec.of(
				(output, item) -> {
					for (CodecAndGetter<T, Object> codecsAndGetter : codecsAndGettersArray) {
						codecsAndGetter.codec.encode(output, codecsAndGetter.getter.apply(item));
					}
				},
				input -> {
					Object[] args = new Object[codecsAndGettersArray.length];
					for (int i = 0; i < codecsAndGettersArray.length; i++) {
						args[i] = codecsAndGettersArray[i].codec.decode(input);
					}
					return constructorN.create(args);
				}
		);
	}

	record CodecAndGetter<T, U>(StreamCodec<U> codec, Function<T, U> getter) {
	}
}
