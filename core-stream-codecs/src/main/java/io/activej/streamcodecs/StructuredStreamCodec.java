package io.activej.streamcodecs;

import io.activej.common.tuple.*;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public final class StructuredStreamCodec<T> implements StreamCodec<T> {
	private final Function<Object[], T> constructor;
	private final CodecAndGetter<T, ?>[] codecAndGetters;

	@SafeVarargs
	private StructuredStreamCodec(Function<Object[], T> constructor, CodecAndGetter<T, ?>... codecAndGetters) {
		this.constructor = constructor;
		this.codecAndGetters = codecAndGetters;
	}

	public static <T> StructuredStreamCodec<T> create(TupleConstructor0<T> constructor0) {
		return new StructuredStreamCodec<>(objects -> constructor0.create());
	}

	public static <T1, T> StructuredStreamCodec<T> create(TupleConstructor1<T1, T> constructor1,
			Function<T, T1> getter1, StreamCodec<T1> codec1
	) {
		//noinspection unchecked
		return new StructuredStreamCodec<>(objects ->
				constructor1.create(
						(T1) objects[0]
				),
				new CodecAndGetter<>(codec1, getter1)
		);
	}

	public static <T1, T2, T> StructuredStreamCodec<T> create(TupleConstructor2<T1, T2, T> constructor2,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2
	) {
		//noinspection unchecked
		return new StructuredStreamCodec<>(objects ->
				constructor2.create(
						(T1) objects[0],
						(T2) objects[1]
				),
				new CodecAndGetter<>(codec1, getter1),
				new CodecAndGetter<>(codec2, getter2)
		);
	}

	public static <T1, T2, T3, T> StructuredStreamCodec<T> create(TupleConstructor3<T1, T2, T3, T> constructor3,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2,
			Function<T, T3> getter3, StreamCodec<T3> codec3
	) {
		//noinspection unchecked
		return new StructuredStreamCodec<>(objects ->
				constructor3.create(
						(T1) objects[0],
						(T2) objects[1],
						(T3) objects[2]
				),
				new CodecAndGetter<>(codec1, getter1),
				new CodecAndGetter<>(codec2, getter2),
				new CodecAndGetter<>(codec3, getter3)
		);
	}

	public static <T1, T2, T3, T4, T> StructuredStreamCodec<T> create(TupleConstructor4<T1, T2, T3, T4, T> constructor4,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2,
			Function<T, T3> getter3, StreamCodec<T3> codec3,
			Function<T, T4> getter4, StreamCodec<T4> codec4
	) {
		//noinspection unchecked
		return new StructuredStreamCodec<>(objects ->
				constructor4.create(
						(T1) objects[0],
						(T2) objects[1],
						(T3) objects[2],
						(T4) objects[3]
				),
				new CodecAndGetter<>(codec1, getter1),
				new CodecAndGetter<>(codec2, getter2),
				new CodecAndGetter<>(codec3, getter3),
				new CodecAndGetter<>(codec4, getter4)
		);
	}

	public static <T1, T2, T3, T4, T5, T> StructuredStreamCodec<T> create(TupleConstructor5<T1, T2, T3, T4, T5, T> constructor5,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2,
			Function<T, T3> getter3, StreamCodec<T3> codec3,
			Function<T, T4> getter4, StreamCodec<T4> codec4,
			Function<T, T5> getter5, StreamCodec<T5> codec5
	) {
		//noinspection unchecked
		return new StructuredStreamCodec<>(objects ->
				constructor5.create(
						(T1) objects[0],
						(T2) objects[1],
						(T3) objects[2],
						(T4) objects[3],
						(T5) objects[4]
				),
				new CodecAndGetter<>(codec1, getter1),
				new CodecAndGetter<>(codec2, getter2),
				new CodecAndGetter<>(codec3, getter3),
				new CodecAndGetter<>(codec4, getter4),
				new CodecAndGetter<>(codec5, getter5)
		);
	}

	public static <T1, T2, T3, T4, T5, T6, T> StructuredStreamCodec<T> create(TupleConstructor6<T1, T2, T3, T4, T5, T6, T> constructor6,
			Function<T, T1> getter1, StreamCodec<T1> codec1,
			Function<T, T2> getter2, StreamCodec<T2> codec2,
			Function<T, T3> getter3, StreamCodec<T3> codec3,
			Function<T, T4> getter4, StreamCodec<T4> codec4,
			Function<T, T5> getter5, StreamCodec<T5> codec5,
			Function<T, T6> getter6, StreamCodec<T6> codec6
	) {
		//noinspection unchecked
		return new StructuredStreamCodec<>(objects ->
				constructor6.create(
						(T1) objects[0],
						(T2) objects[1],
						(T3) objects[2],
						(T4) objects[3],
						(T5) objects[4],
						(T6) objects[5]
				),
				new CodecAndGetter<>(codec1, getter1),
				new CodecAndGetter<>(codec2, getter2),
				new CodecAndGetter<>(codec3, getter3),
				new CodecAndGetter<>(codec4, getter4),
				new CodecAndGetter<>(codec5, getter5),
				new CodecAndGetter<>(codec6, getter6)
		);
	}

	public static <T> StructuredStreamCodec<T> create(Function<Object[], T> constructorN,
			List<CodecAndGetter<T, ?>> codecsAndGetters
	) {
		//noinspection unchecked
		return new StructuredStreamCodec<>(constructorN, codecsAndGetters.toArray(CodecAndGetter[]::new));
	}

	@Override
	public T decode(StreamInput input) throws IOException {
		Object[] fields = new Object[codecAndGetters.length];
		for (int i = 0, codecAndGettersSize = codecAndGetters.length; i < codecAndGettersSize; i++) {
			CodecAndGetter<T, ?> codecAndGetter = codecAndGetters[i];
			Object field = codecAndGetter.codec.decode(input);
			fields[i] = field;
		}
		return constructor.apply(fields);
	}

	@Override
	public void encode(StreamOutput output, T item) throws IOException {
		for (CodecAndGetter<T, ?> codecAndGetter : codecAndGetters) {
			Object field = codecAndGetter.getter.apply(item);
			//noinspection unchecked
			((StreamCodec<Object>) codecAndGetter.codec).encode(output, field);
		}
	}

	public record CodecAndGetter<T, U>(StreamCodec<U> codec, Function<T, U> getter) {
	}
}
