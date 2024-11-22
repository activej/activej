package io.activej.json;

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.collection.CollectorUtils;
import io.activej.common.collection.IteratorUtils;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public final class ObjectJsonCodec<T, A> extends AbstractMapJsonCodec<T, A, Object> {

	public interface JsonCodecProvider<T, A, V> extends JsonEncoderProvider<T, V>, JsonDecoderProvider<A, V> {
	}

	public interface JsonEncoderProvider<T, V> {
		@Nullable JsonCodec<V> encoder(String key, int index, T item, V value);

		static <T, V> JsonEncoderProvider<T, V> of(JsonCodec<V> codec) {
			return (key, index, item, value) -> codec;
		}
	}

	public interface JsonDecoderProvider<A, V> {
		@Nullable JsonCodec<V> decoder(String key, int index, A accumulator) throws JsonValidationException;

		static <A, V> JsonDecoderProvider<A, V> of(JsonCodec<V> codec) {
			return (key, index, accumulator) -> codec;
		}
	}

	private record Field<T, A, V>(int index, String key,
		Function<T, V> getter, JsonSetter<A, V> setter,
		JsonEncoderProvider<T, V> encoderFn, JsonDecoderProvider<A, V> decoderFn
	) {
	}

	private final Supplier<A> accumulatorSupplier;

	private final Field<T, A, ?>[] fields;
	private final Map<String, Field<T, A, Object>> map;
	private final JsonFunction<A, T> constructor;

	private ObjectJsonCodec(Supplier<A> accumulatorSupplier, JsonFunction<A, T> constructor,
		Field<T, A, Object>[] fields, Map<String, Field<T, A, Object>> map
	) {
		this.accumulatorSupplier = accumulatorSupplier;
		this.constructor = constructor;
		this.fields = fields;
		this.map = map;
	}

	public static <T, A> BuilderObject<T, A> builder(Supplier<A> accumulatorSupplier, JsonFunction<A, T> constructor) {
		return new BuilderObject<>(accumulatorSupplier, constructor);
	}

	public static <T> BuilderArray<T> builder(JsonConstructorN<T> constructor) {
		return new BuilderArray<>(constructor);
	}

	public static class BuilderObject<T, A> extends AbstractBuilder<BuilderObject<T, A>, ObjectJsonCodec<T, A>> {
		private final Supplier<A> accumulatorSupplier;
		private final JsonFunction<A, T> constructor;
		private final List<Field<T, A, Object>> fields = new ArrayList<>();

		public BuilderObject(Supplier<A> accumulatorSupplier, JsonFunction<A, T> constructor) {
			this.accumulatorSupplier = accumulatorSupplier;
			this.constructor = constructor;
		}

		public <V> BuilderObject<T, A> with(String key,
			Function<T, V> getter, JsonSetter<A, V> setter,
			JsonCodec<V> codec
		) {
			return with(key, getter, setter, JsonEncoderProvider.of(codec), JsonDecoderProvider.of(codec));
		}

		public <V> BuilderObject<T, A> with(String key,
			Function<T, V> getter, JsonSetter<A, V> setter,
			JsonCodecProvider<T, A, V> codecFn
		) {
			return with(key, getter, setter, codecFn, codecFn);
		}

		public <V> BuilderObject<T, A> with(String key,
			Function<T, V> getter, JsonSetter<A, V> setter,
			JsonEncoderProvider<T, V> encoderFn, JsonDecoderProvider<A, V> decoderFn
		) {
			Field<T, A, V> field = new Field<>(fields.size(), key, getter, setter, encoderFn, decoderFn);
			fields.add((Field<T, A, Object>) field);
			return this;
		}

		@Override
		protected ObjectJsonCodec<T, A> doBuild() {
			return new ObjectJsonCodec<>(
				accumulatorSupplier,
				constructor,
				fields.toArray(Field[]::new),
				fields.stream().collect(CollectorUtils.toHashMap(f -> f.key, f -> f)));
		}
	}

	public static final class BuilderArray<T> extends AbstractBuilder<BuilderArray<T>, ObjectJsonCodec<T, Object[]>> {
		private static final Object NO_DEFAULT_VALUE = new Object();
		private final List<Field<T, Object[], Object>> fields = new ArrayList<>();
		private final JsonConstructorN<T> constructor;

		private final List<Object> prototype = new ArrayList<>();

		private BuilderArray(JsonConstructorN<T> constructor) {this.constructor = constructor;}

		public <V> BuilderArray<T> with(String key, Function<T, V> getter, JsonCodec<V> codec) {
			int index = fields.size();
			Field<T, Object[], V> field = new Field<>(index, key,
				getter,
				(array, value) -> array[index] = value,
				JsonEncoderProvider.of(codec),
				JsonDecoderProvider.of(codec));
			fields.add((Field<T, Object[], Object>) field);
			prototype.add(NO_DEFAULT_VALUE);
			return this;
		}

		public <V> BuilderArray<T> with(String key, Function<T, V> getter, JsonCodec<V> codec, V defaultValue) {
			int index = fields.size();
			Field<T, Object[], V> field = new Field<>(index, key,
				getter,
				(array, value) -> array[index] = value,
				(key_, index_, item, value) -> Objects.equals(defaultValue, value) ? null : codec,
				JsonDecoderProvider.of(codec));
			fields.add((Field<T, Object[], Object>) field);
			prototype.add(defaultValue);
			return this;
		}

		@Override
		protected ObjectJsonCodec<T, Object[]> doBuild() {
			Object[] prototype = this.prototype.toArray(Object[]::new);
			return Arrays.stream(prototype).anyMatch(v -> v == NO_DEFAULT_VALUE) ?
				new ObjectJsonCodec<>(
					() -> Arrays.copyOf(prototype, prototype.length),
					array -> {
						//noinspection ForLoopReplaceableByForEach
						for (int i = 0; i < array.length; i++) {
							if (array[i] == NO_DEFAULT_VALUE) {
								throw new JsonValidationException();
							}
						}
						return constructor.create(array);
					},
					fields.toArray(Field[]::new),
					fields.stream().collect(CollectorUtils.toHashMap(f -> f.key, f -> f))) :
				new ObjectJsonCodec<>(
					() -> new Object[prototype.length],
					constructor::create,
					fields.toArray(Field[]::new),
					fields.stream().collect(CollectorUtils.toHashMap(f -> f.key, f -> f)));
		}
	}

	@Override
	protected Iterator<JsonMapEntry<Object>> iterate(T item) {
		return IteratorUtils.transformIterator(IteratorUtils.iteratorOf(fields), field -> new JsonMapEntry<>(field.key, field.getter.apply(item)));
	}

	@Override
	protected @Nullable JsonEncoder<Object> encoder(String key, int index, T item, Object value) {
		return ((JsonEncoderProvider<T, Object>) fields[index].encoderFn).encoder(key, index, item, value);
	}

	@Override
	protected @Nullable JsonDecoder<Object> decoder(String key, int index, A accumulator) throws JsonValidationException {
		Field<T, A, Object> field = map.get(key);
		if (field == null) throw new JsonValidationException("Key not found: " + key);
		return field.decoderFn.decoder(key, index, accumulator);
	}

	@Override
	protected A accumulator() {
		return accumulatorSupplier.get();
	}

	@Override
	protected void accumulate(A accumulator, String key, int index, Object value) throws JsonValidationException {
		map.get(key).setter.set(accumulator, value);
	}

	@Override
	protected T result(A accumulator, int count) throws JsonValidationException {
		return constructor.apply(accumulator);
	}

}
