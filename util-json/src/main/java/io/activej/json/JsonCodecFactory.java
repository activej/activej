package io.activej.json;

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.builder.Rebuildable;
import io.activej.json.annotations.JsonNullable;
import io.activej.json.annotations.JsonSubclasses;
import io.activej.types.TypeT;
import io.activej.types.scanner.TypeScannerRegistry;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static io.activej.common.Checks.checkArgument;
import static io.activej.types.Utils.getAnnotation;
import static io.activej.types.Utils.hasAnnotation;

public class JsonCodecFactory implements Rebuildable<JsonCodecFactory, JsonCodecFactory.Builder> {
	private static final JsonCodecFactory DEFAULT_INSTANCE = JsonCodecFactory.builder().build();

	private final TypeScannerRegistry<JsonCodec<?>> registry;

	private JsonCodecFactory(TypeScannerRegistry<JsonCodec<?>> registry) {this.registry = registry;}

	public static JsonCodecFactory defaultInstance() {
		return DEFAULT_INSTANCE;
	}

	public static Builder builder() {
		JsonCodecFactory factory = new JsonCodecFactory(TypeScannerRegistry.create());
		//noinspection unchecked,rawtypes
		return factory.new Builder()
			.with(String.class, ctx -> JsonCodecs.ofString())

			.with(byte.class, ctx -> JsonCodecs.ofByte())
			.with(short.class, ctx -> JsonCodecs.ofShort())
			.with(int.class, ctx -> JsonCodecs.ofInteger())
			.with(long.class, ctx -> JsonCodecs.ofLong())
			.with(float.class, ctx -> JsonCodecs.ofFloat())
			.with(double.class, ctx -> JsonCodecs.ofDouble())
			.with(boolean.class, ctx -> JsonCodecs.ofBoolean())
			.with(char.class, ctx -> JsonCodecs.ofCharacter())

			.with(Byte.class, ctx -> JsonCodecs.ofByte())
			.with(Short.class, ctx -> JsonCodecs.ofShort())
			.with(Integer.class, ctx -> JsonCodecs.ofInteger())
			.with(Long.class, ctx -> JsonCodecs.ofLong())
			.with(Float.class, ctx -> JsonCodecs.ofFloat())
			.with(Double.class, ctx -> JsonCodecs.ofDouble())
			.with(Boolean.class, ctx -> JsonCodecs.ofBoolean())
			.with(Character.class, ctx -> JsonCodecs.ofCharacter())

			.with(LocalDate.class, ctx -> JsonCodecs.ofLocalDate())

			.with(Enum.class, ctx -> JsonCodecs.ofEnum((Class<Enum>) ctx.getRawType()))

			.with(List.class, ctx -> JsonCodecs.ofList(ctx.scanTypeArgument(0)))
			.with(Map.class, ctx -> {
				AnnotatedType keyAnnotatedType = ctx.getTypeArgument(0);
				Class<?> keyType = (Class<?>) keyAnnotatedType.getType();
				if (keyAnnotatedType.getAnnotations().length == 0 && keyType == String.class) {
					//noinspection unchecked
					return JsonCodecs.ofMap((JsonCodec<Object>) ctx.scanTypeArgument(1));
				}
				if (Number.class.isAssignableFrom(keyType)) {
					//noinspection unchecked
					JsonKeyCodec<?> keyCodec = JsonKeyCodec.ofNumberKey((Class<Number>) keyType);
					return JsonCodecs.ofMap(keyCodec, ctx.scanTypeArgument(1));
				}
				throw new IllegalArgumentException("TODO");
			})

			.with(Object.class, ctx -> {
				throw new UnsupportedOperationException();
			})
			;
	}

	public final class Builder extends AbstractBuilder<Builder, JsonCodecFactory> {
		private Builder() {}

		public Builder with(TypeT<?> typeT, TypeScannerRegistry.Mapping<JsonCodec<?>> fn) {
			checkNotBuilt(this);
			return with(typeT.getType(), fn);
		}

		public Builder with(Type type, TypeScannerRegistry.Mapping<JsonCodec<?>> fn) {
			checkNotBuilt(this);
			registry.with(type, ctx -> {
				JsonCodec<?> jsonCodec;

				JsonSubclasses annotation;
				if ((annotation = getAnnotation(ctx.getAnnotations(), JsonSubclasses.class)) != null) {
					SubclassJsonCodec<Object>.Builder subclassBuilder = SubclassJsonCodec.builder();
					Class<?>[] subclasses = annotation.value();
					String[] tags = annotation.tags();
					checkArgument(tags.length == 0 || tags.length == subclasses.length);
					for (int i = 0; i < subclasses.length; i++) {
						//noinspection unchecked
						Class<Object> subclass = (Class<Object>) subclasses[i];
						//noinspection unchecked
						JsonCodec<Object> codec = (JsonCodec<Object>) ctx.scan(subclass);
						if (tags.length != 0) {
							subclassBuilder.with(subclass, tags[i], codec);
						} else {
							subclassBuilder.with(subclass, codec);
						}
					}
					jsonCodec = subclassBuilder.build();
				} else {
					jsonCodec = fn.apply(ctx);
				}

				if (hasAnnotation(ctx.getAnnotations(), JsonNullable.class)) {
					if (!(jsonCodec instanceof JsonCodecs.NullableJsonCodec)) {
						jsonCodec = jsonCodec.nullable();
					}
				}

				return jsonCodec;
			});
			return this;
		}

		@Override
		protected JsonCodecFactory doBuild() {
			return JsonCodecFactory.this;
		}
	}

	@Override
	public Builder rebuild() {
		return new JsonCodecFactory(TypeScannerRegistry.copyOf(this.registry)).new Builder();
	}

	public <T> JsonCodec<T> resolve(Class<T> type) {
		//noinspection unchecked
		return (JsonCodec<T>) registry.scanner().scan(type);
	}

	public <T> JsonCodec<T> resolve(Type type) {
		//noinspection unchecked
		return (JsonCodec<T>) registry.scanner().scan(type);
	}

	public <T> JsonCodec<T> resolve(TypeT<T> type) {
		//noinspection unchecked
		return (JsonCodec<T>) registry.scanner().scan(type.getAnnotatedType());
	}

}
