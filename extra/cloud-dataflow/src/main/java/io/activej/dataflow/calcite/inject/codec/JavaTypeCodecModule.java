package io.activej.dataflow.calcite.inject.codec;

import io.activej.dataflow.codec.Subtype;
import io.activej.dataflow.codec.Utils;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.serializer.stream.StreamCodec;
import io.activej.serializer.stream.StreamCodecs;
import io.activej.types.Types;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public final class JavaTypeCodecModule extends AbstractModule {
	@Provides
	@Subtype(0)
	StreamCodec<Class<?>> cls() {
		return Utils.CLASS_STREAM_CODEC;
	}

	@Provides
	@Subtype(1)
	StreamCodec<ParameterizedType> parameterizedType(StreamCodec<Type> typeStreamCodec) {
		return StreamCodec.create(Types::parameterizedType,
			ParameterizedType::getOwnerType, StreamCodecs.ofNullable(typeStreamCodec),
			ParameterizedType::getRawType, typeStreamCodec,
			ParameterizedType::getActualTypeArguments, StreamCodecs.ofArray(typeStreamCodec, Type[]::new)
		);
	}

	@Provides
	@Subtype(2)
	StreamCodec<GenericArrayType> genericArrayType(StreamCodec<Type> typeStreamCodec) {
		return StreamCodec.create(Types::genericArrayType,
			GenericArrayType::getGenericComponentType, typeStreamCodec
		);
	}
}
