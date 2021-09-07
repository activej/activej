package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.codegen.expression.Variable;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.AbstractSerializerDefMap;
import io.activej.serializer.impl.SerializerDefNullable;

import java.util.function.Function;

import static io.activej.serializer.examples.SerializerBuilderUtils.capitalize;

public final class SerializerDefHppc7Map extends AbstractSerializerDefMap {
	public SerializerDefHppc7Map(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		this(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, false);
	}

	private SerializerDefHppc7Map(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		super(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, nullable);
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return super.encoder(staticEncoders, buf, pos, value, version, compatibilityLevel);
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return super.decoder(staticDecoders, in, version, compatibilityLevel);
	}

	@Override
	protected Expression mapForEach(Expression collection, Function<Expression, Expression> forEachKey, Function<Expression, Expression> forEachValue) {
		try {
			String prefix = capitalize(keyType.getSimpleName()) + capitalize(valueType.getSimpleName());
			Class<?> iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			return new ForEachHppcMap(collection, forEachValue, forEachKey, iteratorType);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("There is no hppc cursor for " + keyType.getSimpleName(), e);
		}
	}

	@Override
	public SerializerDef ensureNullable(CompatibilityLevel compatibilityLevel) {
		if (compatibilityLevel.compareTo(CompatibilityLevel.LEVEL_3) < 0) {
			return new SerializerDefNullable(this);
		}
		return new SerializerDefHppc7Map(keySerializer, valueSerializer, encodeType, decodeType, keyType, valueType, true);
	}
}
