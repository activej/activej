package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.AbstractSerializerDefMap;
import org.jetbrains.annotations.NotNull;

import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.call;
import static io.activej.codegen.expression.Expressions.constructor;
import static io.activej.serializer.examples.SerializerBuilderUtils.capitalize;

public final class SerializerDefHppc7Map extends AbstractSerializerDefMap {
	public SerializerDefHppc7Map(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		this(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, false);
	}

	private SerializerDefHppc7Map(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		super(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, nullable);
	}

	@Override
	protected Expression mapForEach(Expression collection, UnaryOperator<Expression> forEachKey, UnaryOperator<Expression> forEachValue, Expression length) {
		try {
			String prefix = capitalize(keyType.getSimpleName()) + capitalize(valueType.getSimpleName());
			Class<?> iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			return new ForEachHppcMap(collection, forEachValue, forEachKey, iteratorType);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("There is no hppc cursor for " + keyType.getSimpleName(), e);
		}
	}

    @Override
    protected Expression createBuilder(Expression length) {
		return constructor(decodeType, initialSize(length));
	}

	@Override
	protected @NotNull Expression putToBuilder(Expression builder, Expression index, Expression key, Expression value) {
		return call(builder, "put", key, value);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefHppc7Map(keySerializer, valueSerializer, encodeType, decodeType, keyType, valueType, true);
	}
}
