package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.common.exception.UncheckedException;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.AbstractSerializerDefMap;
import org.jetbrains.annotations.NotNull;

import java.util.function.BinaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.examples.SerializerBuilderUtils.capitalize;

public final class SerializerDefHppc7Map extends AbstractSerializerDefMap {
	public SerializerDefHppc7Map(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		this(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, false);
	}

	private SerializerDefHppc7Map(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		super(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefHppc7Map(keySerializer, valueSerializer, encodeType, decodeType, keyType, valueType, true);
	}

	@Override
	protected Expression doIterateMap(Expression collection, BinaryOperator<Expression> action) {
		try {
			String prefix = capitalize(keyType.getSimpleName()) + capitalize(valueType.getSimpleName());
			Class<?> iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			return iterateIterable(collection,
					it -> action.apply(
							property(cast(it, iteratorType), "key"),
							property(cast(it, iteratorType), "value")));
		} catch (ClassNotFoundException e) {
			throw UncheckedException.of(e);
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
}
