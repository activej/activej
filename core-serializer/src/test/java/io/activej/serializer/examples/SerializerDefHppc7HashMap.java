package io.activej.serializer.examples;

import io.activej.codegen.expression.Expression;
import io.activej.common.exception.UncheckedException;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.SerializerDef;
import io.activej.serializer.impl.SerializerDefRegularMap;
import org.jetbrains.annotations.NotNull;

import java.util.function.BinaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static io.activej.serializer.examples.SerializerBuilderUtils.capitalize;
import static io.activej.serializer.util.Utils.hashInitialSize;

public final class SerializerDefHppc7HashMap extends SerializerDefRegularMap {
	public SerializerDefHppc7HashMap(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		this(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, false);
	}

	private SerializerDefHppc7HashMap(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		super(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, nullable);
	}

	@Override
	protected @NotNull SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new SerializerDefHppc7HashMap(keySerializer, valueSerializer, encodeType, decodeType, keyType, valueType, true);
	}

	@Override
	protected @NotNull Expression doIterateMap(Expression collection, BinaryOperator<Expression> action) {
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
	protected @NotNull Expression createBuilder(Expression length) {
		return constructor(decodeType, hashInitialSize(length));
	}
}
