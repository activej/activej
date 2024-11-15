package hppc;

import io.activej.codegen.expression.Expression;
import io.activej.common.exception.UncheckedException;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.def.SerializerDef;
import io.activej.serializer.def.impl.RegularMapSerializerDef;

import java.util.function.BinaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static hppc.SerializerFactoryUtils.capitalize;
import static io.activej.serializer.util.Utils.initialCapacity;

public final class Hppc9HashMapSerializerDef extends RegularMapSerializerDef {
	public Hppc9HashMapSerializerDef(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType) {
		this(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, false);
	}

	private Hppc9HashMapSerializerDef(SerializerDef keySerializer, SerializerDef valueSerializer, Class<?> mapType, Class<?> mapImplType, Class<?> keyType, Class<?> valueType, boolean nullable) {
		super(keySerializer, valueSerializer, mapType, mapImplType, keyType, valueType, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new Hppc9HashMapSerializerDef(keySerializer, valueSerializer, encodeType, decodeType, keyType, valueType, true);
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
		return constructor(decodeType, initialCapacity(length));
	}
}
