package hppc;

import io.activej.codegen.expression.Expression;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.def.SerializerDef;

import static io.activej.codegen.expression.Expressions.constructor;
import static io.activej.serializer.util.Utils.initialCapacity;

public final class Hppc9HashSetSerializerDef extends Hppc9RegularCollectionSerializerDef {
	public Hppc9HashSetSerializerDef(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType) {
		this(valueSerializer, collectionType, collectionImplType, valueType, false);
	}

	private Hppc9HashSetSerializerDef(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> elementType, boolean nullable) {
		super(valueSerializer, collectionType, collectionImplType, elementType, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new Hppc9HashSetSerializerDef(valueSerializer, encodeType, decodeType, elementType, true);
	}

	@Override
	protected Expression createBuilder(Expression length) {
		return constructor(decodeType, initialCapacity(length));
	}
}
