package hppc;

import io.activej.codegen.expression.Expression;
import io.activej.common.exception.UncheckedException;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.def.SerializerDef;
import io.activej.serializer.def.impl.RegularCollectionSerializerDef;

import java.util.function.UnaryOperator;

import static io.activej.codegen.expression.Expressions.*;
import static hppc.SerializerFactoryUtils.capitalize;

public class Hppc9RegularCollectionSerializerDef extends RegularCollectionSerializerDef {
	public Hppc9RegularCollectionSerializerDef(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> valueType) {
		this(valueSerializer, collectionType, collectionImplType, valueType, false);
	}

	protected Hppc9RegularCollectionSerializerDef(SerializerDef valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> elementType, boolean nullable) {
		super(valueSerializer, collectionType, collectionImplType, elementType, nullable);
	}

	@Override
	protected SerializerDef doEnsureNullable(CompatibilityLevel compatibilityLevel) {
		return new Hppc9RegularCollectionSerializerDef(valueSerializer, encodeType, decodeType, elementType, true);
	}

	@Override
	protected Expression doIterate(Expression collection, UnaryOperator<Expression> action) {
		try {
			String prefix = capitalize(elementType.getSimpleName());
			Class<?> iteratorType = Class.forName("com.carrotsearch.hppc.cursors." + prefix + "Cursor");
			return iterateIterable(collection,
				it -> action.apply(
					property(cast(it, iteratorType), "value")));
		} catch (ClassNotFoundException e) {
			throw UncheckedException.of(e);
		}
	}
}
