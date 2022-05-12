package io.activej.serializer;

import io.activej.codegen.expression.Expression;

import java.util.Map;
import java.util.Set;

public abstract class AbstractSerializerDef implements SerializerDef {
	@Override
	public void accept(Visitor visitor) {
	}

	@Override
	public Set<Integer> getVersions() {
		return Set.of();
	}

	@Override
	public boolean isInline(int version, CompatibilityLevel compatibilityLevel) {
		return true;
	}

	@Override
	public Class<?> getDecodeType() {
		return getEncodeType();
	}

	@Override
	public Map<Object, Expression> getEncoderInitializer() {
		return Map.of();
	}

	@Override
	public Map<Object, Expression> getDecoderInitializer() {
		return Map.of();
	}

	@Override
	public Map<Object, Expression> getEncoderFinalizer() {
		return Map.of();
	}

	@Override
	public Map<Object, Expression> getDecoderFinalizer() {
		return Map.of();
	}
}
