package io.activej.serializer;

import io.activej.codegen.expression.Expression;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public abstract class AbstractSerializerDef implements SerializerDef {
	@Override
	public void accept(Visitor visitor) {
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
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
		return Collections.emptyMap();
	}

	@Override
	public Map<Object, Expression> getDecoderInitializer() {
		return Collections.emptyMap();
	}

	@Override
	public Map<Object, Expression> getEncoderFinalizer() {
		return Collections.emptyMap();
	}

	@Override
	public Map<Object, Expression> getDecoderFinalizer() {
		return Collections.emptyMap();
	}
}
