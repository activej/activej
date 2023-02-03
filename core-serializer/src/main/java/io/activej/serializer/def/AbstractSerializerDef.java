package io.activej.serializer.def;

import io.activej.serializer.CompatibilityLevel;

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

}
