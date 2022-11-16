package io.activej.serializer;

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

}
