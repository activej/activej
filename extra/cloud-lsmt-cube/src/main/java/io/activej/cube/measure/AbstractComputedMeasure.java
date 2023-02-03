package io.activej.cube.measure;

import io.activej.aggregation.measure.Measure;
import org.jetbrains.annotations.Nullable;

import java.util.*;

public abstract class AbstractComputedMeasure implements ComputedMeasure {
	protected final Set<ComputedMeasure> dependencies;

	protected AbstractComputedMeasure(ComputedMeasure... dependencies) {
		this.dependencies = new LinkedHashSet<>(List.of(dependencies));
	}

	@Override
	public @Nullable Class<?> getType(Map<String, Measure> storedMeasures) {
		return null;
	}

	@Override
	public final Set<String> getMeasureDependencies() {
		Set<String> result = new LinkedHashSet<>();
		for (ComputedMeasure dependency : dependencies) {
			result.addAll(dependency.getMeasureDependencies());
		}
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AbstractComputedMeasure other = (AbstractComputedMeasure) o;
		return dependencies.equals(other.dependencies);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dependencies);
	}
}
