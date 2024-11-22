package io.activej.cube;

import io.activej.common.builder.AbstractBuilder;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.initializer.WithInitializer;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import io.activej.cube.attributes.IAttributeResolver;
import io.activej.cube.exception.QueryException;
import io.activej.cube.measure.ComputedMeasure;

import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkState;
import static io.activej.common.collection.CollectorUtils.entriesToLinkedHashMap;
import static io.activej.cube.Utils.filterEntryKeys;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.*;
import static io.activej.types.Primitives.wrap;
import static java.util.function.Predicate.not;

@SuppressWarnings("rawtypes")
public final class CubeStructure {

	private final Map<String, FieldType> fieldTypes = new LinkedHashMap<>();
	private final Map<String, FieldType> dimensionTypes = new LinkedHashMap<>();
	private final Map<String, AggregationPredicate> validityPredicates = new LinkedHashMap<>();
	private final Map<String, Measure> measures = new LinkedHashMap<>();
	private final Map<String, ComputedMeasure> computedMeasures = new LinkedHashMap<>();

	public static final class AttributeResolverContainer {
		final List<String> attributes = new ArrayList<>();
		final List<String> dimensions;
		final IAttributeResolver resolver;

		private AttributeResolverContainer(List<String> dimensions, IAttributeResolver resolver) {
			this.dimensions = dimensions;
			this.resolver = resolver;
		}
	}

	private final List<AttributeResolverContainer> attributeResolvers = new ArrayList<>();
	private final Map<String, Class<?>> attributeTypes = new LinkedHashMap<>();
	private final Map<String, AttributeResolverContainer> attributes = new LinkedHashMap<>();

	private final Map<String, String> childParentRelations = new LinkedHashMap<>();

	public static final class AggregationConfig implements WithInitializer<AggregationConfig> {
		private final String id;
		private final List<String> dimensions = new ArrayList<>();
		private final List<String> measures = new ArrayList<>();
		private AggregationPredicate predicate = alwaysTrue();
		private final List<String> partitioningKey = new ArrayList<>();

		public AggregationConfig(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		public List<String> getDimensions() {
			return dimensions;
		}

		public List<String> getMeasures() {
			return measures;
		}

		public AggregationPredicate getPredicate() {
			return predicate;
		}

		public List<String> getPartitioningKey() {
			return partitioningKey;
		}

		public static AggregationConfig id(String id) {
			return new AggregationConfig(id);
		}

		public AggregationConfig withDimensions(Collection<String> dimensions) {
			this.dimensions.addAll(dimensions);
			return this;
		}

		public AggregationConfig withDimensions(String... dimensions) {
			return withDimensions(List.of(dimensions));
		}

		public AggregationConfig withMeasures(Collection<String> measures) {
			this.measures.addAll(measures);
			return this;
		}

		public AggregationConfig withMeasures(String... measures) {
			return withMeasures(List.of(measures));
		}

		public AggregationConfig withPredicate(AggregationPredicate predicate) {
			this.predicate = predicate;
			return this;
		}

		public AggregationConfig withPartitioningKey(List<String> partitioningKey) {
			this.partitioningKey.addAll(partitioningKey);
			return this;
		}

		public AggregationConfig withPartitioningKey(String... partitioningKey) {
			this.partitioningKey.addAll(List.of(partitioningKey));
			return this;
		}
	}

	// state
	private final Map<String, AggregationStructure> aggregationStructures = new LinkedHashMap<>();

	private CubeStructure() {
	}

	public static Builder builder() {
		return new CubeStructure().new Builder(new LinkedHashMap<>());
	}

	public final class Builder extends AbstractBuilder<Builder, CubeStructure> {
		private final Map<String, AggregationConfig> aggregationConfigs;

		private Builder(Map<String, AggregationConfig> aggregationConfigs) {this.aggregationConfigs = aggregationConfigs;}

		public Builder withAttribute(String attribute, IAttributeResolver resolver) {
			checkNotBuilt(this);
			checkArgument(!attributes.containsKey(attribute), "Attribute %s has already been defined", attribute);
			int pos = attribute.indexOf('.');
			if (pos == -1)
				throw new IllegalArgumentException("Attribute identifier is not split into name and dimension");
			String dimension = attribute.substring(0, pos);
			String attributeName = attribute.substring(pos + 1);
			checkArgument(resolver.getAttributeTypes().containsKey(attributeName), "Resolver does not support %s", attribute);
			List<String> dimensions = getAllParents(dimension);
			checkArgument(dimensions.size() == resolver.getKeyTypes().length, "Parent dimensions: %s, key types: %s", dimensions, List.of(resolver.getKeyTypes()));
			for (int i = 0; i < dimensions.size(); i++) {
				String d = dimensions.get(i);
				checkArgument(((Class<?>) dimensionTypes.get(d).getInternalDataType()).equals(resolver.getKeyTypes()[i]), "Dimension type mismatch for %s", d);
			}
			AttributeResolverContainer resolverContainer = null;
			for (AttributeResolverContainer r : attributeResolvers) {
				if (r.resolver == resolver) {
					resolverContainer = r;
					break;
				}
			}
			if (resolverContainer == null) {
				resolverContainer = new AttributeResolverContainer(dimensions, resolver);
				attributeResolvers.add(resolverContainer);
			}
			resolverContainer.attributes.add(attribute);
			attributes.put(attribute, resolverContainer);
			attributeTypes.put(attribute, resolver.getAttributeTypes().get(attributeName));
			return this;
		}

		public Builder withDimension(String dimensionId, FieldType type, AggregationPredicate validityPredicate) {
			withDimension(dimensionId, type);

			Set<String> predicateDimensions = validityPredicate.getDimensions();
			checkArgument(predicateDimensions.isEmpty() || predicateDimensions.equals(Set.of(dimensionId)),
				"Predicate refers to other dimensions");
			validityPredicates.put(dimensionId, validityPredicate);
			return this;
		}

		public Builder withDimension(String dimensionId, FieldType type) {
			checkNotBuilt(this);
			checkState(Comparable.class.isAssignableFrom(wrap((Class<?>) type.getDataType())), "Dimension type is not primitive or Comparable");
			dimensionTypes.put(dimensionId, type);
			fieldTypes.put(dimensionId, type);
			return this;
		}

		public Builder withMeasure(String measureId, Measure measure) {
			checkNotBuilt(this);
			measures.put(measureId, measure);
			fieldTypes.put(measureId, measure.getFieldType());
			return this;
		}

		public Builder withComputedMeasure(String measureId, ComputedMeasure computedMeasure) {
			checkNotBuilt(this);
			computedMeasures.put(measureId, computedMeasure);
			return this;
		}

		public Builder withRelation(String child, String parent) {
			checkNotBuilt(this);
			childParentRelations.put(child, parent);
			return this;
		}

		public Builder withAggregation(AggregationConfig aggregationConfig) {
			checkNotBuilt(this);
			checkArgument(!aggregationConfigs.containsKey(aggregationConfig.id), "Aggregation '%s' is already defined", aggregationConfig.id);
			aggregationConfigs.put(aggregationConfig.id, aggregationConfig);
			return this;
		}

		@Override
		protected CubeStructure doBuild() {
			for (AggregationConfig aggregationConfig : aggregationConfigs.values()) {
				addAggregation(aggregationConfig);
			}
			return CubeStructure.this;
		}

		private void addAggregation(AggregationConfig aggregationConfig) {
			AggregationStructure aggregationStructure = new AggregationStructure();
			for (String dimensionId : aggregationConfig.dimensions) {
				aggregationStructure.addKey(dimensionId, dimensionTypes.get(dimensionId));
			}
			for (String measureId : aggregationConfig.measures) {
				aggregationStructure.addMeasure(measureId, measures.get(measureId));
			}
			for (Entry<String, Measure> entry : measures.entrySet()) {
				String measureId = entry.getKey();
				Measure measure = entry.getValue();
				if (!aggregationConfig.measures.contains(measureId)) {
					aggregationStructure.addIgnoredMeasure(measureId, measure.getFieldType());
				}
			}

			aggregationStructure.addPartitioningKey(aggregationConfig.partitioningKey);
			aggregationStructure.setPredicate(aggregationConfig.getPredicate());
			aggregationStructure.setPrecondition(and(aggregationConfig.getDimensions().stream()
				.map(validityPredicates::get)
				.filter(Objects::nonNull)
				.toList())
				.simplify());

			aggregationStructures.put(aggregationConfig.id, aggregationStructure);
		}
	}

	public List<String> getAllParents(String dimension) {
		ArrayList<String> chain = new ArrayList<>();
		chain.add(dimension);
		String child = dimension;
		String parent;
		while ((parent = childParentRelations.get(child)) != null) {
			chain.add(0, parent);
			child = parent;
		}
		return chain;
	}

	public Map<String, FieldType> getFieldTypes() {
		return fieldTypes;
	}

	public Map<String, FieldType> getDimensionTypes() {
		return dimensionTypes;
	}

	public Map<String, AggregationPredicate> getValidityPredicates() {
		return validityPredicates;
	}

	public Map<String, Measure> getMeasures() {
		return measures;
	}

	public Map<String, ComputedMeasure> getComputedMeasures() {
		return computedMeasures;
	}

	public List<AttributeResolverContainer> getAttributeResolvers() {
		return attributeResolvers;
	}

	public Map<String, Class<?>> getAttributeTypes() {
		return attributeTypes;
	}

	public Map<String, AttributeResolverContainer> getAttributes() {
		return attributes;
	}

	public Map<String, String> getChildParentRelations() {
		return childParentRelations;
	}

	public Map<String, AggregationStructure> getAggregationStructures() {
		return aggregationStructures;
	}

	public Map<String, Type> getAllAttributeTypes() {
		Map<String, Type> result = new LinkedHashMap<>();
		for (Entry<String, FieldType> entry : dimensionTypes.entrySet()) {
			result.put(entry.getKey(), entry.getValue().getDataType());
		}
		result.putAll(attributeTypes);
		return result;
	}

	public Map<String, Type> getAllMeasureTypes() {
		Map<String, Type> result = new LinkedHashMap<>();
		for (Entry<String, Measure> entry : measures.entrySet()) {
			result.put(entry.getKey(), entry.getValue().getFieldType().getDataType());
		}
		for (Entry<String, ComputedMeasure> entry : computedMeasures.entrySet()) {
			result.put(entry.getKey(), entry.getValue().getType(measures));
		}
		return result;
	}

	public Class<?> getAttributeInternalType(String attribute) {
		if (dimensionTypes.containsKey(attribute))
			return dimensionTypes.get(attribute).getInternalDataType();
		if (attributeTypes.containsKey(attribute))
			return attributeTypes.get(attribute);
		throw new IllegalArgumentException("No attribute: " + attribute);
	}

	public Class<?> getMeasureInternalType(String field) {
		if (measures.containsKey(field))
			return measures.get(field).getFieldType().getInternalDataType();
		if (computedMeasures.containsKey(field))
			return computedMeasures.get(field).getType(measures);
		throw new IllegalArgumentException("No measure: " + field);
	}

	public Type getAttributeType(String attribute) {
		if (dimensionTypes.containsKey(attribute))
			return dimensionTypes.get(attribute).getDataType();
		if (attributeTypes.containsKey(attribute))
			return attributeTypes.get(attribute);
		throw new IllegalArgumentException("No attribute: " + attribute);
	}

	public Type getMeasureType(String field) {
		if (measures.containsKey(field))
			return measures.get(field).getFieldType().getDataType();
		if (computedMeasures.containsKey(field))
			return computedMeasures.get(field).getType(measures);
		throw new IllegalArgumentException("No measure: " + field);
	}

	public AggregationStructure getAggregationStructure(String aggregationId) {
		return aggregationStructures.get(aggregationId);
	}

	public Set<String> getAggregationIds() {
		return aggregationStructures.keySet();
	}

	public Set<String> getCompatibleAggregationsForQuery(
		Collection<String> dimensions, Collection<String> storedMeasures, AggregationPredicate where
	) {
		where = where.simplify();
		List<String> allDimensions = Stream.concat(dimensions.stream(), where.getDimensions().stream()).toList();

		Set<String> compatibleAggregations = new HashSet<>();
		for (Entry<String, AggregationStructure> entry : aggregationStructures.entrySet()) {
			AggregationStructure structure = entry.getValue();
			Set<String> keys = new HashSet<>(structure.getKeys());
			if (!keys.containsAll(allDimensions)) continue;

			List<String> compatibleMeasures = storedMeasures.stream().filter(structure.getMeasures()::contains).toList();
			if (compatibleMeasures.isEmpty()) continue;
			AggregationPredicate intersection = and(where, structure.getPredicate()).simplify();

			if (!intersection.equals(where)) continue;
			compatibleAggregations.add(entry.getKey());
		}
		return compatibleAggregations;
	}

	public Map<String, AggregationPredicate> getCompatibleAggregationsForDataInput(
		Map<String, String> dimensionFields, Map<String, String> measureFields, AggregationPredicate predicate
	) {
		AggregationPredicate dataPredicate = predicate.simplify();
		Map<String, AggregationPredicate> aggregationToDataInputFilterPredicate = new HashMap<>();
		for (Entry<String, AggregationStructure> entry : aggregationStructures.entrySet()) {
			AggregationStructure structure = entry.getValue();

			Set<String> dimensions = dimensionFields.keySet();
			if (!dimensions.containsAll(structure.getKeys())) continue;

			Map<String, String> aggregationMeasureFields = filterEntryKeys(measureFields.entrySet().stream(), structure.getMeasures()::contains)
				.collect(entriesToLinkedHashMap());
			if (aggregationMeasureFields.isEmpty()) continue;

			AggregationPredicate aggregationPredicate = structure.getPredicate().simplify();

			AggregationPredicate intersection = and(aggregationPredicate, dataPredicate).simplify();
			if (alwaysFalse().equals(intersection)) continue;

			if (intersection.equals(dataPredicate)) {
				aggregationToDataInputFilterPredicate.put(entry.getKey(), alwaysTrue());
				continue;
			}

			aggregationToDataInputFilterPredicate.put(entry.getKey(), aggregationPredicate);
		}
		return aggregationToDataInputFilterPredicate;
	}

	public void validateMeasures(String aggregationId, List<String> measures) throws MalformedDataException {
		AggregationStructure aggregationStructure = getAggregationStructure(aggregationId);
		if (aggregationStructure == null) {
			throw new MalformedDataException("Unknown aggregation: " + aggregationId);
		}
		Set<String> allowedMeasures = aggregationStructure.getMeasureTypes().keySet();
		List<String> unknownMeasures = measures.stream()
			.filter(not(allowedMeasures::contains))
			.toList();
		if (!unknownMeasures.isEmpty()) {
			throw new MalformedDataException(String.format("Unknown measures %s in aggregation '%s'", unknownMeasures, aggregationId));
		}
	}

	public PreprocessedQuery preprocessQuery(CubeQuery query) throws QueryException {
		return new CubePreprocessor(query).preprocess();
	}

	public class CubePreprocessor {
		private final Set<String> resultDimensions = new LinkedHashSet<>();
		private final Set<String> resultAttributes = new LinkedHashSet<>();

		private final Set<String> resultMeasures = new LinkedHashSet<>();
		private final Set<String> resultStoredMeasures = new LinkedHashSet<>();
		private final Set<String> resultComputedMeasures = new LinkedHashSet<>();

		private final List<String> recordAttributes = new ArrayList<>();
		private final List<String> recordMeasures = new ArrayList<>();

		private final CubeQuery query;

		public CubePreprocessor(CubeQuery query) {
			this.query = query;
		}

		private CubeStructure.PreprocessedQuery preprocess() throws QueryException {
			prepareDimensions();
			prepareMeasures();

			return new CubeStructure.PreprocessedQuery(
				query,
				resultDimensions,
				resultAttributes,
				resultMeasures,
				resultStoredMeasures,
				resultComputedMeasures,
				recordAttributes,
				recordMeasures
			);
		}

		void prepareDimensions() throws QueryException {
			for (String attribute : query.getAttributes()) {
				recordAttributes.add(attribute);
				List<String> dimensions = new ArrayList<>();
				if (getDimensionTypes().containsKey(attribute)) {
					dimensions = getAllParents(attribute);
				} else if (getAttributes().containsKey(attribute)) {
					AttributeResolverContainer resolverContainer = getAttributes().get(attribute);
					for (String dimension : resolverContainer.dimensions) {
						dimensions.addAll(getAllParents(dimension));
					}
				} else {
					throw new QueryException("Attribute not found: " + attribute);
				}
				resultDimensions.addAll(dimensions);
				resultAttributes.addAll(dimensions);
				resultAttributes.add(attribute);
			}
		}

		void prepareMeasures() {
			Set<String> queryStoredMeasures = new HashSet<>();
			for (String measure : query.getMeasures()) {
				if (getComputedMeasures().containsKey(measure)) {
					queryStoredMeasures.addAll(getComputedMeasures().get(measure).getMeasureDependencies());
				} else if (getMeasures().containsKey(measure)) {
					queryStoredMeasures.add(measure);
				}
			}
			Set<String> compatibleAggregations = getCompatibleAggregationsForQuery(resultDimensions, queryStoredMeasures, query.getWhere());

			Set<String> compatibleMeasures = new LinkedHashSet<>();
			for (String aggregationId : compatibleAggregations) {
				compatibleMeasures.addAll(getAggregationStructure(aggregationId).getMeasures());
			}
			for (Entry<String, ComputedMeasure> entry : getComputedMeasures().entrySet()) {
				if (compatibleMeasures.containsAll(entry.getValue().getMeasureDependencies())) {
					compatibleMeasures.add(entry.getKey());
				}
			}

			for (String queryMeasure : query.getMeasures()) {
				if (!compatibleMeasures.contains(queryMeasure) || recordMeasures.contains(queryMeasure))
					continue;
				recordMeasures.add(queryMeasure);
				if (getMeasures().containsKey(queryMeasure)) {
					resultStoredMeasures.add(queryMeasure);
					resultMeasures.add(queryMeasure);
				} else if (getComputedMeasures().containsKey(queryMeasure)) {
					ComputedMeasure expression = getComputedMeasures().get(queryMeasure);
					Set<String> dependencies = expression.getMeasureDependencies();
					resultStoredMeasures.addAll(dependencies);
					resultComputedMeasures.add(queryMeasure);
					resultMeasures.addAll(dependencies);
					resultMeasures.add(queryMeasure);
				}
			}
		}

	}

	public record PreprocessedQuery(
		CubeQuery query,

		Set<String> resultDimensions,
		Set<String> resultAttributes,

		Set<String> resultMeasures,
		Set<String> resultStoredMeasures,
		Set<String> resultComputedMeasures,

		List<String> recordAttributes,
		List<String> recordMeasures
	) {
	}
}
