package io.activej.cube;

import io.activej.cube.CubeStructure.AggregationConfig;
import io.activej.cube.aggregation.fieldtype.FieldType;
import io.activej.cube.aggregation.measure.Measure;
import io.activej.cube.aggregation.predicate.AggregationPredicate;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.activej.common.collection.CollectionUtils.first;
import static io.activej.common.collection.CollectorUtils.entriesToLinkedHashMap;
import static io.activej.cube.CubeStructure.AggregationConfig.id;
import static io.activej.cube.aggregation.fieldtype.FieldTypes.*;
import static io.activej.cube.aggregation.measure.Measures.*;
import static io.activej.cube.aggregation.predicate.AggregationPredicates.*;
import static org.junit.Assert.*;

@SuppressWarnings("rawtypes")
public class TestCompatibleAggregations {
	private static final Map<String, String> DATA_ITEM_DIMENSIONS = Stream.of(
			Map.entry("date", "date"),
			Map.entry("advertiser", "advertiser"),
			Map.entry("campaign", "campaign"),
			Map.entry("banner", "banner"),
			Map.entry("affiliate", "affiliate"),
			Map.entry("site", "site"),
			Map.entry("placement", "placement"))
		.collect(entriesToLinkedHashMap());

	private static final Map<String, String> DATA_ITEM_MEASURES = Stream.of(
			Map.entry("eventCount", "null"),
			Map.entry("minRevenue", "revenue"),
			Map.entry("maxRevenue", "revenue"),
			Map.entry("revenue", "revenue"),
			Map.entry("impressions", "impressions"),
			Map.entry("clicks", "clicks"),
			Map.entry("conversions", "conversions"),
			Map.entry("uniqueUserIdsCount", "userId"),
			Map.entry("errors", "errors"))
		.collect(entriesToLinkedHashMap());

	private static final Map<String, FieldType> DIMENSIONS_DAILY_AGGREGATION = Stream.of(
			Map.entry("date", ofLocalDate(LocalDate.parse("2000-01-01"))))
		.collect(entriesToLinkedHashMap());

	private static final Map<String, FieldType> DIMENSIONS_ADVERTISERS_AGGREGATION = Stream.of(
			Map.entry("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			Map.entry("advertiser", ofInt()),
			Map.entry("campaign", ofInt()),
			Map.entry("banner", ofInt()))
		.collect(entriesToLinkedHashMap());

	private static final Map<String, FieldType> DIMENSIONS_AFFILIATES_AGGREGATION = Stream.of(
			Map.entry("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			Map.entry("affiliate", ofInt()),
			Map.entry("site", ofString()))
		.collect(entriesToLinkedHashMap());

	private static final Map<String, FieldType> DIMENSIONS_DETAILED_AFFILIATES_AGGREGATION = Stream.of(
			Map.entry("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			Map.entry("affiliate", ofInt()),
			Map.entry("site", ofString()),
			Map.entry("placement", ofInt()))
		.collect(entriesToLinkedHashMap());

	private static final Map<String, Measure> MEASURES = Stream.of(
			Map.entry("impressions", sum(ofLong())),
			Map.entry("clicks", sum(ofLong())),
			Map.entry("conversions", sum(ofLong())),
			Map.entry("revenue", sum(ofDouble())),
			Map.entry("eventCount", count(ofInt())),
			Map.entry("minRevenue", min(ofDouble())),
			Map.entry("maxRevenue", max(ofDouble())),
			Map.entry("uniqueUserIdsCount", hyperLogLog(1024)),
			Map.entry("errors", sum(ofLong())))
		.collect(entriesToLinkedHashMap());

	private static final AggregationPredicate DAILY_AGGREGATION_PREDICATE = alwaysTrue();
	private static final AggregationConfig DAILY_AGGREGATION = id("daily")
		.withDimensions(DIMENSIONS_DAILY_AGGREGATION.keySet())
		.withMeasures(MEASURES.keySet())
		.withPredicate(DAILY_AGGREGATION_PREDICATE);

	private static final int EXCLUDE_AFFILIATE = 0;
	private static final String EXCLUDE_SITE = "--";
	private static final Object EXCLUDE_PLACEMENT = 0;

	private static final int EXCLUDE_ADVERTISER = 0;
	private static final int EXCLUDE_CAMPAIGN = 0;
	private static final int EXCLUDE_BANNER = 0;

	private static final AggregationPredicate ADVERTISER_AGGREGATION_PREDICATE =
		and(has("advertiser"), has("banner"), has("campaign"));
	private static final AggregationConfig ADVERTISERS_AGGREGATION = id("advertisers")
		.withDimensions(DIMENSIONS_ADVERTISERS_AGGREGATION.keySet())
		.withMeasures(MEASURES.keySet())
		.withPredicate(ADVERTISER_AGGREGATION_PREDICATE);

	private static final AggregationPredicate AFFILIATES_AGGREGATION_PREDICATE =
		and(has("affiliate"), has("site"));
	private static final AggregationConfig AFFILIATES_AGGREGATION = id("affiliates")
		.withDimensions(DIMENSIONS_AFFILIATES_AGGREGATION.keySet())
		.withMeasures(MEASURES.keySet())
		.withPredicate(AFFILIATES_AGGREGATION_PREDICATE);

	private static final AggregationPredicate DETAILED_AFFILIATES_AGGREGATION_PREDICATE =
		and(has("affiliate"), has("site"), has("placement"));
	private static final AggregationConfig DETAILED_AFFILIATES_AGGREGATION = id("detailed_affiliates")
		.withDimensions(DIMENSIONS_DETAILED_AFFILIATES_AGGREGATION.keySet())
		.withMeasures(MEASURES.keySet())
		.withPredicate(DETAILED_AFFILIATES_AGGREGATION_PREDICATE);

	private static final AggregationPredicate LIMITED_DATES_AGGREGATION_PREDICATE =
		and(between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2010-01-01")));
	private static final AggregationConfig LIMITED_DATES_AGGREGATION = id("limited_date")
		.withDimensions(DIMENSIONS_DAILY_AGGREGATION.keySet())
		.withMeasures(MEASURES.keySet())
		.withPredicate(LIMITED_DATES_AGGREGATION_PREDICATE);

	private CubeStructure structure;
	private CubeStructure structureWithDetailedAggregation;

	@Before
	public void setUp() {
		structure = CubeStructure.builder()
			.initialize(cube -> {
				MEASURES.forEach(cube::withMeasure);

				cube.withDimension("date", ofLocalDate(LocalDate.parse("2000-01-01")));
				cube.withDimension("advertiser", ofInt(), notEq("advertiser", EXCLUDE_ADVERTISER));
				cube.withDimension("campaign", ofInt(), notEq("campaign", EXCLUDE_CAMPAIGN));
				cube.withDimension("banner", ofInt(), notEq("banner", EXCLUDE_BANNER));
				cube.withDimension("affiliate", ofInt(), notEq("affiliate", EXCLUDE_AFFILIATE));
				cube.withDimension("site", ofString(), notEq("site", EXCLUDE_SITE));

				List.of(DAILY_AGGREGATION, ADVERTISERS_AGGREGATION, AFFILIATES_AGGREGATION).forEach(cube::withAggregation);
			})
			.build();

		structureWithDetailedAggregation = CubeStructure.builder()
			.initialize(cube -> {
				MEASURES.forEach(cube::withMeasure);

				cube.withDimension("date", ofLocalDate(LocalDate.parse("2000-01-01")));
				cube.withDimension("advertiser", ofInt(), notEq("advertiser", EXCLUDE_ADVERTISER));
				cube.withDimension("campaign", ofInt(), notEq("campaign", EXCLUDE_CAMPAIGN));
				cube.withDimension("banner", ofInt(), notEq("banner", EXCLUDE_BANNER));
				cube.withDimension("affiliate", ofInt(), notEq("affiliate", EXCLUDE_AFFILIATE));
				cube.withDimension("site", ofString(), notEq("site", EXCLUDE_SITE));
				cube.withDimension("placement", ofInt(), notEq("placement", EXCLUDE_PLACEMENT));

				List.of(DAILY_AGGREGATION, ADVERTISERS_AGGREGATION, AFFILIATES_AGGREGATION).forEach(cube::withAggregation);
			})
			.withAggregation(DETAILED_AFFILIATES_AGGREGATION)
			.withAggregation(LIMITED_DATES_AGGREGATION.withPredicate(LIMITED_DATES_AGGREGATION_PREDICATE))
			.build();
	}

	// region test getCompatibleAggregationsForQuery for data input
	@Test
	public void withAlwaysTrueDataPredicate_MatchesAllAggregations() {
		AggregationPredicate dataPredicate = alwaysTrue();
		Set<String> compatibleAggregations = structure.getCompatibleAggregationsForDataInput(
			DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate).keySet();

		assertEquals(3, compatibleAggregations.size());
		assertTrue(compatibleAggregations.contains(DAILY_AGGREGATION.getId()));
		assertTrue(compatibleAggregations.contains(ADVERTISERS_AGGREGATION.getId()));
		assertTrue(compatibleAggregations.contains(AFFILIATES_AGGREGATION.getId()));
	}

	@Test
	public void withCompatibleDataPredicate_MatchesAggregationWithPredicateThatSubsetOfDataPredicate2() {
		AggregationPredicate dataPredicate = and(has("affiliate"), has("site"));
		Map<String, AggregationPredicate> compatibleAggregationsWithFilterPredicate = structure.getCompatibleAggregationsForDataInput(
			DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate);

		assertEquals(3, compatibleAggregationsWithFilterPredicate.size());

		// matches aggregation with optimization
		// (if dataPredicate equals aggregationPredicate -> do not use stream filter)
		assertTrue(compatibleAggregationsWithFilterPredicate.containsKey(AFFILIATES_AGGREGATION.getId()));
		assertEquals(alwaysTrue(), compatibleAggregationsWithFilterPredicate.get(AFFILIATES_AGGREGATION.getId()));

		assertTrue(compatibleAggregationsWithFilterPredicate.containsKey(ADVERTISERS_AGGREGATION.getId()));
		assertEquals(ADVERTISER_AGGREGATION_PREDICATE.simplify(), compatibleAggregationsWithFilterPredicate.get(ADVERTISERS_AGGREGATION.getId()));

		assertTrue(compatibleAggregationsWithFilterPredicate.containsKey(DAILY_AGGREGATION.getId()));
		assertEquals(alwaysTrue(), compatibleAggregationsWithFilterPredicate.get(DAILY_AGGREGATION.getId()));
	}

	@Test
	public void withIncompatibleDataPredicate_DoesNotMatchAggregationWithLimitedDateRange() {
		AggregationPredicate dataPredicate = and(has("affiliate"), has("site"),
			between("date", LocalDate.parse("2012-01-01"), LocalDate.parse("2016-01-01")));
		Set<String> compatibleAggregations = structureWithDetailedAggregation.getCompatibleAggregationsForDataInput(
			DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate).keySet();

		assertFalse(compatibleAggregations.contains(LIMITED_DATES_AGGREGATION.getId()));
	}

	@Test
	public void withSubsetBetweenDataPredicate_MatchesAggregation() {
		AggregationPredicate dataPredicate = and(notEq("date", LocalDate.parse("2001-01-04")),
			between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01")));

		Map<String, AggregationPredicate> compatibleAggregations = structureWithDetailedAggregation.getCompatibleAggregationsForDataInput(
			DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate);

		//matches all aggregations, but with different filtering logic
		assertTrue(compatibleAggregations.containsKey(LIMITED_DATES_AGGREGATION.getId()));
		assertEquals(alwaysTrue(), compatibleAggregations.get(LIMITED_DATES_AGGREGATION.getId()));

		assertTrue(compatibleAggregations.containsKey(DAILY_AGGREGATION.getId()));
		assertEquals(alwaysTrue(), compatibleAggregations.get(DAILY_AGGREGATION.getId()));

		assertTrue(compatibleAggregations.containsKey(ADVERTISERS_AGGREGATION.getId()));
		assertEquals(ADVERTISER_AGGREGATION_PREDICATE.simplify(), compatibleAggregations.get(ADVERTISERS_AGGREGATION.getId()));

		assertTrue(compatibleAggregations.containsKey(AFFILIATES_AGGREGATION.getId()));
		assertEquals(AFFILIATES_AGGREGATION_PREDICATE.simplify(), compatibleAggregations.get(AFFILIATES_AGGREGATION.getId()));

		assertTrue(compatibleAggregations.containsKey(DETAILED_AFFILIATES_AGGREGATION.getId()));
		assertEquals(DETAILED_AFFILIATES_AGGREGATION_PREDICATE.simplify(), compatibleAggregations.get(DETAILED_AFFILIATES_AGGREGATION.getId()));
	}
	// endregion

	// region test getCompatibleAggregationsForQuery for query
	@Test
	public void withWherePredicateAlwaysTrue_MatchesDailyAggregation() {
		AggregationPredicate whereQueryPredicate = alwaysTrue();

		Set<String> actualAggregations = structure.getCompatibleAggregationsForQuery(
			List.of("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		AggregationStructure expected = structure.getAggregationStructure(DAILY_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), structure.getAggregationStructure(first(actualAggregations)).toString());
	}

	@Test
	public void withWherePredicateForAdvertisersAggregation_MatchesAdvertisersAggregation() {
		AggregationPredicate whereQueryPredicate = and(
			has("advertiser"),
			has("campaign"),
			has("banner"));

		Set<String> actualAggregations = structure.getCompatibleAggregationsForQuery(
			List.of("advertiser", "campaign", "banner"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		AggregationStructure expected = structure.getAggregationStructure(ADVERTISERS_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), structure.getAggregationStructure(first(actualAggregations)).toString());
	}

	@Test
	public void withWherePredicateForAffiliatesAggregation_MatchesAffiliatesAggregation() {
		AggregationPredicate whereQueryPredicate = and(has("affiliate"), has("site"));

		Set<String> actualAggregations = structure.getCompatibleAggregationsForQuery(
			List.of("affiliate", "site"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		AggregationStructure expected = structure.getAggregationStructure(AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), structure.getAggregationStructure(first(actualAggregations)).toString());
	}

	@Test
	public void withWherePredicateForBothAffiliatesAggregations_MatchesAffiliatesAggregation() {
		AggregationPredicate whereQueryPredicate = and(
			has("affiliate"),
			has("site"),
			has("placement"));

		Set<String> actualAggregations =
			structureWithDetailedAggregation.getCompatibleAggregationsForQuery(
				List.of("affiliate", "site", "placement"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		AggregationStructure expected = structureWithDetailedAggregation.getAggregationStructure(DETAILED_AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), structureWithDetailedAggregation.getAggregationStructure(first(actualAggregations)).toString());
	}

	@Test
	public void withWherePredicateForDetailedAffiliatesAggregations_MatchesDetailedAffiliatesAggregation() {
		AggregationPredicate whereQueryPredicate = and(has("affiliate"), has("site"), has("placement"));

		Set<String> actualAggregations =
			structureWithDetailedAggregation.getCompatibleAggregationsForQuery(
				List.of("affiliate", "site", "placement"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		AggregationStructure expected = structureWithDetailedAggregation.getAggregationStructure(DETAILED_AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), structureWithDetailedAggregation.getAggregationStructure(first(actualAggregations)).toString());
	}

	@Test
	public void withWherePredicateForDailyAggregation_MatchesOnlyDailyAggregations() {
		AggregationPredicate whereQueryPredicate = between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01"));

		Set<String> actualAggregations =
			structureWithDetailedAggregation.getCompatibleAggregationsForQuery(
				List.of("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		AggregationStructure expected = structureWithDetailedAggregation.getAggregationStructure(DAILY_AGGREGATION.getId());
		AggregationStructure expected2 = structureWithDetailedAggregation.getAggregationStructure(LIMITED_DATES_AGGREGATION.getId());

		assertEquals(
			Set.of(
				expected.toString(),
				expected2.toString()
			),
			actualAggregations.stream()
				.map(structureWithDetailedAggregation::getAggregationStructure)
				.map(Object::toString)
				.collect(Collectors.toSet())
		);
	}

	@Test
	public void withWherePredicateForAdvertisersAggregation_MatchesOneAggregation() {
		AggregationPredicate whereQueryPredicate = and(
			has("advertiser"), has("campaign"), has("banner"),
			between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01")));

		Set<String> actualAggregations =
			structureWithDetailedAggregation.getCompatibleAggregationsForQuery(
				List.of("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		AggregationStructure expected = structureWithDetailedAggregation.getAggregationStructure(ADVERTISERS_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), structureWithDetailedAggregation.getAggregationStructure(first(actualAggregations)).toString());
	}

	@Test
	public void withWherePredicateForDailyAggregation_MatchesTwoAggregations() {
		AggregationPredicate whereQueryPredicate = eq("date", LocalDate.parse("2001-01-01"));

		Set<String> actualAggregations =
			structureWithDetailedAggregation.getCompatibleAggregationsForQuery(
				List.of("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		AggregationStructure expected = structureWithDetailedAggregation.getAggregationStructure(DAILY_AGGREGATION.getId());
		AggregationStructure expected2 = structureWithDetailedAggregation.getAggregationStructure(LIMITED_DATES_AGGREGATION.getId());

		assertEquals(
			Set.of(
				expected.toString(),
				expected2.toString()
			),
			actualAggregations.stream()
				.map(structureWithDetailedAggregation::getAggregationStructure)
				.map(Object::toString)
				.collect(Collectors.toSet())
		);
	}
	//endregion

}
