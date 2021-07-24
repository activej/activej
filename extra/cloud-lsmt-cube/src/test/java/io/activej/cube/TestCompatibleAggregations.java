package io.activej.cube;

import io.activej.aggregation.Aggregation;
import io.activej.aggregation.AggregationPredicate;
import io.activej.aggregation.fieldtype.FieldType;
import io.activej.aggregation.measure.Measure;
import io.activej.cube.Cube.AggregationConfig;
import io.activej.cube.Cube.AggregationContainer;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.activej.aggregation.AggregationPredicates.*;
import static io.activej.aggregation.fieldtype.FieldTypes.*;
import static io.activej.aggregation.measure.Measures.*;
import static io.activej.common.Utils.entriesToMap;
import static io.activej.cube.Cube.AggregationConfig.id;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Stream.of;
import static org.junit.Assert.*;

@SuppressWarnings("rawtypes")
public class TestCompatibleAggregations {
	private static final Map<String, String> DATA_ITEM_DIMENSIONS = entriesToMap(of(
			new SimpleEntry<>("date", "date"),
			new SimpleEntry<>("advertiser", "advertiser"),
			new SimpleEntry<>("campaign", "campaign"),
			new SimpleEntry<>("banner", "banner"),
			new SimpleEntry<>("affiliate", "affiliate"),
			new SimpleEntry<>("site", "site"),
			new SimpleEntry<>("placement", "placement")));

	private static final Map<String, String> DATA_ITEM_MEASURES = entriesToMap(of(
			new SimpleEntry<>("eventCount", "null"),
			new SimpleEntry<>("minRevenue", "revenue"),
			new SimpleEntry<>("maxRevenue", "revenue"),
			new SimpleEntry<>("revenue", "revenue"),
			new SimpleEntry<>("impressions", "impressions"),
			new SimpleEntry<>("clicks", "clicks"),
			new SimpleEntry<>("conversions", "conversions"),
			new SimpleEntry<>("uniqueUserIdsCount", "userId"),
			new SimpleEntry<>("errors", "errors")));

	private static final Map<String, FieldType> DIMENSIONS_DAILY_AGGREGATION = entriesToMap(of(
			new SimpleEntry<>("date", ofLocalDate(LocalDate.parse("2000-01-01")))));

	private static final Map<String, FieldType> DIMENSIONS_ADVERTISERS_AGGREGATION = entriesToMap(of(
			new SimpleEntry<>("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			new SimpleEntry<>("advertiser", ofInt()),
			new SimpleEntry<>("campaign", ofInt()),
			new SimpleEntry<>("banner", ofInt())));

	private static final Map<String, FieldType> DIMENSIONS_AFFILIATES_AGGREGATION = entriesToMap(of(
			new SimpleEntry<>("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			new SimpleEntry<>("affiliate", ofInt()),
			new SimpleEntry<>("site", ofString())));

	private static final Map<String, FieldType> DIMENSIONS_DETAILED_AFFILIATES_AGGREGATION = entriesToMap(of(
			new SimpleEntry<>("date", ofLocalDate(LocalDate.parse("2000-01-01"))),
			new SimpleEntry<>("affiliate", ofInt()),
			new SimpleEntry<>("site", ofString()),
			new SimpleEntry<>("placement", ofInt())));

	private static final Map<String, Measure> MEASURES = entriesToMap(of(
			new SimpleEntry<>("impressions", sum(ofLong())),
			new SimpleEntry<>("clicks", sum(ofLong())),
			new SimpleEntry<>("conversions", sum(ofLong())),
			new SimpleEntry<>("revenue", sum(ofDouble())),
			new SimpleEntry<>("eventCount", count(ofInt())),
			new SimpleEntry<>("minRevenue", min(ofDouble())),
			new SimpleEntry<>("maxRevenue", max(ofDouble())),
			new SimpleEntry<>("uniqueUserIdsCount", hyperLogLog(1024)),
			new SimpleEntry<>("errors", sum(ofLong()))));

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
			and(not(eq("advertiser", EXCLUDE_ADVERTISER)), not(eq("banner", EXCLUDE_BANNER)), not(eq("campaign", EXCLUDE_CAMPAIGN)));
	private static final AggregationConfig ADVERTISERS_AGGREGATION = id("advertisers")
			.withDimensions(DIMENSIONS_ADVERTISERS_AGGREGATION.keySet())
			.withMeasures(MEASURES.keySet())
			.withPredicate(ADVERTISER_AGGREGATION_PREDICATE);

	private static final AggregationPredicate AFFILIATES_AGGREGATION_PREDICATE =
			and(not(eq("affiliate", EXCLUDE_AFFILIATE)), not(eq("site", EXCLUDE_SITE)));
	private static final AggregationConfig AFFILIATES_AGGREGATION = id("affiliates")
			.withDimensions(DIMENSIONS_AFFILIATES_AGGREGATION.keySet())
			.withMeasures(MEASURES.keySet())
			.withPredicate(AFFILIATES_AGGREGATION_PREDICATE);

	private static final AggregationPredicate DETAILED_AFFILIATES_AGGREGATION_PREDICATE =
			and(notEq("affiliate", EXCLUDE_AFFILIATE), not(eq("site", EXCLUDE_SITE)), not(eq("placement", EXCLUDE_PLACEMENT)));
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

	private Cube cube;
	private Cube cubeWithDetailedAggregation;

	@Before
	public void setUp() {
		cube = new Cube(null, null, null, null)
				.withInitializer(cube -> {
					MEASURES.forEach(cube::addMeasure);

					DIMENSIONS_DAILY_AGGREGATION.forEach(cube::addDimension);
					DIMENSIONS_ADVERTISERS_AGGREGATION.forEach(cube::addDimension);
					DIMENSIONS_AFFILIATES_AGGREGATION.forEach(cube::addDimension);

					asList(DAILY_AGGREGATION, ADVERTISERS_AGGREGATION, AFFILIATES_AGGREGATION).forEach(cube::addAggregation);
				});

		cubeWithDetailedAggregation = new Cube(null, null, null, null)
				.withInitializer(cube -> {
					MEASURES.forEach(cube::addMeasure);
					DIMENSIONS_DAILY_AGGREGATION.forEach(cube::addDimension);
					DIMENSIONS_ADVERTISERS_AGGREGATION.forEach(cube::addDimension);
					DIMENSIONS_AFFILIATES_AGGREGATION.forEach(cube::addDimension);
					DIMENSIONS_DETAILED_AFFILIATES_AGGREGATION.forEach(cube::addDimension);

					asList(DAILY_AGGREGATION, ADVERTISERS_AGGREGATION, AFFILIATES_AGGREGATION).forEach(cube::addAggregation);
				})
				.withAggregation(DETAILED_AFFILIATES_AGGREGATION)
				.withAggregation(LIMITED_DATES_AGGREGATION.withPredicate(LIMITED_DATES_AGGREGATION_PREDICATE));
	}

	// region test getCompatibleAggregationsForQuery for data input
	@Test
	public void withAlwaysTrueDataPredicate_MatchesAllAggregations() {
		AggregationPredicate dataPredicate = alwaysTrue();
		Set<String> compatibleAggregations = cube.getCompatibleAggregationsForDataInput(
				DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate).keySet();

		assertEquals(3, compatibleAggregations.size());
		assertTrue(compatibleAggregations.contains(DAILY_AGGREGATION.getId()));
		assertTrue(compatibleAggregations.contains(ADVERTISERS_AGGREGATION.getId()));
		assertTrue(compatibleAggregations.contains(AFFILIATES_AGGREGATION.getId()));
	}

	@Test
	public void withCompatibleDataPredicate_MatchesAggregationWithPredicateThatSubsetOfDataPredicate2() {
		AggregationPredicate dataPredicate = and(notEq("affiliate", EXCLUDE_AFFILIATE), notEq("site", EXCLUDE_SITE));
		Map<String, AggregationPredicate> compatibleAggregationsWithFilterPredicate = cube.getCompatibleAggregationsForDataInput(
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
		AggregationPredicate dataPredicate = and(not(eq("affiliate", EXCLUDE_AFFILIATE)), not(eq("site", EXCLUDE_SITE)),
				between("date", LocalDate.parse("2012-01-01"), LocalDate.parse("2016-01-01")));
		Set<String> compatibleAggregations = cubeWithDetailedAggregation.getCompatibleAggregationsForDataInput(
				DATA_ITEM_DIMENSIONS, DATA_ITEM_MEASURES, dataPredicate).keySet();

		assertFalse(compatibleAggregations.contains(LIMITED_DATES_AGGREGATION.getId()));
	}

	@Test
	public void withSubsetBetweenDataPredicate_MatchesAggregation() {
		AggregationPredicate dataPredicate = and(notEq("date", LocalDate.parse("2001-01-04")),
				between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01")));

		Map<String, AggregationPredicate> compatibleAggregations = cubeWithDetailedAggregation.getCompatibleAggregationsForDataInput(
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

		List<AggregationContainer> actualAggregations = cube.getCompatibleAggregationsForQuery(
				singletonList("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cube.getAggregation(DAILY_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForAdvertisersAggregation_MatchesAdvertisersAggregation() {
		AggregationPredicate whereQueryPredicate = and(
				not(eq("advertiser", EXCLUDE_AFFILIATE)),
				not(eq("campaign", EXCLUDE_CAMPAIGN)),
				not(eq("banner", EXCLUDE_BANNER)));

		List<AggregationContainer> actualAggregations = cube.getCompatibleAggregationsForQuery(
				asList("advertiser", "campaign", "banner"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cube.getAggregation(ADVERTISERS_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForAffiliatesAggregation_MatchesAffiliatesAggregation() {
		AggregationPredicate whereQueryPredicate = and(not(eq("affiliate", EXCLUDE_AFFILIATE)), not(eq("site", EXCLUDE_SITE)));

		List<AggregationContainer> actualAggregations = cube.getCompatibleAggregationsForQuery(
				asList("affiliate", "site"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cube.getAggregation(AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForBothAffiliatesAggregations_MatchesAffiliatesAggregation() {
		AggregationPredicate whereQueryPredicate = and(
				not(eq("affiliate", EXCLUDE_AFFILIATE)),
				not(eq("site", EXCLUDE_SITE)),
				not(eq("placement", EXCLUDE_PLACEMENT)));

		List<AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						asList("affiliate", "site", "placement"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(DETAILED_AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForDetailedAffiliatesAggregations_MatchesDetailedAffiliatesAggregation() {
		AggregationPredicate whereQueryPredicate = and(not(eq("affiliate", EXCLUDE_AFFILIATE)), not(eq("site", EXCLUDE_SITE)), not(eq("placement", EXCLUDE_PLACEMENT)));

		List<AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						asList("affiliate", "site", "placement"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(DETAILED_AFFILIATES_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForDailyAggregation_MatchesOnlyDailyAggregations() {
		AggregationPredicate whereQueryPredicate = between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01"));

		List<AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						singletonList("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(DAILY_AGGREGATION.getId());
		Aggregation expected2 = cubeWithDetailedAggregation.getAggregation(LIMITED_DATES_AGGREGATION.getId());

		assertEquals(2, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
		assertEquals(expected2.toString(), actualAggregations.get(1).toString());
	}

	@Test
	public void withWherePredicateForAdvertisersAggregation_MatchesOneAggregation() {
		AggregationPredicate whereQueryPredicate = and(
				not(eq("advertiser", EXCLUDE_ADVERTISER)), not(eq("campaign", EXCLUDE_CAMPAIGN)), not(eq("banner", EXCLUDE_BANNER)),
				between("date", LocalDate.parse("2001-01-01"), LocalDate.parse("2004-01-01")));

		List<AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						singletonList("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(ADVERTISERS_AGGREGATION.getId());

		assertEquals(1, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
	}

	@Test
	public void withWherePredicateForDailyAggregation_MatchesTwoAggregations() {
		AggregationPredicate whereQueryPredicate = eq("date", LocalDate.parse("2001-01-01"));

		List<AggregationContainer> actualAggregations =
				cubeWithDetailedAggregation.getCompatibleAggregationsForQuery(
						singletonList("date"), new ArrayList<>(MEASURES.keySet()), whereQueryPredicate);

		Aggregation expected = cubeWithDetailedAggregation.getAggregation(DAILY_AGGREGATION.getId());
		Aggregation expected2 = cubeWithDetailedAggregation.getAggregation(LIMITED_DATES_AGGREGATION.getId());

		assertEquals(2, actualAggregations.size());
		assertEquals(expected.toString(), actualAggregations.get(0).toString());
		assertEquals(expected2.toString(), actualAggregations.get(1).toString());
	}
	//endregion

}
