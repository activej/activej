package io.activej.cube;

import org.junit.Before;

public class AggregationKeyRelationsTest {
	private Cube cube;

	@Before
	public void setUp() {
		cube = new Cube(null, null, null, null)
				.withRelation("campaign", "advertiser")
				.withRelation("offer", "campaign")
				.withRelation("goal", "offer")
				.withRelation("banner", "offer")
				.withRelation("keyword", "offer");
	}

//	@Test
//	public void testDrillDownChains() throws Exception {
//		Set<List<String>> drillDownChains1 = cube.buildDrillDownChains(Sets.<String>newHashSet(), newHashSet("advertiser", "banner", "campaign", "offer"));
//		assertEquals(newHashSet(asList("advertiser"), asList("advertiser", "campaign", "offer"), asList("advertiser", "campaign", "offer", "banner"),
//				asList("advertiser", "campaign")), drillDownChains1);
//
//		Set<List<String>> drillDownChains2 = cube.buildDrillDownChains(Sets.<String>newHashSet(), newHashSet("banner", "campaign", "offer"));
//		assertEquals(newHashSet(asList("advertiser", "campaign", "offer", "banner"),
//				asList("advertiser", "campaign"), asList("advertiser", "campaign", "offer")), drillDownChains2);
//	}
}
