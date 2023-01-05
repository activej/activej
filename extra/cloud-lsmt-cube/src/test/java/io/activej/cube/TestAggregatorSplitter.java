package io.activej.cube;

import io.activej.cube.bean.TestPubRequest;
import io.activej.cube.bean.TestPubRequest.TestAdvRequest;
import io.activej.cube.bean.TestPubRequest.TestEnum;
import io.activej.cube.ot.CubeDiff;
import io.activej.datastream.StreamDataAcceptor;
import io.activej.etl.LogDataConsumerSplitter;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static io.activej.common.Utils.keysToMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;

public class TestAggregatorSplitter extends LogDataConsumerSplitter<TestPubRequest, CubeDiff> {
	private final Cube cube;

	public TestAggregatorSplitter(Cube cube) {
		this.cube = cube;
	}

	public static class AggregationItem {
		// pub
		public int date;
		public int hourOfDay;
		public int pub;
		public TestEnum testEnum;

		public final long pubRequests = 1;

		// adv
		public int adv;
		public final long advRequests = 1;

		@Override
		public String toString() {
			return "AggregationItem{date=" + date + ", hourOfDay=" + hourOfDay + ", pub=" + pub + ", pubRequests=" + pubRequests + ", adv=" + adv + ", advRequests=" + advRequests + '}';
		}
	}

	private static final Set<String> PUB_DIMENSIONS = Stream.of("date", "hourOfDay", "pub", "testEnum").collect(toSet());
	private static final Set<String> PUB_METRICS = Set.of("pubRequests");
	private static final Set<String> ADV_DIMENSIONS = union(PUB_DIMENSIONS, Set.of("adv"));
	private static final Set<String> ADV_METRICS = Set.of("advRequests");

	@SuppressWarnings("SameParameterValue")
	private static <T> Set<T> union(Set<T> a, Set<T> b) {
		Set<T> set = new HashSet<>(a);
		set.addAll(b);
		return set;
	}

	@Override
	protected StreamDataAcceptor<TestPubRequest> createSplitter(Context ctx) {
		return new StreamDataAcceptor<>() {
			private final AggregationItem outputItem = new AggregationItem();

			private final StreamDataAcceptor<AggregationItem> pubAggregator = ctx.addOutput(
					cube.logStreamConsumer(AggregationItem.class,
							keysToMap(PUB_DIMENSIONS.stream(), identity()),
							keysToMap(PUB_METRICS.stream(), identity())));

			private final StreamDataAcceptor<AggregationItem> advAggregator = ctx.addOutput(
					cube.logStreamConsumer(AggregationItem.class,
							keysToMap(ADV_DIMENSIONS.stream(), identity()),
							keysToMap(ADV_METRICS.stream(), identity())));

			@SuppressWarnings("ConstantConditions")
			@Override
			public void accept(TestPubRequest pubRequest) {
				outputItem.date = (int) (pubRequest.timestamp / (24 * 60 * 60 * 1000L));
				outputItem.hourOfDay = (byte) ((pubRequest.timestamp / (60 * 60 * 1000L)) % 24);
				outputItem.pub = pubRequest.pub;
				pubAggregator.accept(outputItem);
				for (TestAdvRequest remRequest : pubRequest.advRequests) {
					outputItem.adv = remRequest.adv;
					advAggregator.accept(outputItem);
				}
			}
		};
	}

}
