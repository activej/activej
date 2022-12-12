package io.activej.datastream.processor;

import io.activej.datastream.StreamConsumer;
import io.activej.datastream.StreamConsumerToList;
import io.activej.datastream.StreamSupplier;
import io.activej.datastream.processor.StreamLeftJoin.ValueLeftJoiner;
import io.activej.promise.Promise;
import io.activej.test.ExpectedException;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.activej.datastream.TestStreamTransformers.decorate;
import static io.activej.datastream.TestStreamTransformers.oneByOne;
import static io.activej.datastream.TestUtils.assertClosedWithError;
import static io.activej.datastream.TestUtils.assertEndOfStream;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class StreamLeftJoinTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void test1() {
		StreamSupplier<DataItemMaster> source1 = StreamSupplier.of(
				new DataItemMaster(10, 10, "masterA"),
				new DataItemMaster(20, 10, "masterB"),
				new DataItemMaster(25, 15, "masterB+"),
				new DataItemMaster(30, 20, "masterC"),
				new DataItemMaster(40, 20, "masterD"));

		StreamSupplier<DataItemDetail> source2 = StreamSupplier.of(
				new DataItemDetail(10, "detailX"),
				new DataItemDetail(20, "detailY"));

		StreamLeftJoin<Integer, DataItemMaster, DataItemDetail, DataItemMasterDetail> streamLeftJoin =
				StreamLeftJoin.create(Integer::compareTo,
						input -> input.detailId,
						input -> input.id,
						new ValueLeftJoiner<>() {
							@Override
							public DataItemMasterDetail doInnerJoin(Integer key, DataItemMaster left, DataItemDetail right) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, right.detail);
							}

							@Override
							public DataItemMasterDetail doOuterJoin(Integer key, DataItemMaster left) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, null);
							}
						}
				);

		StreamConsumerToList<DataItemMasterDetail> consumer = StreamConsumerToList.create();

		await(
				source1.streamTo(streamLeftJoin.getLeft()),
				source2.streamTo(streamLeftJoin.getRight()),
				streamLeftJoin.getOutput().streamTo(
						consumer.transformWith(oneByOne()))
		);

		assertEquals(List.of(
						new DataItemMasterDetail(10, 10, "masterA", "detailX"),
						new DataItemMasterDetail(20, 10, "masterB", "detailX"),
						new DataItemMasterDetail(25, 15, "masterB+", null),
						new DataItemMasterDetail(30, 20, "masterC", "detailY"),
						new DataItemMasterDetail(40, 20, "masterD", "detailY")
				),
				consumer.getList());
		assertEndOfStream(source1);
		assertEndOfStream(source2);
	}

	@Test
	public void testLeftJoinSingle() {
		StreamSupplier<DataItemMaster> source1 = StreamSupplier.of(
				new DataItemMaster(25, 5, "master")
		);
		StreamSupplier<DataItemDetail> source2 = StreamSupplier.of();

		StreamLeftJoin<Integer, DataItemMaster, DataItemDetail, DataItemMasterDetail> streamJoin =
				StreamLeftJoin.create(Integer::compareTo,
						input -> input.detailId,
						input -> input.id,
						new ValueLeftJoiner<>() {
							@Override
							public DataItemMasterDetail doInnerJoin(Integer key, DataItemMaster left, DataItemDetail right) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, right.detail);
							}

							@Override
							public DataItemMasterDetail doOuterJoin(Integer key, DataItemMaster left) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, null);
							}
						}
				);

		StreamConsumerToList<DataItemMasterDetail> consumer = StreamConsumerToList.create();

		await(
				source1.streamTo(streamJoin.getLeft()),
				source2.streamTo(streamJoin.getRight()),
				streamJoin.getOutput().streamTo(
						consumer.transformWith(oneByOne()))
		);

		assertEquals(List.of(new DataItemMasterDetail(25, 5, "master", null)),
				consumer.getList());

		assertEndOfStream(source1);
		assertEndOfStream(source2);
	}

	@Test
	public void testLeftOuterJoinSingleLeft() {
		StreamSupplier<DataItemMaster> source1 = StreamSupplier.of(
				new DataItemMaster(25, 5, "master")
		);
		StreamSupplier<DataItemDetail> source2 = StreamSupplier.of();

		StreamLeftJoin<Integer, DataItemMaster, DataItemDetail, DataItemMasterDetail> streamLeftJoin =
				StreamLeftJoin.create(Integer::compareTo,
						input -> input.detailId,
						input -> input.id,
						new ValueLeftJoiner<>() {
							@Override
							public DataItemMasterDetail doInnerJoin(Integer key, DataItemMaster left, DataItemDetail right) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, right.detail);
							}

							@Override
							public DataItemMasterDetail doOuterJoin(Integer key, DataItemMaster left) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, null);
							}
						}
				);

		StreamConsumerToList<DataItemMasterDetail> consumer = StreamConsumerToList.create();

		await(
				source1.streamTo(streamLeftJoin.getLeft()),
				source2.streamTo(streamLeftJoin.getRight()),
				streamLeftJoin.getOutput().streamTo(
						consumer.transformWith(oneByOne()))
		);

		assertEquals(singletonList(new DataItemMasterDetail(25, 5, "master", null)),
				consumer.getList());

		assertEndOfStream(source1);
		assertEndOfStream(source2);
	}

	@Test
	public void testWithError() {
		List<DataItemMasterDetail> list = new ArrayList<>();

		StreamSupplier<DataItemMaster> source1 = StreamSupplier.of(
				new DataItemMaster(10, 10, "masterA"),
				new DataItemMaster(20, 10, "masterB"),
				new DataItemMaster(25, 15, "masterB+"),
				new DataItemMaster(30, 20, "masterC"),
				new DataItemMaster(40, 20, "masterD"));

		StreamSupplier<DataItemDetail> source2 = StreamSupplier.of(
				new DataItemDetail(10, "detailX"),
				new DataItemDetail(20, "detailY"));

		StreamLeftJoin<Integer, DataItemMaster, DataItemDetail, DataItemMasterDetail> streamLeftJoin =
				StreamLeftJoin.create(Integer::compareTo,
						input -> input.detailId,
						input -> input.id,
						new ValueLeftJoiner<>() {
							@Override
							public DataItemMasterDetail doInnerJoin(Integer key, DataItemMaster left, DataItemDetail right) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, right.detail);
							}

							@Override
							public DataItemMasterDetail doOuterJoin(Integer key, DataItemMaster left) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, null);
							}
						}
				);

		ExpectedException exception = new ExpectedException("Test Exception");
		StreamConsumerToList<DataItemMasterDetail> consumerToList = StreamConsumerToList.create(list);
		StreamConsumer<DataItemMasterDetail> consumer = consumerToList
				.transformWith(decorate(promise ->
						promise.then(item -> Promise.ofException(exception))));

		Exception e = awaitException(
				source1.streamTo(streamLeftJoin.getLeft()),
				source2.streamTo(streamLeftJoin.getRight()),
				streamLeftJoin.getOutput().streamTo(consumer)
		);

		assertSame(exception, e);
		assertEquals(1, list.size());
		assertClosedWithError(source1);
		assertClosedWithError(source2);
	}

	@Test
	public void testSupplierWithError() {
		ExpectedException exception = new ExpectedException("Test Exception");
		StreamSupplier<DataItemMaster> source1 = StreamSupplier.concat(
				StreamSupplier.of(new DataItemMaster(10, 10, "masterA")),
				StreamSupplier.closingWithError(exception),
				StreamSupplier.of(new DataItemMaster(20, 10, "masterB")),
				StreamSupplier.of(new DataItemMaster(25, 15, "masterB+")),
				StreamSupplier.of(new DataItemMaster(30, 20, "masterC")),
				StreamSupplier.of(new DataItemMaster(40, 20, "masterD"))
		);

		StreamSupplier<DataItemDetail> source2 = StreamSupplier.concat(
				StreamSupplier.of(new DataItemDetail(10, "detailX")),
				StreamSupplier.of(new DataItemDetail(20, "detailY")),
				StreamSupplier.closingWithError(exception)
		);

		StreamLeftJoin<Integer, DataItemMaster, DataItemDetail, DataItemMasterDetail> streamLeftJoin =
				StreamLeftJoin.create(Integer::compareTo,
						input -> input.detailId,
						input -> input.id,
						new ValueLeftJoiner<>() {
							@Override
							public DataItemMasterDetail doInnerJoin(Integer key, DataItemMaster left, DataItemDetail right) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, right.detail);
							}

							@Override
							public DataItemMasterDetail doOuterJoin(Integer key, DataItemMaster left) {
								return new DataItemMasterDetail(left.id, left.detailId, left.master, null);
							}
						}
				);

		List<DataItemMasterDetail> list = new ArrayList<>();
		StreamConsumer<DataItemMasterDetail> consumer = StreamConsumerToList.create(list);

		Exception e = awaitException(
				source1.streamTo(streamLeftJoin.getLeft()),
				source2.streamTo(streamLeftJoin.getRight()),
				streamLeftJoin.getOutput().streamTo(consumer.transformWith(oneByOne()))
		);

		assertSame(exception, e);
		assertEquals(0, list.size());
		assertClosedWithError(source1);
		assertClosedWithError(source2);
	}

	private record DataItemMaster(int id, int detailId, String master) {}

	private record DataItemDetail(int id, String detail) {}

	private record DataItemMasterDetail(int id, int detailId, String master, String detail) {}
}
