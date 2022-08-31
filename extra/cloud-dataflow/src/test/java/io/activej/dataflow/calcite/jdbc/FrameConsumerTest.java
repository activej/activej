package io.activej.dataflow.calcite.jdbc;

import io.activej.common.ref.RefLong;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.test.rules.EventloopRule;
import org.apache.calcite.avatica.Meta;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static io.activej.promise.TestUtils.await;
import static org.junit.Assert.assertEquals;

public class FrameConsumerTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private static final RecordScheme SCHEME = RecordScheme.create()
			.withField("id", long.class)
			.build();

	@Test
	public void test0Frames() {
		doTest(0);
	}

	@Test
	public void test1Frame() {
		doTest(1);
	}

	@Test
	public void test1KFrames() {
		doTest(1000);
	}

	@Test
	public void test10KFrames() {
		doTest(10_000);
	}

	@Test
	public void test100KFrames() {
		doTest(100_000);
	}

	@Test
	public void test1MFrames() {
		doTest(1_000_000);
	}

	@Test
	public void test10MFrames() {
		doTest(10_000_000);
	}

	@Test
	@Ignore("Takes too long")
	public void test100MFrames() {
		doTest(100_000_000);
	}

	@Test
	@Ignore("Takes too long")
	public void test1BFrames() {
		doTest(1_000_000_000);
	}

	private void doTest(long maxCount) {
		AtomicLong countRef = new AtomicLong(0);
		RefLong idRef = new RefLong(0);

		StreamSupplier<Record> recordSupplier = StreamSupplier.ofStream(Stream.generate(() -> {
					Record record = SCHEME.record();
					record.set("id", ++idRef.value);
					return record;
				})
				.limit(maxCount));

		FrameConsumer frameConsumer = new FrameConsumer(SCHEME.size());

		Promise<Void> streamPromise = recordSupplier.streamTo(frameConsumer);

		Eventloop eventloop = Eventloop.getCurrentEventloop();
		eventloop.keepAlive(true);

		Thread readThread = new Thread(() -> {
			ThreadLocalRandom random = ThreadLocalRandom.current();
			while (true) {
				int maxRows = random.nextBoolean() ?
						-1 :
						random.nextInt(1000);

				Meta.Frame frame = frameConsumer.fetch(countRef.get(), maxRows);

				for (Object row : frame.rows) {
					Object[] rowArray = (Object[]) row;
					long id = (long) rowArray[0];
					assertEquals(id, countRef.incrementAndGet());

					if (countRef.get() % 1_000_000 == 0) {
						System.out.println(countRef.get());
					}
				}

				if (frame.done) {
					eventloop.keepAlive(false);
					break;
				}
			}
		});
		readThread.start();

		await(streamPromise);

		try {
			readThread.join();
		} catch (InterruptedException e) {
			throw new AssertionError(e);
		}
		assertEquals(maxCount, countRef.get());
	}
}
