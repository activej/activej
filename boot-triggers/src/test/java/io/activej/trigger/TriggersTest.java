package io.activej.trigger;

import io.activej.common.ref.Ref;
import io.activej.common.ref.RefBoolean;
import io.activej.eventloop.Eventloop;
import io.activej.test.time.TestCurrentTimeProvider;
import io.activej.trigger.Triggers.TriggerWithResult;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.activej.trigger.Severity.HIGH;
import static org.junit.Assert.*;

public class TriggersTest {

	private Triggers triggers;
	private long timestamp;

	@Before
	public void setUp() {
		triggers = Triggers.builder()
				.withTrigger(Severity.HIGH, "Component", "nameOne", TriggerResult::create)
				.withTrigger(Severity.WARNING, "AnotherComponent", "nameTwo", TriggerResult::create)
				.withTrigger(Severity.DEBUG, "AnotherComponent", "name", TriggerResult::create)
				.withTrigger(Severity.DISASTER, "Component", "name", TriggerResult::create)
				.withTrigger(Severity.HIGH, "Component", "name", TriggerResult::create)
				.withTrigger(Severity.WARNING, "Component", "nameThree", TriggerResult::create)
				.withTrigger(Severity.HIGH, "ComponentName", "name", TriggerResult::create)
				.withTrigger(Severity.HIGH, "Component", "nameTwo", TriggerResult::create)
				.withTrigger(Severity.DISASTER, "AnotherComponent", "name", TriggerResult::create)
				.withTrigger(Severity.WARNING, "Component", "nameOne", TriggerResult::create)
				.withTrigger(Severity.INFORMATION, "Component", "name", TriggerResult::create)
				.build();
		timestamp = 10000;
	}

	@Test
	public void testAddTriggers() {
		triggers = Triggers.builder()
				.withTrigger(Severity.HIGH, "Component", "Name", TriggerResult::none)
				.build();
		List<String> triggers = this.triggers.getTriggers();
		assertEquals(1, triggers.size());
	}

	@Test
	public void testSuppressAllTriggers() {
		assertEquals(7, triggers.getResults().size());
		triggers.suppressAllTriggers();
		assertEquals(0, triggers.getResults().size());
	}

	@Test
	public void testDuplicateTriggers() {
		triggers.addTrigger(Severity.HIGH, "Component", "nameOne", TriggerResult::create);
		triggers.suppressTriggerByName("nameOne");
		triggers.getResults().forEach(result -> assertNotEquals("nameOne", result.getTrigger().getName()));
	}

	@Test
	public void testDuplicateTriggersResume() {
		RefBoolean condition = new RefBoolean(true);
		triggers = Triggers.builder()
				.withTrigger(Severity.HIGH, "Component", "nameOne", TriggerResult::create)
				.withTrigger(Severity.HIGH, "Component", "nameOne", TriggerResult::create)
				.withTrigger(Severity.HIGH, "Component", "nameOne", () -> {
					if (!condition.get()) {
						return TriggerResult.none();
					}
					return TriggerResult.create();
				})
				.build();
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(System.currentTimeMillis(), Triggers.CACHE_TIMEOUT.toMillis());

		assertEquals(3, triggers.getResults().size());
		triggers.suppressTriggerByName("nameOne");
		assertEquals(0, triggers.getResults().size());
		condition.set(false);
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(triggers.now.currentTimeMillis() + 10, 10);
		triggers.getResults();
		condition.set(true);
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(triggers.now.currentTimeMillis() + 1000, 10);
		assertEquals(1, triggers.getResults().size());
	}

	@Test
	public void testResumeTrigger() {
		RefBoolean condition = new RefBoolean(true);
		triggers = Triggers.builder()
				.withTrigger(Severity.HIGH, "Component", "nameOne", () -> {
					if (!condition.get()) {
						return TriggerResult.none();
					}
					return TriggerResult.create();
				})
				.build();
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(System.currentTimeMillis(), Triggers.CACHE_TIMEOUT.toMillis());
		List<TriggerWithResult> results = triggers.getResults();
		assertEquals(1, results.size());
		triggers.suppressTriggerByName("nameOne");
		assertEquals(0, triggers.getResults().size());
		condition.set(false);
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(triggers.now.currentTimeMillis() + 10, 10);
		triggers.getResults();
		condition.set(true);
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(triggers.now.currentTimeMillis() + 1000, 10);
		assertEquals(1, triggers.getResults().size());
	}

	@Test
	public void testSuppressByName() {
		assertEquals(11, triggers.getTriggers().size());
		triggers.suppressTriggerByName("nameOne");
		triggers.getResults().forEach(result -> assertNotEquals("nameOne", result.getTrigger().getName()));
	}

	@Test
	public void testSuppressByComponent() {
		assertEquals(11, triggers.getTriggers().size());
		triggers.suppressTriggerByComponent("Component");
		triggers.getResults().forEach(result -> assertNotEquals("Component", result.getTrigger().getComponent()));
	}

	@Test
	public void testSuppressBySeverity() {
		assertEquals(11, triggers.getTriggers().size());
		triggers.suppressTriggerBySeverity("HIGH");
		triggers.getResults().forEach(result -> assertNotEquals("nameOne", result.getTrigger().getSeverity().name()));
	}

	@Test
	public void testTriggersSuppression() {
		initializeTriggers();

		triggers.suppressTriggerByName("fatalErrors");
		triggers.suppressTriggerByName("delay");
		triggers.suppressTriggerByName("error");
		triggers.suppressTriggerByName("errors");
		triggers.suppressTriggerByName("errorProcessLogs");
		triggers.suppressTriggerByName("runDelay");

		assertTrue(triggers.getResults().isEmpty());
	}

	@Test
	public void testSuppressBySignature() {
		initializeTriggers();

		triggers.suppressTriggersBySignature("HIGH : @CubeThread Eventloop : fatalErrors");
		triggers.suppressTriggersBySignature("HIGH : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : delay");
		triggers.suppressTriggersBySignature("HIGH : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : error");
		triggers.suppressTriggersBySignature("HIGH : @Named(\"LogProcessorScheduler\") EventloopTaskScheduler : delay");
		triggers.suppressTriggersBySignature("HIGH : @Named(\"LogProcessorScheduler\") EventloopTaskScheduler : error");
		triggers.suppressTriggersBySignature("HIGH : Cube : errors");
		triggers.suppressTriggersBySignature("AVERAGE : CubeLogProcessorController : errorProcessLogs");
		triggers.suppressTriggersBySignature("AVERAGE : Launcher : runDelay");
		triggers.suppressTriggersBySignature("WARNING : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : delay");
		triggers.suppressTriggersBySignature("WARNING : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : error");
		triggers.suppressTriggersBySignature("WARNING : @Named(\"LogProcessorScheduler\") EventloopTaskScheduler : delay");
		triggers.suppressTriggersBySignature("WARNING : @Named(\"LogProcessorScheduler\") EventloopTaskScheduler : error");

		assertTrue(triggers.getResults().isEmpty());
	}

	@Test
	public void testGetMultilineSuppressedResults() {
		initializeTriggers();
		triggers.suppressTriggersBySignature("HIGH : @CubeThread Eventloop : fatalErrors");
		triggers.suppressTriggersBySignature("HIGH : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : delay");
		triggers.suppressTriggersBySignature("HIGH : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : error");

		assertEquals("""
						HIGH : @CubeThread Eventloop : fatalErrors
						HIGH : @Named("CubeFetchScheduler") EventloopTaskScheduler : delay
						HIGH : @Named("CubeFetchScheduler") EventloopTaskScheduler : error""",
				triggers.getMultilineSuppressedResults());

		String resultsAfterSuppression = triggers.getMultilineResults();
		Arrays.stream(triggers.getMultilineSuppressedResults().split("\n"))
				.forEach(suppressed -> assertFalse(resultsAfterSuppression.contains(suppressed)));
	}

	@Test
	public void testWithoutTimestamp() {
		triggers = Triggers.builder()
				.withTrigger(HIGH, Eventloop.class.getName(), "ProcessDelay", TriggerResult::create)
				.build();
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(timestamp, Triggers.CACHE_TIMEOUT.toMillis() + 1);

		long currentTimestamp = timestamp;
		List<TriggerWithResult> results = triggers.getResults();
		assertEquals(1, results.size());
		assertEquals(currentTimestamp, results.get(0).getTriggerResult().getTimestamp());

		results = triggers.getResults();
		assertEquals(1, results.size());
		assertEquals(currentTimestamp, results.get(0).getTriggerResult().getTimestamp());
	}

	@Test
	public void testOfTimestamp() {
		triggers = Triggers.builder()
				.withTrigger(HIGH, Eventloop.class.getName(), "ProcessDelay", () -> TriggerResult.ofTimestamp(increaseTimestampAndGet()))
				.build();
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(timestamp, Triggers.CACHE_TIMEOUT.toMillis() + 1);

		long currentTimestamp = timestamp;
		List<TriggerWithResult> results = triggers.getResults();
		assertEquals(1, results.size());
		assertEquals(currentTimestamp + 10000, results.get(0).getTriggerResult().getTimestamp());

		results = triggers.getResults();
		assertEquals(1, results.size());
		assertEquals(currentTimestamp + 20000, results.get(0).getTriggerResult().getTimestamp());
	}

	@Test
	public void testOfInstant() {
		triggers = Triggers.builder()
				.withTrigger(HIGH, Eventloop.class.getName(), "ProcessDelay", () -> TriggerResult.ofInstant(Instant.ofEpochMilli(increaseTimestampAndGet())))
				.build();
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(timestamp, Triggers.CACHE_TIMEOUT.toMillis() + 1);

		long currentTimestamp = timestamp;
		List<TriggerWithResult> results = triggers.getResults();
		assertEquals(1, results.size());
		assertEquals(currentTimestamp + 10000, results.get(0).getTriggerResult().getTimestamp());

		results = triggers.getResults();
		assertEquals(1, results.size());
		assertEquals(currentTimestamp + 20000, results.get(0).getTriggerResult().getTimestamp());
	}

	@Test
	public void testOfValueWithPredicate() {
		triggers = Triggers.builder()
				.withTrigger(HIGH, Eventloop.class.getName(), "ProcessDelay", () -> TriggerResult.ofValue(increaseTimestampAndGet(), time -> time > 1000))
				.build();
		int increment = 100;
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(timestamp, Triggers.CACHE_TIMEOUT.toMillis() + increment);

		long currentTimestamp = timestamp;
		List<TriggerWithResult> results = triggers.getResults();

		assertEquals(1, results.size());
		assertEquals(currentTimestamp, results.get(0).getTriggerResult().getTimestamp());
		assertEquals(currentTimestamp + 10000, results.get(0).getTriggerResult().getValue());

		results = triggers.getResults();
		assertEquals(1, results.size());
		assertEquals(currentTimestamp, results.get(0).getTriggerResult().getTimestamp());
		assertEquals(currentTimestamp + 20000, results.get(0).getTriggerResult().getValue());
	}

	@Test
	public void testConcurrentAccess() throws InterruptedException {
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(100, Triggers.CACHE_TIMEOUT.toMillis() * 2);
		int nThreads = 10;
		List<Thread> threads = new ArrayList<>(nThreads);
		Ref<Throwable> throwableRef = new Ref<>();
		for (int i = 0; i < nThreads; i++) {
			threads.add(new Thread(() -> {
				try {
					for (int j = 0; j < 1_000; j++) {
						if (throwableRef.get() != null) {
							break;
						}
						triggers.getMultilineResultsHigh();
					}
				} catch (Throwable e) {
					throwableRef.set(e);
				}
			}));
		}

		threads.forEach(Thread::start);
		for (Thread thread : threads) {
			thread.join();
		}
		assertNull(throwableRef.get());
	}

	@Test
	public void testEmptyNewResults() {
		Triggers triggers = Triggers.create();
		triggers.now = TestCurrentTimeProvider.ofTimeSequence(10_000, Triggers.CACHE_TIMEOUT.toMillis() * 2);

		for (int i = 0; i < 10; i++) {
			RefBoolean s = new RefBoolean(false);
			triggers.addTrigger(Severity.HIGH, "Component", "name" + i, () -> s.flip() ?
					TriggerResult.create() :
					TriggerResult.none());
		}

		assertEquals(10, triggers.getResultsHigh().size());
		assertTrue(triggers.getResultsHigh().isEmpty());
	}

	private long increaseTimestampAndGet() {
		timestamp += 10000;
		return timestamp;
	}

	private void initializeTriggers() {
		triggers = Triggers.builder()
				.withTrigger(Severity.HIGH, "@CubeThread Eventloop", "fatalErrors", TriggerResult::create)
				.withTrigger(Severity.HIGH, "@Named(\"CubeFetchScheduler\") EventloopTaskScheduler", "delay", TriggerResult::create)
				.withTrigger(Severity.HIGH, "@Named(\"CubeFetchScheduler\") EventloopTaskScheduler", "error", TriggerResult::create)
				.withTrigger(Severity.HIGH, "@Named(\"LogProcessorScheduler\") EventloopTaskScheduler", "delay", TriggerResult::create)
				.withTrigger(Severity.HIGH, "@Named(\"LogProcessorScheduler\") EventloopTaskScheduler", "error", TriggerResult::create)
				.withTrigger(Severity.HIGH, "Cube", "errors", TriggerResult::create)
				.withTrigger(Severity.AVERAGE, "CubeLogProcessorController", "errorProcessLogs", TriggerResult::create)
				.withTrigger(Severity.AVERAGE, "Launcher", "runDelay", TriggerResult::create)
				.withTrigger(Severity.WARNING, "@Named(\"CubeFetchScheduler\") EventloopTaskScheduler", "delay", TriggerResult::create)
				.withTrigger(Severity.WARNING, "@Named(\"CubeFetchScheduler\") EventloopTaskScheduler", "error", TriggerResult::create)
				.withTrigger(Severity.WARNING, "@Named(\"LogProcessorScheduler\") EventloopTaskScheduler", "delay", TriggerResult::create)
				.withTrigger(Severity.WARNING, "@Named(\"LogProcessorScheduler\") EventloopTaskScheduler", "error", TriggerResult::create)
				.build();
		assertEquals(
				"@CubeThread Eventloop, " +
						"@Named(\"CubeFetchScheduler\") EventloopTaskScheduler, " +
						"@Named(\"LogProcessorScheduler\") EventloopTaskScheduler, " +
						"Cube, " +
						"CubeLogProcessorController, " +
						"Launcher",
				triggers.getTriggerComponents());
		assertEquals("[" +
						"@CubeThread Eventloop : fatalErrors, " +
						"@Named(\"CubeFetchScheduler\") EventloopTaskScheduler : delay, " +
						"@Named(\"CubeFetchScheduler\") EventloopTaskScheduler : error, " +
						"@Named(\"LogProcessorScheduler\") EventloopTaskScheduler : delay, " +
						"@Named(\"LogProcessorScheduler\") EventloopTaskScheduler : error, " +
						"Cube : errors, " +
						"CubeLogProcessorController : errorProcessLogs, " +
						"Launcher : runDelay]",
				triggers.getTriggerNames().toString());
		assertEquals("[" +
						"HIGH : @CubeThread Eventloop : fatalErrors, " +
						"HIGH : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : delay, " +
						"HIGH : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : error, " +
						"HIGH : @Named(\"LogProcessorScheduler\") EventloopTaskScheduler : delay, " +
						"HIGH : @Named(\"LogProcessorScheduler\") EventloopTaskScheduler : error, " +
						"HIGH : Cube : errors, " +
						"AVERAGE : CubeLogProcessorController : errorProcessLogs, " +
						"AVERAGE : Launcher : runDelay, " +
						"WARNING : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : delay, " +
						"WARNING : @Named(\"CubeFetchScheduler\") EventloopTaskScheduler : error, " +
						"WARNING : @Named(\"LogProcessorScheduler\") EventloopTaskScheduler : delay, " +
						"WARNING : @Named(\"LogProcessorScheduler\") EventloopTaskScheduler : error]",
				triggers.getTriggers().toString());
		assertEquals(8, triggers.getResults().size());
	}
}
