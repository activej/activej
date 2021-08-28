package io.activej.datastream;

import io.activej.datastream.processor.StreamTransformer;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestUtils {
	public static void assertEndOfStream(StreamSupplier<?> streamSupplier) {
		assertTrue(streamSupplier.isResult());
	}

	public static void assertEndOfStream(StreamConsumer<?> streamConsumer) {
		assertTrue(streamConsumer.isResult());
	}

	public static void assertEndOfStream(StreamSupplier<?> streamSupplier, StreamConsumer<?> streamConsumer) {
		assertEndOfStream(streamSupplier);
		assertEndOfStream(streamConsumer);
	}

	public static void assertClosedWithError(StreamSupplier<?> streamSupplier) {
		assertTrue(streamSupplier.isException());
	}

	public static void assertClosedWithError(Exception exception, StreamSupplier<?> streamSupplier) {
		assertSame(exception, streamSupplier.getEndOfStream().getException());
	}

	public static void assertClosedWithError(Class<? extends Exception> exceptionType, StreamSupplier<?> streamSupplier) {
		assertThat(streamSupplier.getEndOfStream().getException(), instanceOf(exceptionType));
	}

	public static void assertClosedWithError(StreamConsumer<?> streamConsumer) {
		assertTrue(streamConsumer.isException());
	}

	public static void assertClosedWithError(Exception exception, StreamConsumer<?> streamConsumer) {
		assertSame(exception, streamConsumer.getAcknowledgement().getException());
	}

	public static void assertClosedWithError(Class<? extends Exception> exceptionType, StreamConsumer<?> streamConsumer) {
		assertThat(streamConsumer.getAcknowledgement().getException(), instanceOf(exceptionType));
	}

	public static void assertClosedWithError(Exception exception, StreamSupplier<?> streamSupplier, StreamConsumer<?> streamConsumer) {
		assertSame(exception, streamSupplier.getEndOfStream().getException());
		assertSame(exception, streamConsumer.getAcknowledgement().getException());
	}

	public static void assertClosedWithError(Class<? extends Exception> exceptionType, StreamSupplier<?> streamSupplier, StreamConsumer<?> streamConsumer) {
		assertThat(streamSupplier.getEndOfStream().getException(), instanceOf(exceptionType));
		assertThat(streamConsumer.getAcknowledgement().getException(), instanceOf(exceptionType));
	}

	public static void assertSuppliersEndOfStream(List<? extends StreamSupplier<?>> streamSuppliers) {
		assertTrue(streamSuppliers.stream().allMatch(StreamSupplier::isResult));
	}

	public static void assertConsumersEndOfStream(List<? extends StreamConsumer<?>> streamConsumers) {
		assertTrue(streamConsumers.stream().allMatch(StreamConsumer::isResult));
	}

	public static void assertSuppliersClosedWithError(List<? extends StreamSupplier<?>> streamSuppliers) {
		assertTrue(streamSuppliers.stream().allMatch(StreamSupplier::isException));
	}

	public static void assertConsumersClosedWithError(List<? extends StreamConsumer<?>> streamConsumers) {
		assertTrue(streamConsumers.stream().allMatch(StreamConsumer::isException));
	}

	public static void assertEndOfStream(StreamTransformer<?, ?> streamTransformer) {
		assertTrue(streamTransformer.getInput().isResult());
		assertTrue(streamTransformer.getOutput().isResult());
	}

	public static void assertClosedWithError(StreamTransformer<?, ?> streamTransformer) {
		assertTrue(streamTransformer.getInput().isException());
		assertTrue(streamTransformer.getOutput().isException());
	}

	public static void assertClosedWithError(Exception exception, StreamTransformer<?, ?> streamTransformer) {
		assertSame(exception, streamTransformer.getInput().getAcknowledgement().getException());
		assertSame(exception, streamTransformer.getOutput().getEndOfStream().getException());
	}

	public static class CountingStreamConsumer<T> extends AbstractStreamConsumer<T> {
		private int count;

		@Override
		protected void onStarted() {
			resume(this::accept);
		}

		private void accept(T item) {
			count++;
		}

		@Override
		protected void onEndOfStream() {
			acknowledge();
		}

		public int getCount() {
			return count;
		}
	}

}
