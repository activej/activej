package io.activej.etcd;

import io.activej.common.time.CurrentTimeProvider;
import io.activej.etcd.codec.key.EtcdKeyEncoder;
import io.activej.etcd.codec.kv.EtcdKVDecoder;
import io.activej.etcd.codec.kv.EtcdKVEncoder;
import io.activej.etcd.codec.kv.KeyValue;
import io.activej.etcd.codec.value.EtcdValueCodec;
import io.activej.etcd.codec.value.EtcdValueCodecs;
import io.activej.etcd.codec.value.EtcdValueEncoder;
import io.activej.etcd.exception.EtcdException;
import io.activej.etcd.exception.MalformedEtcdDataException;
import io.activej.etcd.exception.NoKeyFoundException;
import io.activej.etcd.exception.TransactionNotSucceededException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Response;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;

import static io.activej.common.Checks.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

public class EtcdUtils {

	public static final EtcdValueCodec<Long> TOUCH_TIMESTAMP_CODEC = EtcdValueCodecs.ofLongString();

	private static final String DEADLINE_EXCEEDED_STATUS_MESSAGE = "context deadline exceeded";

	public static ByteSequence byteSequenceFrom(char ch) {
		checkArgument(ch <= 0x7f);
		return byteSequenceFrom((byte) ch);
	}

	public static ByteSequence byteSequenceFrom(byte b) {
		return ByteSequence.from(new byte[]{b});
	}

	public static ByteSequence byteSequenceFrom(String string) {
		return ByteSequence.from(string, UTF_8);
	}

	public record CheckoutResponse<T>(io.etcd.jetcd.Response.Header header, T response) {}

	public record CheckoutRequest<KV, R>(ByteSequence prefix, EtcdKVDecoder<?, ? extends KV> codec, Collector<KV, ?, ? extends R> collector) {

		public static <K, V, R> CheckoutRequest<Map.Entry<K, V>, R> ofMapEntry(
			ByteSequence prefix, EtcdKVDecoder<?, ? extends Map.Entry<K, V>> codec, Collector<Map.Entry<? extends K, ? extends V>, ?, ? extends R> collector
		) {
			//noinspection unchecked,rawtypes
			return CheckoutRequest.<Map.Entry<K, V>, R>of(prefix, codec, (Collector) collector);
		}

		public static <KV, R> CheckoutRequest<KV, R> of(
			ByteSequence prefix, EtcdKVDecoder<?, ? extends KV> codec, Collector<KV, ?, ? extends R> collector
		) {
			return new CheckoutRequest<>(prefix, codec, collector);
		}

	}

	public static <T, A, R> CompletableFuture<CheckoutResponse<R>> checkout(KV client, long revision,
		ByteSequence prefix, EtcdKVDecoder<?, T> codec, Collector<T, A, R> collector
	) {
		//noinspection unchecked
		return checkout(client, revision, new CheckoutRequest[]{new CheckoutRequest<>(prefix, codec, collector)},
			(header, objects) -> new CheckoutResponse<>(header, (R) objects[0]));
	}

	public interface CheckoutFinisher<T> {
		T finish(Response.Header header, Object[] objects) throws MalformedEtcdDataException;
	}

	public static <R> CompletableFuture<R> checkout(KV client, long revision,
		CheckoutRequest<?, ?>[] checkoutRequests, CheckoutFinisher<R> checkoutFinisher
	) {
		return client.txn()
			.Then(Arrays.stream(checkoutRequests)
				.map(checkoutRequest -> Op.get(
					checkoutRequest.prefix,
					GetOption.builder()
						.isPrefix(true)
						.withRevision(revision)
						.build()))
				.toArray(Op[]::new))
			.commit()
			.exceptionallyCompose(e -> failedFuture(new EtcdException("Checkout failed", convertStatusException(e.getCause()))))
			.thenCompose(new Function<TxnResponse, CompletionStage<R>>() {
				@Override
				public CompletionStage<R> apply(TxnResponse response) {
					try {
						Object[] result = new Object[checkoutRequests.length];
						for (int i = 0; i < checkoutRequests.length; i++) {
							//noinspection unchecked
							CheckoutRequest<Object, Object> checkoutRequest = (CheckoutRequest<Object, Object>) checkoutRequests[i];
							GetResponse getResponse = response.getGetResponses().get(i);
							result[i] = getCheckoutResult(checkoutRequest.prefix, checkoutRequest.codec, checkoutRequest.collector, getResponse);
						}
						return completedFuture(checkoutFinisher.finish(response.getHeader(), result));
					} catch (MalformedEtcdDataException e) {
						return failedFuture(e);
					}
				}

				static <KV, A, R> R getCheckoutResult(ByteSequence prefix, EtcdKVDecoder<?, ? extends KV> codec, Collector<KV, A, R> collector, GetResponse response) throws MalformedEtcdDataException {
					A accumulator = collector.supplier().get();
					BiConsumer<A, ? super KV> accumulatorFn = collector.accumulator();
					for (var kv : response.getKvs()) {
						KV item;
						ByteSequence key = kv.getKey();
						try {
							item = codec.decodeKV(new KeyValue(key.substring(prefix.size()), kv.getValue()));
						} catch (MalformedEtcdDataException e) {
							throw new MalformedEtcdDataException("Failed to decode KV of key '" + key + '\'', e);
						}
						accumulatorFn.accept(accumulator, item);
					}
					return collector.finisher().apply(accumulator);
				}
			});
	}

	public static <K, KV, R> Watch.Watcher watch(Watch watch, long revision,
		ByteSequence prefix, EtcdKVDecoder<? extends K, ? extends KV> codec, EtcdEventProcessor<K, KV, R> eventProcessor,
		EtcdListener<R> listener
	) {
		return watch(watch, revision,
			new WatchRequest[]{new WatchRequest<>(prefix, codec, eventProcessor)},
			new EtcdListener<>() {
				@Override
				public void onConnectionEstablished() {
					listener.onConnectionEstablished();
				}

				@Override
				public void onNext(long revision, Object[] operations) throws MalformedEtcdDataException {
					assert operations.length == 1;
					//noinspection unchecked
					listener.onNext(revision, (R) operations[0]);
				}

				@Override
				public void onError(Throwable throwable) {
					listener.onError(throwable);
				}

				@Override
				public void onCompleted() {
					listener.onCompleted();
				}
			});
	}

	public record WatchRequest<K, KV, R>(ByteSequence prefix, EtcdKVDecoder<? extends K, ? extends KV> codec, EtcdEventProcessor<K, KV, ? extends R> eventProcessor) {

		public static <K, V, R> WatchRequest<K, Map.Entry<K, V>, R> ofMapEntry(
			ByteSequence prefix, EtcdKVDecoder<? extends K, ? extends Map.Entry<K, V>> codec, EtcdEventProcessor<K, Map.Entry<K, V>, ? extends R> eventProcessor
		) {
			return of(prefix, codec, eventProcessor);
		}

		public static <K, KV, R> WatchRequest<K, KV, R> of(
			ByteSequence prefix, EtcdKVDecoder<? extends K, ? extends KV> codec, EtcdEventProcessor<K, KV, ? extends R> eventProcessor
		) {
			return new WatchRequest<>(prefix, codec, eventProcessor);
		}

	}

	public static Watch.Watcher watch(Watch client, long revision,
		WatchRequest<?, ?, ?>[] watchRequests,
		EtcdListener<Object[]> listener
	) {
		byte[][] prefixes = Arrays.stream(watchRequests).map(WatchRequest::prefix).map(ByteSequence::getBytes).toArray(byte[][]::new);
		int rootSize = 0;
		LOOP:
		for (; rootSize < prefixes[0].length; rootSize++) {
			for (int i = 1; i < prefixes.length; i++) {
				if (rootSize > prefixes[i].length || prefixes[0][rootSize] != prefixes[i][rootSize]) {
					break LOOP;
				}
			}
		}
		ByteSequence root = ByteSequence.from(Arrays.copyOf(prefixes[0], rootSize));

		//noinspection rawtypes,unchecked
		WatchRequest[] requests = Arrays.stream(watchRequests)
			.map(w -> new WatchRequest(w.prefix.substring(root.size()), w.codec, w.eventProcessor))
			.toArray(WatchRequest[]::new);

		return client.watch(
			root,
			WatchOption.builder()
				.isPrefix(true)
				.withRevision(revision)
				.withCreateNotify(true)
				.build(),
			new Watch.Listener() {
				long currentRevision = -1;

				@Override
				public void onNext(WatchResponse response) {
					if (response.isCreatedNotify()) {
						listener.onConnectionEstablished();
					}
					try {
						Object[] accumulators = new Object[requests.length];

						boolean firstRun = true;
						for (var event : response.getEvents()) {
							var keyValue = event.getKeyValue();
							var rootKey = keyValue.getKey().substring(root.size());

							long modRevision = keyValue.getModRevision();
							if (modRevision != currentRevision) {
								if (!firstRun) listener.onNext(currentRevision, accumulators);

								for (int i = 0; i < accumulators.length; i++) {
									accumulators[i] = requests[i].eventProcessor.createEventsAccumulator();
								}
								currentRevision = modRevision;
							}

							for (int i = 0; i < accumulators.length; i++) {
								var request = requests[i];
								if (!rootKey.startsWith(request.prefix)) continue;
								var key = rootKey.substring(request.prefix.size());
								if (event.getEventType() == WatchEvent.EventType.PUT) {
									Object kv;
									try {
										kv = request.codec.decodeKV(new KeyValue(key, keyValue.getValue()));
									} catch (MalformedEtcdDataException e) {
										throw new MalformedEtcdDataException("Failed to decode KV of key '" + rootKey + '\'', e);
									}
									//noinspection unchecked
									request.eventProcessor.onPut(accumulators[i], kv);
								}
								if (event.getEventType() == WatchEvent.EventType.DELETE) {
									Object k;
									try {
										k = request.codec.decodeKey(key);
									} catch (MalformedEtcdDataException e) {
										throw new MalformedEtcdDataException("Failed to decode key '" + rootKey + '\'', e);
									}
									//noinspection unchecked
									request.eventProcessor.onDelete(accumulators[i], k);
								}
							}
							firstRun = false;
						}
						if (currentRevision != -1) {
							listener.onNext(currentRevision, accumulators);
						}
					} catch (MalformedEtcdDataException e) {
						onError(e);
					}
				}

				@Override
				public void onError(Throwable throwable) {
					listener.onError(throwable);
				}

				@Override
				public void onCompleted() {
					listener.onCompleted();
				}
			});
	}

	public static void touch(TxnOps txn, ByteSequence key) {
		txn.put(key, ByteSequence.EMPTY, PutOption.DEFAULT);
	}

	public static void touchTimestamp(TxnOps txn, ByteSequence key, CurrentTimeProvider timeProvider) {
		ByteSequence value = TOUCH_TIMESTAMP_CODEC.encodeValue(timeProvider.currentTimeMillis());
		txn.put(key, value, PutOption.DEFAULT);
	}

	public record AtomicUpdateResponse<T>(Response.Header header, T prevValue, T newValue) {}

	public static CompletableFuture<AtomicUpdateResponse<Integer>> atomicAdd(KV client, ByteSequence key, int delta) {
		return atomicUpdate(client, key, EtcdValueCodecs.ofIntegerString(), value -> value + delta);
	}

	public static CompletableFuture<AtomicUpdateResponse<Long>> atomicAdd(KV client, ByteSequence key, long delta) {
		return atomicUpdate(client, key, EtcdValueCodecs.ofLongString(), value -> value + delta);
	}

	public static <T> CompletableFuture<AtomicUpdateResponse<T>> atomicUpdate(
		KV client, ByteSequence key, EtcdValueCodec<T> codec, UnaryOperator<T> operator
	) {
		CompletableFuture<AtomicUpdateResponse<T>> future = new CompletableFuture<>();
		atomicUpdate1(client, key, codec, operator, future);
		return future;
	}

	private static <T> void atomicUpdate1(
		KV client, ByteSequence key, EtcdValueCodec<T> codec, UnaryOperator<T> operator,
		CompletableFuture<AtomicUpdateResponse<T>> future
	) {
		client
			.get(key, GetOption.builder().withSerializable(true).build())
			.whenComplete((getResponse, throwable) -> {
				if (throwable != null) {
					future.completeExceptionally(new EtcdException("Atomic update failed", convertStatusException(throwable.getCause())));
					return;
				}
				if (getResponse.getKvs().isEmpty()) {
					future.completeExceptionally(new NoKeyFoundException(key));
					return;
				}
				ByteSequence prevSequence = getResponse.getKvs().get(0).getValue();
				T prevValue;
				try {
					prevValue = codec.decodeValue(prevSequence);
				} catch (MalformedEtcdDataException e) {
					future.completeExceptionally(new MalformedEtcdDataException(
						"Failed to decode value of key '" + key + '\'', e));
					return;
				}
				T newValue = operator.apply(prevValue);
				ByteSequence newSequence = codec.encodeValue(newValue);
				atomicUpdate2(client, key, codec, operator, prevSequence, newSequence, prevValue, newValue, future);
			});
	}

	private static <T> void atomicUpdate2(
		KV client, ByteSequence key, EtcdValueCodec<T> codec, UnaryOperator<T> operator,
		ByteSequence prevSequence, ByteSequence newSequence, T prevValue, T newValue,
		CompletableFuture<AtomicUpdateResponse<T>> future
	) {
		client.txn()
			.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.value(prevSequence)))
			.Then(Op.put(key, newSequence, PutOption.DEFAULT))
			.commit()
			.whenComplete((txnResponse, throwable) -> {
				if (throwable != null) {
					future.completeExceptionally(new EtcdException("Atomic update failed", convertStatusException(throwable.getCause())));
					return;
				}
				if (!txnResponse.isSucceeded()) {
					atomicUpdate1(client, key, codec, operator, future);
					return;
				}
				future.complete(new AtomicUpdateResponse<>(txnResponse.getHeader(), prevValue, newValue));
			});
	}

	public static <V> void checkAndUpdate(TxnOps txn, ByteSequence key, EtcdValueEncoder<V> codec, V prev, V next) {
		checkAndReplace(txn, key, codec.encodeValue(prev), codec.encodeValue(next));
	}

	public static <K, V> void checkAndUpdate(TxnOps txn, EtcdKVEncoder<?, Map.Entry<K, V>> codec, Map<K, V> prev, Map<K, V> next) {
		for (var prevEntry : prev.entrySet()) {
			K k = prevEntry.getKey();
			KeyValue prevKV = codec.encodeKV(prevEntry);
			if (next.containsKey(k)) {
				V nextV = next.get(k);
				KeyValue nextKV = codec.encodeKV(Map.entry(k, nextV));
				if (prevKV.key().equals(nextKV.key())) {
					checkAndReplace(txn, prevKV.key(), prevKV.value(), nextKV.value());
				} else {
					checkAndMove(txn, prevKV.key(), prevKV.value(), nextKV.key(), nextKV.value());
				}
			} else {
				checkAndDelete(txn, prevKV.key(), prevKV.value());
			}
		}
		for (var nextEntry : next.entrySet()) {
			if (prev.containsKey(nextEntry.getKey())) return;
			KeyValue nextKV = codec.encodeKV(nextEntry);
			checkAndInsert(txn, nextKV.key(), nextKV.value());
		}
	}

	public static <K, V> void checkAndInsert(TxnOps txn, EtcdKVEncoder<?, Map.Entry<K, V>> codec, Map<K, V> next) {
		for (var nextEntry : next.entrySet()) {
			K k = nextEntry.getKey();
			V nextV = nextEntry.getValue();
			KeyValue nextKeyValue = codec.encodeKV(Map.entry(k, nextV));
			checkAndInsert(txn, nextKeyValue.key(), nextKeyValue.value());
		}
	}

	public static <T> void checkAndInsert(TxnOps txn, EtcdKVEncoder<?, T> codec, Collection<T> next) {
		for (var item : next) {
			KeyValue nextKV = codec.encodeKV(item);
			checkAndInsert(txn, nextKV.key(), nextKV.value());
		}
	}

	public static <T> void checkAndReplace(TxnOps txn, EtcdKVEncoder<?, T> codec, Collection<T> next) {
		for (var item : next) {
			KeyValue nextKV = codec.encodeKV(item);
			checkAndReplace(txn, nextKV.key(), nextKV.value());
		}
	}

	public static <T> void checkAndDelete(TxnOps txn, EtcdKVEncoder<?, T> codec, Collection<T> prev) {
		for (var item : prev) {
			KeyValue prevKV = codec.encodeKV(item);
			checkAndDelete(txn, prevKV.key(), prevKV.value());
		}
	}

	public static <K> void checkAndDelete(TxnOps txn, EtcdKeyEncoder<K> codec, Collection<K> prev) {
		for (var item : prev) {
			checkAndDelete(txn, codec.encodeKey(item));
		}
	}

	public static void checkAndReplace(TxnOps txn, ByteSequence key, ByteSequence prevValue, ByteSequence nextValue) {
		txn.cmp(key, Cmp.Op.EQUAL, CmpTarget.value(prevValue));
		txn.put(key, nextValue, PutOption.DEFAULT);
	}

	public static void checkAndReplace(TxnOps txn, ByteSequence key, ByteSequence nextValue) {
		txn.cmp(key, Cmp.Op.GREATER, CmpTarget.createRevision(0));
		txn.put(key, nextValue, PutOption.DEFAULT);
	}

	public static void checkAndMove(TxnOps txn, ByteSequence prevKey, ByteSequence prevValue, ByteSequence nextKey, ByteSequence nextValue) {
		txn.cmp(prevKey, Cmp.Op.EQUAL, CmpTarget.value(prevValue));
		checkAndMove(txn, prevKey, nextKey, nextValue);
	}

	public static void checkAndMove(TxnOps txn, ByteSequence prevKey, ByteSequence nextKey, ByteSequence nextValue) {
		txn.cmp(nextKey, Cmp.Op.EQUAL, CmpTarget.createRevision(0));
		txn.delete(prevKey, DeleteOption.DEFAULT);
		txn.put(nextKey, nextValue, PutOption.DEFAULT);
	}

	public static void checkAndInsert(TxnOps txn, ByteSequence key, ByteSequence value) {
		txn.cmp(key, Cmp.Op.EQUAL, CmpTarget.createRevision(0));
		txn.put(key, value, PutOption.DEFAULT);
	}

	public static void checkAndDelete(TxnOps txn, ByteSequence key, ByteSequence value) {
		txn.cmp(key, Cmp.Op.EQUAL, CmpTarget.value(value));
		checkAndDelete(txn, key);
	}

	public static void checkAndDelete(TxnOps txn, ByteSequence key) {
		txn.cmp(key, Cmp.Op.GREATER, CmpTarget.createRevision(0));
		txn.delete(key, DeleteOption.DEFAULT);
	}

	public static CompletableFuture<TxnResponse> executeTxnOps(KV client, ByteSequence prefix, Consumer<TxnOps> txnOpsConsumer) {
		TxnOps txnOps = new TxnOps(prefix);
		txnOpsConsumer.accept(txnOps);
		return executeTxnOps(client, txnOps);
	}

	public static CompletableFuture<TxnResponse> executeTxnOps(KV client, TxnOps txnOps) {
		return client.txn()
			.If(txnOps.cmps.toArray(Cmp[]::new))
			.Then(txnOps.ops.toArray(Op[]::new))
			.commit()
			.exceptionallyCompose(e -> failedFuture(new EtcdException("Transaction failed", convertStatusException(e.getCause()))))
			.thenCompose(txnResponse ->
				txnResponse.isSucceeded() ?
					completedFuture(txnResponse) :
					failedFuture(new TransactionNotSucceededException()));
	}

	public static <T> CompletionStage<T> convertStatusExceptionStage(Throwable throwable) {
		if (throwable instanceof CompletionException) {
			throwable = throwable.getCause();
		}
		return failedFuture(convertStatusException(throwable));
	}

	public static Throwable convertStatusException(Throwable throwable) {
		if (!(throwable instanceof StatusRuntimeException sre)) {
			return throwable;
		}
		Status status = Status.fromThrowable(throwable);
		return switch (status.getCode()) {
			case OK,
				CANCELLED,
				NOT_FOUND,
				ALREADY_EXISTS,
				DEADLINE_EXCEEDED,
				UNAVAILABLE,
				ABORTED -> status.asException(sre.getTrailers());
			case PERMISSION_DENIED,
				RESOURCE_EXHAUSTED,
				INVALID_ARGUMENT,
				OUT_OF_RANGE,
				FAILED_PRECONDITION,
				UNIMPLEMENTED,
				INTERNAL,
				DATA_LOSS,
				UNAUTHENTICATED -> status.asRuntimeException(sre.getTrailers());
			case UNKNOWN -> DEADLINE_EXCEEDED_STATUS_MESSAGE.equals(status.getDescription()) ?
				status.asException(sre.getTrailers()) :
				status.asRuntimeException(sre.getTrailers());
		};
	}

}
