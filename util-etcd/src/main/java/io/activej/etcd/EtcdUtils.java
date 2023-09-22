package io.activej.etcd;

import io.activej.common.exception.MalformedDataException;
import io.activej.etcd.codec.key.EtcdKeyEncoder;
import io.activej.etcd.codec.kv.EtcdKVDecoder;
import io.activej.etcd.codec.kv.EtcdKVEncoder;
import io.activej.etcd.codec.kv.KeyValue;
import io.activej.etcd.codec.value.EtcdValueCodec;
import io.activej.etcd.codec.value.EtcdValueCodecs;
import io.activej.etcd.codec.value.EtcdValueEncoder;
import io.etcd.jetcd.*;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

	public static final ByteSequence SLASH = byteSequenceFrom('/');
	public static final ByteSequence DOT = byteSequenceFrom('.');
	public static final ByteSequence UNDERSCORE = byteSequenceFrom('_');
	public static final ByteSequence X00 = byteSequenceFrom((byte) 0x00);
	public static final ByteSequence XFF = byteSequenceFrom((byte) 0xFF);

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

	public static <T, A, R> CompletableFuture<CheckoutResponse<R>> checkout(Client client, long revision,
		ByteSequence prefix, EtcdKVDecoder<?, T> codec, Collector<T, A, R> collector
	) {
		//noinspection unchecked
		return checkout(client, revision, new CheckoutRequest[]{new CheckoutRequest<>(prefix, codec, collector)},
			(header, objects) -> new CheckoutResponse<>(header, (R) objects[0]));
	}

	public interface CheckoutFinisher<T> {
		T finish(Response.Header header, Object[] objects) throws MalformedDataException;
	}

	public static <R> CompletableFuture<R> checkout(Client client, long revision,
		CheckoutRequest<?, ?>[] checkoutRequests, CheckoutFinisher<R> checkoutFinisher
	) {
		KV kvClient = client.getKVClient();
		return kvClient.txn()
			.Then(Arrays.stream(checkoutRequests)
				.map(checkoutRequest -> Op.get(
					checkoutRequest.prefix,
					GetOption.builder()
						.isPrefix(true)
						.withRevision(revision)
						.build()))
				.toArray(Op[]::new))
			.commit()
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
					} catch (MalformedDataException e) {
						return failedFuture(e);
					}
				}

				static <KV, A, R> R getCheckoutResult(ByteSequence prefix, EtcdKVDecoder<?, ? extends KV> codec, Collector<KV, A, R> collector, GetResponse response) throws MalformedDataException {
					A accumulator = collector.supplier().get();
					BiConsumer<A, ? super KV> accumulatorFn = collector.accumulator();
					for (var kv : response.getKvs()) {
						KV item = codec.decodeKV(new KeyValue(kv.getKey().substring(prefix.size()), kv.getValue()));
						accumulatorFn.accept(accumulator, item);
					}
					return collector.finisher().apply(accumulator);
				}
			});
	}

	public static <K, KV, R> Watch.Watcher watch(Client client, long revision,
		ByteSequence prefix, EtcdKVDecoder<? extends K, ? extends KV> codec, EtcdEventProcessor<K, KV, ?> eventProcessor,
		EtcdListener<R> listener
	) {
		return watch(client, revision,
			new WatchRequest[]{new WatchRequest<>(prefix, codec, eventProcessor)},
			new EtcdListener<>() {
				@Override
				public void onNext(Response.Header header, Object[] operations) throws MalformedDataException {
					assert operations.length == 1;
					//noinspection unchecked
					listener.onNext(header, (R) operations[0]);
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

	public static Watch.Watcher watch(Client client, long revision,
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

		return client.getWatchClient().watch(
			root,
			WatchOption.builder()
				.isPrefix(true)
				.withRevision(revision)
				.build(),
			new Watch.Listener() {
				@Override
				public void onNext(WatchResponse response) {
					try {
						Object[] accumulators = new Object[requests.length];
						for (int i = 0; i < accumulators.length; i++) {
							accumulators[i] = requests[i].eventProcessor.createEventsAccumulator();
						}
						for (var event : response.getEvents()) {
							var keyValue = event.getKeyValue();
							var rootKey = keyValue.getKey().substring(root.size());

							for (int i = 0; i < accumulators.length; i++) {
								var request = requests[i];
								if (!rootKey.startsWith(request.prefix)) continue;
								var key = rootKey.substring(request.prefix.size());
								if (event.getEventType() == WatchEvent.EventType.PUT) {
									Object kv = request.codec.decodeKV(new KeyValue(key, keyValue.getValue()));
									//noinspection unchecked
									request.eventProcessor.onPut(accumulators[i], kv);
								}
								if (event.getEventType() == WatchEvent.EventType.DELETE) {
									Object k = request.codec.decodeKey(key);
									//noinspection unchecked
									request.eventProcessor.onDelete(accumulators[i], k);
								}
							}
						}
						listener.onNext(response.getHeader(), accumulators);
					} catch (MalformedDataException e) {
						onError(e);
					}
				}

				boolean onError = false;

				@Override
				public void onError(Throwable throwable) {
					if (!onError) {
						onError = true;
						listener.onError(throwable);
					}
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

	public record AtomicUpdateResponse<T>(Response.Header header, T prevValue, T newValue) {}

	public static CompletableFuture<AtomicUpdateResponse<Integer>> atomicAdd(Client client, ByteSequence key, int delta) {
		return atomicUpdate(client, key, EtcdValueCodecs.ofIntegerString(), value -> value + delta);
	}

	public static CompletableFuture<AtomicUpdateResponse<Long>> atomicAdd(Client client, ByteSequence key, long delta) {
		return atomicUpdate(client, key, EtcdValueCodecs.ofLongString(), value -> value + delta);
	}

	public static <T> CompletableFuture<AtomicUpdateResponse<T>> atomicUpdate(
		Client client, ByteSequence key, EtcdValueCodec<T> codec, UnaryOperator<T> operator
	) {
		CompletableFuture<AtomicUpdateResponse<T>> future = new CompletableFuture<>();
		atomicUpdate1(client, key, codec, operator, future);
		return future;
	}

	private static <T> void atomicUpdate1(
		Client client, ByteSequence key, EtcdValueCodec<T> codec, UnaryOperator<T> operator,
		CompletableFuture<AtomicUpdateResponse<T>> future
	) {
		client.getKVClient()
			.get(key, GetOption.builder().withSerializable(true).build())
			.whenComplete((getResponse, throwable) -> {
				if (throwable != null) {
					future.completeExceptionally(throwable);
					return;
				}
				if (getResponse.getKvs().isEmpty()) {
					future.completeExceptionally(new IllegalArgumentException());
					return;
				}
				ByteSequence prevSequence = getResponse.getKvs().get(0).getValue();
				T prevValue;
				try {
					prevValue = codec.decodeValue(prevSequence);
				} catch (MalformedDataException e) {
					future.completeExceptionally(e);
					return;
				}
				T newValue = operator.apply(prevValue);
				ByteSequence newSequence = codec.encodeValue(newValue);
				atomicUpdate2(client, key, codec, operator, prevSequence, newSequence, prevValue, newValue, future);
			});
	}

	private static <T> void atomicUpdate2(
		Client client, ByteSequence key, EtcdValueCodec<T> codec, UnaryOperator<T> operator,
		ByteSequence prevSequence, ByteSequence newSequence, T prevValue, T newValue,
		CompletableFuture<AtomicUpdateResponse<T>> future
	) {
		client.getKVClient().txn()
			.If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.value(prevSequence)))
			.Then(Op.put(key, newSequence, PutOption.DEFAULT))
			.commit()
			.whenComplete((txnResponse, throwable) -> {
				if (throwable != null) {
					future.completeExceptionally(throwable);
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

	public static CompletableFuture<TxnResponse> executeTxnOps(Client client, ByteSequence prefix, Consumer<TxnOps> txnOpsConsumer) {
		TxnOps txnOps = new TxnOps(prefix);
		txnOpsConsumer.accept(txnOps);
		return executeTxnOps(client, txnOps);
	}

	public static CompletableFuture<TxnResponse> executeTxnOps(Client client, TxnOps txnOps) {
		KV kvClient = client.getKVClient();
		return kvClient.txn()
			.If(txnOps.cmps.toArray(Cmp[]::new))
			.Then(txnOps.ops.toArray(Op[]::new))
			.commit()
			.thenCompose(txnResponse ->
				txnResponse.isSucceeded() ?
					completedFuture(txnResponse) :
					failedFuture(new IOException("Transaction failed")));
	}

}