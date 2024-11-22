package io.activej.csp.supplier.impl;

import io.activej.common.annotation.ExposedInternals;
import io.activej.csp.supplier.AbstractChannelSupplier;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.promise.Promise;
import io.activej.promise.SettableCallback;

import java.util.Iterator;
import java.util.function.Function;

import static io.activej.common.collection.IteratorUtils.iteratorOf;

@ExposedInternals
public final class Remap<T, V> extends AbstractChannelSupplier<V> {
	public final ChannelSupplier<T> supplier;
	public final Function<? super T, ? extends Iterator<? extends V>> fn;

	public Iterator<? extends V> iterator;
	public boolean endOfStream;

	public Remap(ChannelSupplier<T> supplier, Function<? super T, ? extends Iterator<? extends V>> fn) {
		super(supplier);
		this.supplier = supplier;
		this.fn = fn;
		iterator = iteratorOf();
	}

	@Override
	protected Promise<V> doGet() {
		if (iterator.hasNext()) return Promise.of(iterator.next());
		return Promise.ofCallback(this::next);
	}

	private void next(SettableCallback<V> cb) {
		if (!endOfStream) {
			supplier.get()
				.subscribe((item, e) -> {
					if (e == null) {
						if (item == null) endOfStream = true;
						iterator = fn.apply(item);
						if (iterator.hasNext()) {
							cb.set(iterator.next());
						} else {
							next(cb);
						}
					} else {
						cb.setException(e);
					}
				});
		} else {
			cb.set(null);
		}
	}
}
