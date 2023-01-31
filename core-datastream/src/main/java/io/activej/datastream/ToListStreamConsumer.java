/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.datastream;

import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;

import java.util.ArrayList;
import java.util.List;

/**
 * Accumulates items from this supplier until it closes and
 * then completes the returned promise with a list of those items.
 *
 * @see StreamSupplier#toList()
 */
public final class ToListStreamConsumer<T> extends AbstractStreamConsumer<T> {
	private final SettablePromise<List<T>> resultPromise = new SettablePromise<>();
	private final List<T> list;

	private ToListStreamConsumer(List<T> list) {
		this.list = list;
	}

	public static <T> ToListStreamConsumer<T> create() {
		return create(new ArrayList<>());
	}

	public static <T> ToListStreamConsumer<T> create(List<T> list) {
		return new ToListStreamConsumer<>(list);
	}

	public Promise<List<T>> getResult() {
		return resultPromise;
	}

	public List<T> getList() {
		return list;
	}

	@Override
	protected void onInit() {
		resultPromise.whenResult(this::acknowledge);
	}

	@Override
	protected void onStarted() {
		resume(list::add);
	}

	@Override
	protected void onEndOfStream() {
		resultPromise.set(list);
	}

	@Override
	protected void onError(Exception e) {
		resultPromise.setException(e);
	}
}
