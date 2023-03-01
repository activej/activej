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

package io.activej.etl;

import io.activej.async.AsyncAccumulator;
import io.activej.datastream.consumer.StreamConsumerWithResult;
import io.activej.datastream.processor.StreamSplitter;
import io.activej.datastream.supplier.StreamDataAcceptor;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.Reactor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;

@SuppressWarnings("unchecked")
public abstract class SplitterLogDataConsumer<T, D> extends AbstractReactive
		implements ILogDataConsumer<T, D> {

	public final class Context {
		private final List<ILogDataConsumer<?, D>> logDataConsumers = new ArrayList<>();
		private Iterator<? extends StreamDataAcceptor<?>> acceptors;

		public <X> StreamDataAcceptor<X> addOutput(ILogDataConsumer<X, D> logDataConsumer) {
			if (acceptors == null) {
				// initial run, recording scheme
				logDataConsumers.add(logDataConsumer);
				return null;
			}
			// receivers must correspond outputs for recorded scheme
			return (StreamDataAcceptor<X>) acceptors.next();
		}
	}

	public SplitterLogDataConsumer(Reactor reactor) {
		super(reactor);
	}

	@Override
	public StreamConsumerWithResult<T, List<D>> consume() {
		checkInReactorThread(this);
		AsyncAccumulator<List<D>> diffsAccumulator = AsyncAccumulator.create(new ArrayList<>());

		Context ctx = new Context();
		createSplitter(ctx);

		StreamSplitter<T, Object> splitter = StreamSplitter.create(acceptors -> {
			ctx.acceptors = List.of(acceptors).iterator();
			return createSplitter(ctx);
		});

		checkState(!ctx.logDataConsumers.isEmpty());

		for (ILogDataConsumer<?, D> logDataConsumer : ctx.logDataConsumers) {
			diffsAccumulator.addPromise(
					splitter.newOutput().streamTo(((ILogDataConsumer<Object, D>) logDataConsumer).consume()),
					List::addAll);
		}

		return StreamConsumerWithResult.of(splitter.getInput(), diffsAccumulator.run().promise());
	}

	protected abstract StreamDataAcceptor<T> createSplitter(Context ctx);

}
