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

import java.util.HashSet;
import java.util.Set;

import static io.activej.common.Checks.checkState;
import static io.activej.reactor.Reactive.checkInReactorThread;

/**
 * A consumer that wraps around another consumer that can be hot swapped with some other consumer.
 * <p>
 * It sets its acknowledgement on supplier end of stream, and acts as if suspended when current consumer stops and acknowledges.
 */
public final class StreamConsumer_Switcher<T> extends AbstractStreamConsumer<T> {
	private InternalSupplier internalSupplier = new InternalSupplier();
	private final Set<InternalSupplier> pendingAcknowledgements = new HashSet<>();

	private StreamConsumer_Switcher() {
	}

	/**
	 * Creates a new instance of this consumer.
	 */
	public static <T> StreamConsumer_Switcher<T> create() {
		return new StreamConsumer_Switcher<>();
	}

	public Promise<Void> switchTo(StreamConsumer<T> consumer) {
		checkInReactorThread(this);
		checkState(!isComplete());
		checkState(!isEndOfStream());
		assert this.internalSupplier != null;

		InternalSupplier internalSupplierOld = this.internalSupplier;
		InternalSupplier internalSupplierNew = new InternalSupplier();

		this.internalSupplier = internalSupplierNew;
		internalSupplierNew.streamTo(consumer);

		internalSupplierOld.sendEndOfStream();

		return internalSupplierNew.getAcknowledgement();
	}

	public int getPendingAcknowledgements() {
		return pendingAcknowledgements.size();
	}

	@Override
	protected void onStarted() {
		resume(internalSupplier.getDataAcceptor());
	}

	@Override
	protected void onEndOfStream() {
		internalSupplier.sendEndOfStream();
	}

	@Override
	protected void onError(Exception e) {
		internalSupplier.closeEx(e);
		for (InternalSupplier pendingAcknowledgement : pendingAcknowledgements) {
			pendingAcknowledgement.getConsumer().closeEx(e);
		}
	}

	@Override
	protected void onCleanup() {
		internalSupplier = null;
		pendingAcknowledgements.clear();
	}

	private class InternalSupplier extends AbstractStreamSupplier<T> {
		@Override
		protected void onStarted() {
			pendingAcknowledgements.add(this);
		}

		@Override
		protected void onResumed() {
			if (StreamConsumer_Switcher.this.internalSupplier == this) {
				StreamConsumer_Switcher.this.resume(getDataAcceptor());
			}
		}

		@Override
		protected void onSuspended() {
			if (StreamConsumer_Switcher.this.internalSupplier == this) {
				StreamConsumer_Switcher.this.suspend();
			}
		}

		@Override
		protected void onAcknowledge() {
			pendingAcknowledgements.remove(this);
			if (pendingAcknowledgements.isEmpty()) {
				StreamConsumer_Switcher.this.acknowledge();
			}
		}

		@Override
		protected void onError(Exception e) {
			StreamConsumer_Switcher.this.closeEx(e);
		}
	}
}
