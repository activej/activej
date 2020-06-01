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

import org.jetbrains.annotations.NotNull;

/**
 * A consumer that wraps around another consumer that can be hot swapped with some other consumer.
 * <p>
 * It sets its acknowledgement on supplier end of stream, and acts as if suspended when current consumer stops and acknowledges.
 */
public final class StreamConsumerSwitcher<T> extends AbstractStreamConsumer<T> {
	private InternalSupplier internalSupplier = new InternalSupplier();

	private StreamConsumerSwitcher() {
	}

	/**
	 * Creates a new instance of this consumer.
	 */
	public static <T> StreamConsumerSwitcher<T> create() {
		return new StreamConsumerSwitcher<>();
	}

	public void switchTo(@NotNull StreamConsumer<T> consumer) {
		InternalSupplier internalSupplierOld = this.internalSupplier;
		InternalSupplier internalSupplierNew = new InternalSupplier();

		if (getAcknowledgement().isException()) {
			internalSupplierNew.closeEx(getAcknowledgement().getException());
		} else if (isEndOfStream()) {
			internalSupplierNew.sendEndOfStream();
		} else {
			this.internalSupplier = internalSupplierNew;
		}

		internalSupplierNew.streamTo(consumer);

		if (internalSupplierOld != null) {
			internalSupplierOld.sendEndOfStream();
		}
	}

	@Override
	protected void onStarted() {
		resume(internalSupplier.getDataAcceptor());
	}

	@Override
	protected void onEndOfStream() {
		internalSupplier.sendEndOfStream();
		acknowledge();
	}

	@Override
	protected void onError(Throwable e) {
		internalSupplier.closeEx(e);
	}

	@Override
	protected void onCleanup() {
		internalSupplier = null;
	}

	private final class InternalSupplier extends AbstractStreamSupplier<T> {
		@Override
		protected void onResumed() {
			if (StreamConsumerSwitcher.this.internalSupplier == this) {
				StreamConsumerSwitcher.this.resume(getDataAcceptor());
			}
		}

		@Override
		protected void onSuspended() {
			if (StreamConsumerSwitcher.this.internalSupplier == this) {
				StreamConsumerSwitcher.this.suspend();
			}
		}

		@Override
		protected void onAcknowledge() {
			if (StreamConsumerSwitcher.this.internalSupplier == this) {
				StreamConsumerSwitcher.this.acknowledge();
			}
		}

		@Override
		protected void onError(Throwable e) {
			if (StreamConsumerSwitcher.this.internalSupplier == this) {
				StreamConsumerSwitcher.this.closeEx(e);
			}
		}
	}
}
