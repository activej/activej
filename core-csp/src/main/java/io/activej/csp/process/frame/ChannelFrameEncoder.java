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

package io.activej.csp.process.frame;

import io.activej.bytebuf.ByteBuf;
import io.activej.common.Checks;
import io.activej.common.builder.AbstractBuilder;
import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.csp.supplier.ChannelSupplier;

import static io.activej.reactor.Reactive.checkInReactorThread;

public final class ChannelFrameEncoder extends AbstractCommunicatingProcess
	implements WithChannelTransformer<ChannelFrameEncoder, ByteBuf, ByteBuf> {

	private static final boolean CHECKS = Checks.isEnabled(ChannelFrameEncoder.class);

	private final BlockEncoder encoder;
	private boolean encoderResets;

	private ChannelSupplier<ByteBuf> input;
	private ChannelConsumer<ByteBuf> output;

	private ChannelFrameEncoder(BlockEncoder encoder) {
		this.encoder = encoder;
	}

	public static ChannelFrameEncoder create(FrameFormat format) {
		return builder(format.createEncoder()).build();
	}

	public static ChannelFrameEncoder create(BlockEncoder encoder) {
		return builder(encoder).build();
	}

	public static Builder builder(FrameFormat format) {
		return builder(format.createEncoder());
	}

	public static Builder builder(BlockEncoder encoder) {
		return new ChannelFrameEncoder(encoder).new Builder();
	}

	public final class Builder extends AbstractBuilder<Builder, ChannelFrameEncoder> {
		private Builder() {}

		public Builder withEncoderResets() {
			checkNotBuilt(this);
			return withEncoderResets(true);
		}

		public Builder withEncoderResets(boolean encoderResets) {
			checkNotBuilt(this);
			ChannelFrameEncoder.this.encoderResets = encoderResets;
			return this;
		}

		@Override
		protected ChannelFrameEncoder doBuild() {
			return ChannelFrameEncoder.this;
		}
	}

	//check input for clarity
	@Override
	public ChannelInput<ByteBuf> getInput() {
		return input -> {
			if (CHECKS) checkInReactorThread(this);
			this.input = sanitize(input);
			//noinspection ConstantConditions
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@SuppressWarnings("ConstantConditions") //check output for clarity
	@Override
	public ChannelOutput<ByteBuf> getOutput() {
		return output -> {
			if (CHECKS) checkInReactorThread(this);
			this.output = sanitize(output);
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	protected void doProcess() {
		encodeBufs();
	}

	private void encodeBufs() {
		input.filter(ByteBuf::canRead)
			.get()
			.whenResult(buf -> {
				if (encoderResets) encoder.reset();
				if (buf != null) {
					ByteBuf outputBuf = encoder.encode(buf);
					buf.recycle();
					output.accept(outputBuf)
						.whenResult(this::encodeBufs);
				} else {
					output.acceptAll(encoder.encodeEndOfStreamBlock(), null)
						.whenResult(this::completeProcess);
				}
			});
	}

	@Override
	protected void doClose(Exception e) {
		input.closeEx(e);
		output.closeEx(e);
	}
}
