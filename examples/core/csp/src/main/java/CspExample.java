import io.activej.csp.ChannelInput;
import io.activej.csp.ChannelOutput;
import io.activej.csp.consumer.ChannelConsumer;
import io.activej.csp.consumer.ChannelConsumers;
import io.activej.csp.dsl.WithChannelTransformer;
import io.activej.csp.process.AbstractCommunicatingProcess;
import io.activej.csp.supplier.ChannelSupplier;
import io.activej.csp.supplier.ChannelSuppliers;
import io.activej.eventloop.Eventloop;

/**
 * AsyncProcess that takes a string, sets it to upper-case and adds string's length in parentheses
 */
//[START EXAMPLE]
public final class CspExample extends AbstractCommunicatingProcess implements WithChannelTransformer<CspExample, String, String> {
	private ChannelSupplier<String> input;
	private ChannelConsumer<String> output;

	@Override
	public ChannelOutput<String> getOutput() {
		return output -> {
			this.output = output;
			if (this.input != null && this.output != null) startProcess();
		};
	}

	@Override
	public ChannelInput<String> getInput() {
		return input -> {
			this.input = input;
			if (this.input != null && this.output != null) startProcess();
			return getProcessCompletion();
		};
	}

	@Override
	//[START REGION_1]
	protected void doProcess() {
		input.get()
				.whenResult(data -> {
					if (data == null) {
						output.acceptEndOfStream()
								.whenResult(this::completeProcess);
					} else {
						data = data.toUpperCase() + '(' + data.length() + ')';

						output.accept(data)
								.whenResult(this::doProcess);
					}
				})
				.whenException(Throwable::printStackTrace);
	}
	//[END REGION_1]

	@Override
	protected void doClose(Exception e) {
		System.out.println("Process has been closed with exception: " + e);
		input.closeEx(e);
		output.closeEx(e);
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.builder()
				.withCurrentThread()
				.build();

		CspExample process = new CspExample();
		ChannelSuppliers.ofValues("hello", "world", "nice", "to", "see", "you")
				.transformWith(process)
				.streamTo(ChannelConsumers.ofConsumer(System.out::println));

		eventloop.run();
	}
}
//[END EXAMPLE]
