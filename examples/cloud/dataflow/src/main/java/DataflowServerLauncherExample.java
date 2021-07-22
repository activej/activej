import dto.CreateStringCountFunction;
import dto.ExtractStringFunction;
import dto.StringCountReducer;
import io.activej.config.Config;
import io.activej.dataflow.inject.DatasetId;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.launchers.dataflow.DataflowServerLauncher;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import static io.activej.dataflow.json.JsonUtils.codec;
import static io.activej.dataflow.json.JsonUtils.ofObject;
import static java.util.stream.Collectors.toList;

/**
 * Simple Dataflow node server launcher. Launch it with the first argument set to
 * the port that you want to bind it to, otherwise it will be bound to 9000
 * <p>
 * And the second argument defines which resource file to use as the source for the server data
 */
//[START EXAMPLE]
public final class DataflowServerLauncherExample extends DataflowServerLauncher {

	@Override
	protected Module getOverrideModule() {
		return ModuleBuilder.create()
				.bind(codec(CreateStringCountFunction.class)).toInstance(ofObject(CreateStringCountFunction::new))
				.bind(codec(ExtractStringFunction.class)).toInstance(ofObject(ExtractStringFunction::new))
				.bind(codec(StringCountReducer.class)).toInstance(ofObject(StringCountReducer::new))

				.bind(StreamSorterStorageFactory.class).toInstance(StreamMergeSorterStorageStub.FACTORY_STUB)

				.bind(Config.class).toInstance(
						Config.create()
								.with("dataflow.server.listenAddresses", args.length > 0 ? args[0] : "9000")
								.with("dataflow.secondaryBufferPath", Util.createTempDir("dataflow-server-secondary-storage")))
				.build();
	}

	@Provides
	@DatasetId("items")
	List<String> words() {
		String file = args.length > 1 ? args[1] : "words1.txt";
		return new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(file)))
				.lines()
				.filter(s -> !s.isEmpty())
				.collect(toList());
	}

	public static void main(String[] args) throws Exception {
		new DataflowServerLauncherExample().launch(args);
	}
}
//[END EXAMPLE]
