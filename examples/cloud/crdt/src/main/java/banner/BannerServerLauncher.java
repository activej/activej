package banner;

import io.activej.config.ConfigModule;
import io.activej.crdt.primitives.GSet;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.launchers.crdt.rpc.CrdtRpcServerModule;
import io.activej.service.ServiceGraphModule;

import java.util.List;

public final class BannerServerLauncher extends Launcher {
	public static final List<Class<?>> MESSAGE_TYPES = List.of(
			BannerCommands.GetRequest.class, BannerCommands.GetResponse.class,
			BannerCommands.PutRequest.class, BannerCommands.PutResponse.class,
			BannerCommands.IsBannerSeenRequest.class, Boolean.class);

	@Override
	protected Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
				new CrdtRpcServerModule<Long, GSet<Integer>>() {
					@Override
					protected List<Class<?>> getMessageTypes() {
						return MESSAGE_TYPES;
					}
				},
				new BannerServerModule()
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new BannerServerLauncher().launch(args);
	}
}
