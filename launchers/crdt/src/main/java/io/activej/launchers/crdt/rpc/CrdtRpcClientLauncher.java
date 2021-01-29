package io.activej.launchers.crdt.rpc;

import io.activej.config.ConfigModule;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.service.ServiceGraphModule;

import java.util.List;

public abstract class CrdtRpcClientLauncher extends Launcher {

	protected Module getBusinessLogicModule() {
		return Module.empty();
	}

	protected abstract List<Class<?>> getMessageTypes();

	@Override
	protected final Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
				new CrdtRpcClientModule(),
				getBusinessLogicModule(),
				ModuleBuilder.create()
						.bind(new Key<List<Class<?>>>() {})
						.toInstance(getMessageTypes())
						.build()
		);
	}

}
