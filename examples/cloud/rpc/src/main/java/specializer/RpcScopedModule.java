package specializer;

import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.Scope;
import io.activej.inject.module.AbstractModule;
import io.activej.rpc.server.RpcRequestHandler;
import specializer.CookieBucketModule.CookieBucket;

public final class RpcScopedModule extends AbstractModule {

	public static final Scope RPC_REQUEST_SCOPE = Scope.of(RpcRequestScope.class);

	private RpcScopedModule() {
	}

	public static RpcScopedModule create() {
		return new RpcScopedModule();
	}

	@Override
	protected void configure() {
		bind(RpcRequest.class).in(RpcRequestScope.class).to(() -> {
			throw new AssertionError();
		});

		bind(RpcResponse.class).to(RpcResponse::create, RpcRequest.class, CookieBucket.class).in(RpcRequestScope.class);

		bind(new Key<RpcRequestHandler<RpcRequest, RpcResponse>>() {}).to(
				injector ->
						request -> {
							Injector subInjector = injector.enterScope(RPC_REQUEST_SCOPE);
							subInjector.putInstance(RpcRequest.class, request);
							return subInjector.getInstance(RpcResponse.class);
						},
				Injector.class);
	}
}
