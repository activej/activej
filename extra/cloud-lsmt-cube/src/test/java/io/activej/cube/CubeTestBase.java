package io.activej.cube;

import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.function.StateQueryFunction;
import io.activej.common.ref.RefLong;
import io.activej.cube.etcd.CubeEtcdOTUplink;
import io.activej.cube.etcd.CubeEtcdStateManager;
import io.activej.cube.json.PrimaryKeyJsonCodecFactory;
import io.activej.cube.linear.CubeMySqlOTUplink;
import io.activej.cube.linear.MeasuresValidator;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeOT;
import io.activej.cube.service.ServiceStateManager;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.etl.LogState;
import io.activej.ot.OTState;
import io.activej.ot.OTStateManager;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.repository.MySqlOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.ot.uplink.OTUplink;
import io.activej.promise.Promise;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.DescriptionRule;
import io.activej.test.rules.EventloopRule;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.activej.cube.TestUtils.*;
import static io.activej.cube.json.JsonCodecs.ofCubeDiff;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.etl.json.JsonCodecs.ofLogDiff;
import static io.activej.promise.TestUtils.await;
import static io.activej.test.TestUtils.dataSource;

@RunWith(Parameterized.class)
public abstract class CubeTestBase {
	public static final OTSystem<LogDiff<CubeDiff>> LOG_OT = LogOT.createLogOT(CubeOT.createCubeOT());
	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Rule
	public DescriptionRule descriptionRule = new DescriptionRule();

	@Parameter()
	public String testName;

	@Parameter(1)
	public StateManagerFactory<TestStateManager> stateManagerFactory;

	public static final Executor EXECUTOR = Executors.newCachedThreadPool();
	public static final DataSource DATA_SOURCE;

	public NioReactor reactor;

	public static final Client ETCD_CLIENT = Client.builder().waitForReady(false).endpoints("http://127.0.0.1:2379").build();

	public Description description;

	@Before
	public void setUp() throws Exception {
		reactor = Reactor.getCurrentReactor();
		description = descriptionRule.getDescription();
	}

	static {
		try {
			DATA_SOURCE = dataSource("test.properties");
		} catch (IOException | SQLException e) {
			throw new AssertionError(e);
		}
	}

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return List.of(
			new Object[]{
				"OT graph",
				new StateManagerFactory<OTManager>() {
					private final AsyncSupplier<Long> idGenerator = AsyncSupplier.of(new RefLong(0)::inc);

					@Override
					public OTManager createUninitialized(CubeStructure structure, Description description) {
						Reactor reactor = Reactor.getCurrentReactor();
						AsyncOTRepository<Long, LogDiff<CubeDiff>> repository = MySqlOTRepository.create(reactor, EXECUTOR, DATA_SOURCE, idGenerator,
							LOG_OT, ofLogDiff(ofCubeDiff(structure)));
						var uplink = OTUplink.create(reactor, repository, LOG_OT);
						var stateManager = OTStateManager.create(reactor, LOG_OT, uplink, LogState.create(CubeState.create(structure)));

						return new OTManager(uplink, stateManager);
					}

					@Override
					public void initialize(OTManager stateManager) {
						OTUplink<Long, LogDiff<CubeDiff>, ?> uplink = (OTUplink<Long, LogDiff<CubeDiff>, ?>) stateManager.uplink;
						MySqlOTRepository<LogDiff<CubeDiff>> repository = (MySqlOTRepository<LogDiff<CubeDiff>>) uplink.getRepository();
						noFail(() -> initializeRepository(repository));
					}
				},
			},

			new Object[]{
				"Linear graph",
				new StateManagerFactory<OTManager>() {
					@Override
					public OTManager createUninitialized(CubeStructure structure, Description description) {
						Reactor reactor = Reactor.getCurrentReactor();
						var uplink = CubeMySqlOTUplink.builder(reactor, EXECUTOR, DATA_SOURCE, PrimaryKeyJsonCodecFactory.ofCubeStructure(structure))
							.withMeasuresValidator(MeasuresValidator.ofCubeStructure(structure))
							.build();
						var stateManager = OTStateManager.create(reactor, LOG_OT, uplink, LogState.create(CubeState.create(structure)));

						return new OTManager(uplink, stateManager);
					}

					@Override
					public void initialize(OTManager stateManager) {
						noFail(() -> initializeUplink((CubeMySqlOTUplink) stateManager.uplink));
					}
				},
			},

			new Object[]{
				"etcd graph",
				new StateManagerFactory<OTManager>() {
					@Override
					public OTManager createUninitialized(CubeStructure structure, Description description) {
						ByteSequence root = byteSequenceFrom("test." + description.getClassName() + "#" + description.getMethodName());
						var uplink = CubeEtcdOTUplink.builder(Reactor.getCurrentReactor(), ETCD_CLIENT, root)
							.withChunkCodecsFactoryJson(structure)
							.withMeasuresValidator(MeasuresValidator.ofCubeStructure(structure))
							.build();
						var stateManager = OTStateManager.create(Reactor.getCurrentReactor(), LOG_OT, uplink, LogState.create(CubeState.create(structure)));

						return new OTManager(uplink, stateManager);
					}

					@Override
					public void initialize(OTManager stateManager) {
						noFail(((CubeEtcdOTUplink) stateManager.uplink)::delete);
					}
				}
			},

			new Object[]{
				"etcd state",
				new StateManagerFactory<EtcdManager>() {
					@Override
					public EtcdManager createUninitialized(CubeStructure structure, Description description) {
						ByteSequence root = byteSequenceFrom("test." + description.getClassName() + "#" + description.getMethodName());

						CubeEtcdStateManager stateManager = CubeEtcdStateManager.builder(ETCD_CLIENT, root, structure)
							.withChunkCodecsFactoryJson(structure)
							.withMeasuresValidator(MeasuresValidator.ofCubeStructure(structure))
							.build();

						return new EtcdManager(stateManager);
					}

					@Override
					public void initialize(EtcdManager stateManager) {
						noFail((stateManager.stateManager)::delete);
					}
				}
			}
		);
	}

	public interface TestStateManager {
		void checkout() throws Exception;

		void push(List<LogDiff<CubeDiff>> diffs) throws Exception;

		default void push(LogDiff<CubeDiff> diff) throws Exception {
			push(List.of(diff));
		}

		void sync() throws Exception;

		void reset();

		StateQueryFunction<LogState<CubeDiff, ?>> getLogState();

		StateQueryFunction<CubeState> getCubeState();
	}

	public interface StateManagerFactory<S extends TestStateManager> {
		default S create(CubeStructure structure, Description description) {
			S stateManager = createUninitialized(structure, description);
			initialize(stateManager);
			return stateManager;
		}

		S createUninitialized(CubeStructure structure, Description description);

		void initialize(S stateManager);
	}

	private static final class OTManager implements TestStateManager {
		private final AsyncOTUplink<Long, LogDiff<CubeDiff>, ?> uplink;
		private final OTStateManager<Long, LogDiff<CubeDiff>> stateManager;

		private OTManager(
			AsyncOTUplink<Long, LogDiff<CubeDiff>, ?> uplink,
			OTStateManager<Long, LogDiff<CubeDiff>> stateManager
		) {
			this.uplink = uplink;
			this.stateManager = stateManager;
		}

		@Override
		public void checkout() {
			await(stateManager.checkout());
		}

		@Override
		public void push(List<LogDiff<CubeDiff>> diffs) {
			stateManager.addAll(diffs);
			await(stateManager.sync());
		}

		@Override
		public void sync() {
			await(stateManager.sync());
		}

		@Override
		public void reset() {
			stateManager.reset();
		}

		@Override
		public StateQueryFunction<LogState<CubeDiff, ?>> getLogState() {
			return StateQueryFunction.ofState(((LogState<CubeDiff, ?>) stateManager.getState()));
		}

		@Override
		public StateQueryFunction<CubeState> getCubeState() {
			OTState<CubeDiff> dataState = ((LogState<CubeDiff, ?>) stateManager.getState()).getDataState();
			return StateQueryFunction.ofState(((CubeState) dataState));
		}
	}

	private static final class EtcdManager implements TestStateManager {
		private final CubeEtcdStateManager stateManager;

		private EtcdManager(CubeEtcdStateManager stateManager) {
			this.stateManager = stateManager;
		}

		@Override
		public void checkout() throws Exception {
			stateManager.start();
		}

		@Override
		public void push(List<LogDiff<CubeDiff>> diffs) throws ExecutionException, InterruptedException {
			stateManager.push(diffs).get();
		}

		@Override
		public void sync() {
		}

		@Override
		public void reset() {
		}

		@Override
		public StateQueryFunction<LogState<CubeDiff, ?>> getLogState() {
			return new StateQueryFunction<>() {
				@Override
				public <R> R query(Function<LogState<CubeDiff, ?>, R> queryFunction) {
					return stateManager.query(queryFunction::apply);
				}
			};
		}

		@Override
		public StateQueryFunction<CubeState> getCubeState() {
			return new StateQueryFunction<>() {
				@Override
				public <R> R query(Function<CubeState, R> queryFunction) {
					return stateManager.query(state -> queryFunction.apply(state.getDataState()));
				}
			};
		}

	}

	public static ServiceStateManager<LogDiff<CubeDiff>> serviceStateManager(TestStateManager stateManager) {
		return new ServiceStateManager<>() {
			@Override
			public Promise<Void> checkout() {
				try {
					stateManager.checkout();
				} catch (Exception e) {
					return Promise.ofException(e);
				}
				return Promise.complete();
			}

			@Override
			public Promise<Void> sync() {
				try {
					stateManager.sync();
				} catch (Exception e) {
					return Promise.ofException(e);
				}
				return Promise.complete();
			}

			@Override
			public Promise<Void> push(List<LogDiff<CubeDiff>> diffs) {
				try {
					stateManager.push(diffs);
				} catch (Exception e) {
					return Promise.ofException(e);
				}
				return Promise.complete();
			}

			@Override
			public void reset() {
				stateManager.reset();
			}
		};
	}

}
