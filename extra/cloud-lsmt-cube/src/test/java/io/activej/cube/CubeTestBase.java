package io.activej.cube;

import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.cube.etcd.CubeEtcdOTUplink;
import io.activej.cube.etcd.CubeEtcdStateManager;
import io.activej.cube.linear.CubeMySqlOTUplink;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeOT;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.etl.LogState;
import io.activej.ot.OTStateManager;
import io.activej.ot.StateManager;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.repository.MySqlOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.OTUplink;
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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.cube.TestUtils.*;
import static io.activej.cube.json.JsonCodecs.createCubeDiffCodec;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.etl.json.JsonCodecs.ofLogDiff;
import static io.activej.promise.TestUtils.await;

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
	public StateManagerFactory<StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>>> stateManagerFactory;

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
				new StateManagerFactory<OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>>>() {
					private final AsyncSupplier<Long> idGenerator = AsyncSupplier.of(new RefLong(0)::inc);

					@Override
					public OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> createUninitialized(CubeStructure structure, Description description) {
						Reactor reactor = Reactor.getCurrentReactor();
						AsyncOTRepository<Long, LogDiff<CubeDiff>> repository = MySqlOTRepository.create(reactor, EXECUTOR, DATA_SOURCE, idGenerator,
							LOG_OT, ofLogDiff(createCubeDiffCodec(structure)));
						var uplink = OTUplink.create(reactor, repository, LOG_OT);

						return OTStateManager.create(reactor, LOG_OT, uplink, LogState.create(CubeState.create(structure)));
					}

					@Override
					public void initialize(OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager) {
						OTUplink<Long, LogDiff<CubeDiff>, ?> uplink = (OTUplink<Long, LogDiff<CubeDiff>, ?>) stateManager.getUplink();
						MySqlOTRepository<LogDiff<CubeDiff>> repository = (MySqlOTRepository<LogDiff<CubeDiff>>) uplink.getRepository();
						noFail(() -> initializeRepository(repository));
						start(stateManager);
					}

					@Override
					public void start(OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager) {
						await(stateManager.start());
					}
				},
			},

			new Object[]{
				"Linear graph",
				new StateManagerFactory<OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>>>() {
					@Override
					public OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> createUninitialized(CubeStructure structure, Description description) {
						Reactor reactor = Reactor.getCurrentReactor();
						var uplink = CubeMySqlOTUplink.builder(reactor, EXECUTOR, structure, DATA_SOURCE)
							.build();

						return OTStateManager.create(reactor, LOG_OT, uplink, LogState.create(CubeState.create(structure)));
					}

					@Override
					public void initialize(OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager) {
						noFail(() -> initializeUplink((CubeMySqlOTUplink) stateManager.getUplink()));
						start(stateManager);
					}

					@Override
					public void start(OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager) {
						await(stateManager.start());
					}
				},
			},

			new Object[]{
				"etcd graph",
				new StateManagerFactory<OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>>>() {
					@Override
					public OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> createUninitialized(CubeStructure structure, Description description) {
						ByteSequence root = byteSequenceFrom("test." + description.getClassName() + "#" + description.getMethodName());
						var uplink = CubeEtcdOTUplink.builder(Reactor.getCurrentReactor(), structure, ETCD_CLIENT, root)
							.build();

						return OTStateManager.create(Reactor.getCurrentReactor(), LOG_OT, uplink, LogState.create(CubeState.create(structure)));
					}

					@Override
					public void initialize(OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager) {
						noFail(((CubeEtcdOTUplink) stateManager.getUplink())::delete);
						start(stateManager);
					}

					@Override
					public void start(OTStateManager<Long, LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>> stateManager) {
						await(stateManager.start());
					}
				}
			},

			new Object[]{
				"etcd state",
				new StateManagerFactory<CubeEtcdStateManager>() {
					@Override
					public CubeEtcdStateManager createUninitialized(CubeStructure structure, Description description) {
						ByteSequence root = byteSequenceFrom("test." + description.getClassName() + "#" + description.getMethodName());

						return CubeEtcdStateManager.builder(ETCD_CLIENT, root, structure)
							.build();
					}

					@Override
					public void initialize(CubeEtcdStateManager stateManager) {
						noFail(stateManager::delete);
						start(stateManager);
					}

					@Override
					public void start(CubeEtcdStateManager stateManager) {
						noFail(stateManager::start);
					}
				}
			}
		);
	}

	public interface StateManagerFactory<S extends StateManager<LogDiff<CubeDiff>, LogState<CubeDiff, CubeState>>> {
		default S create(CubeStructure structure, Description description) {
			S stateManager = createUninitialized(structure, description);
			initialize(stateManager);
			return stateManager;
		}

		S createUninitialized(CubeStructure structure, Description description);

		void initialize(S stateManager);

		void start(S stateManager);
	}
}
