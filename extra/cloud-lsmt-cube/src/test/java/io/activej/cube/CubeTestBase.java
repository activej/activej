package io.activej.cube;

import io.activej.async.function.AsyncSupplier;
import io.activej.codegen.DefiningClassLoader;
import io.activej.common.ref.RefLong;
import io.activej.cube.etcd.CubeEtcdOTUplink;
import io.activej.cube.linear.MeasuresValidator;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.cube.ot.CubeOT;
import io.activej.etl.LogDiff;
import io.activej.etl.LogOT;
import io.activej.ot.OTCommit;
import io.activej.ot.repository.AsyncOTRepository;
import io.activej.ot.repository.MySqlOTRepository;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.AsyncOTUplink;
import io.activej.ot.uplink.OTUplink;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import io.etcd.jetcd.Client;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
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

import static io.activej.cube.TestUtils.initializeRepository;
import static io.activej.cube.TestUtils.noFail;
import static io.activej.cube.json.JsonCodecs.ofCubeDiff;
import static io.activej.etcd.EtcdUtils.byteSequenceFrom;
import static io.activej.etl.json.JsonCodecs.ofLogDiff;
import static io.activej.test.TestUtils.dataSource;

@RunWith(Parameterized.class)
public abstract class CubeTestBase {
	public static final OTSystem<LogDiff<CubeDiff>> LOG_OT = LogOT.createLogOT(CubeOT.createCubeOT());
	public static final CubeDiffScheme<LogDiff<CubeDiff>> DIFF_SCHEME = CubeDiffScheme.ofLogDiffs();
	public static final DefiningClassLoader CLASS_LOADER = DefiningClassLoader.create();

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	@Parameter()
	public String testName;

	@Parameter(1)
	public UplinkFactory<AsyncOTUplink<Long, LogDiff<CubeDiff>, ?>> uplinkFactory;

	public static final Executor EXECUTOR = Executors.newCachedThreadPool();
	public static final DataSource DATA_SOURCE;

	public NioReactor reactor;

	public static final Client ETCD_CLIENT = Client.builder().waitForReady(false).endpoints("http://127.0.0.1:2379").build();

	@Before
	public void setUp() throws Exception {
		reactor = Reactor.getCurrentReactor();
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
				new UplinkFactory<OTUplink<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>>>() {
					@Override
					public OTUplink<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> createUninitialized(Cube cube) {
						Reactor reactor = cube.getReactor();
						AsyncOTRepository<Long, LogDiff<CubeDiff>> repository = MySqlOTRepository.create(reactor, EXECUTOR, DATA_SOURCE, AsyncSupplier.of(new RefLong(0)::inc),
							LOG_OT, ofLogDiff(ofCubeDiff(cube)));
						return OTUplink.create(reactor, repository, LOG_OT);
					}

					@Override
					public void initialize(OTUplink<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> uplink) {
						noFail(() -> initializeRepository((MySqlOTRepository<LogDiff<CubeDiff>>) uplink.getRepository()));
					}
				}},

			// Linear
/*
			new Object[]{
				"Linear graph",
				new UplinkFactory<CubeMySqlOTUplink>() {
					@Override
					public CubeMySqlOTUplink createUninitialized(Cube cube) {
						return CubeMySqlOTUplink.builder(cube.getReactor(), EXECUTOR, DATA_SOURCE, PrimaryKeyJsonCodecFactory.ofCube(cube))
							.withMeasuresValidator(MeasuresValidator.ofCube(cube))
							.build();
					}

					@Override
					public void initialize(CubeMySqlOTUplink uplink) {
						noFail(() -> initializeUplink(uplink));
					}
				}
			},
*/

			new Object[]{
				"etcd graph",
				new UplinkFactory<CubeEtcdOTUplink>() {
					@Override
					public CubeEtcdOTUplink createUninitialized(Cube cube) {
						return CubeEtcdOTUplink.builder(cube.getReactor(), ETCD_CLIENT, byteSequenceFrom("test."))
							.withChunkCodecsFactoryJson(cube)
							.withMeasuresValidator(MeasuresValidator.ofCube(cube))
							.build();
					}

					@Override
					public void initialize(CubeEtcdOTUplink uplink) {
						noFail(uplink::delete);
					}
				}
			}
		);
	}

	public interface UplinkFactory<U extends AsyncOTUplink<Long, LogDiff<CubeDiff>, ?>> {
		default U create(Cube cube) {
			U uplink = createUninitialized(cube);
			initialize(uplink);
			return uplink;
		}

		U createUninitialized(Cube cube);

		void initialize(U uplink);
	}

}
