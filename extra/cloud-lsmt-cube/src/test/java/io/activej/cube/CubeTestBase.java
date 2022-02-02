package io.activej.cube;

import io.activej.codegen.DefiningClassLoader;
import io.activej.cube.linear.CubeUplinkMySql;
import io.activej.cube.linear.MeasuresValidator;
import io.activej.cube.linear.PrimaryKeyCodecs;
import io.activej.cube.ot.CubeDiff;
import io.activej.cube.ot.CubeDiffCodec;
import io.activej.cube.ot.CubeDiffScheme;
import io.activej.cube.ot.CubeOT;
import io.activej.etl.LogDiff;
import io.activej.etl.LogDiffCodec;
import io.activej.etl.LogOT;
import io.activej.eventloop.Eventloop;
import io.activej.ot.OTCommit;
import io.activej.ot.repository.OTRepositoryMySql;
import io.activej.ot.system.OTSystem;
import io.activej.ot.uplink.OTUplink;
import io.activej.ot.uplink.OTUplinkImpl;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
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

import static io.activej.cube.TestUtils.*;
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
	public UplinkFactory<OTUplink<Long, LogDiff<CubeDiff>, ?>> uplinkFactory;

	public static final Eventloop EVENTLOOP = Eventloop.getCurrentEventloop();
	public static final Executor EXECUTOR = Executors.newCachedThreadPool();
	public static final DataSource DATA_SOURCE;

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
						new UplinkFactory<OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>>>() {
							@Override
							public OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> createUninitialized(Cube cube) {
								Eventloop eventloop = Eventloop.getCurrentEventloop();
								OTRepositoryMySql<LogDiff<CubeDiff>> repository = OTRepositoryMySql.create(eventloop, EXECUTOR, DATA_SOURCE, new IdGeneratorStub(),
										LOG_OT, LogDiffCodec.create(CubeDiffCodec.create(cube)));
								return OTUplinkImpl.create(repository, LOG_OT);
							}

							@Override
							public void initialize(OTUplinkImpl<Long, LogDiff<CubeDiff>, OTCommit<Long, LogDiff<CubeDiff>>> uplink) {
								noFail(() -> initializeRepository((OTRepositoryMySql<LogDiff<CubeDiff>>) uplink.getRepository()));
							}
						}},

				// Linear
				new Object[]{
						"Linear graph",
						new UplinkFactory<CubeUplinkMySql>() {
							@Override
							public CubeUplinkMySql createUninitialized(Cube cube) {
								return CubeUplinkMySql.create(EXECUTOR, DATA_SOURCE, PrimaryKeyCodecs.ofCube(cube))
										.withMeasuresValidator(MeasuresValidator.ofCube(cube));
							}

							@Override
							public void initialize(CubeUplinkMySql uplink) {
								noFail(() -> initializeUplink(uplink));
							}
						}
				}
		);
	}

	protected interface UplinkFactory<U extends OTUplink<Long, LogDiff<CubeDiff>, ?>> {
		default U create(Cube cube) {
			U uplink = createUninitialized(cube);
			initialize(uplink);
			return uplink;
		}

		U createUninitialized(Cube cube);

		void initialize(U uplink);
	}

}
