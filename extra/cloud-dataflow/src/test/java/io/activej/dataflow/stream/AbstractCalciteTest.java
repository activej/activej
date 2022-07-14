package io.activej.dataflow.stream;

import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.SqlDataflow;
import io.activej.dataflow.calcite.DataflowSchema;
import io.activej.dataflow.calcite.DataflowTable;
import io.activej.dataflow.calcite.RecordFunction;
import io.activej.dataflow.calcite.inject.CalciteModule;
import io.activej.dataflow.calcite.inject.CalciteServerModule;
import io.activej.dataflow.calcite.inject.SerializersModule;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.node.NodeSort.StreamSorterStorageFactory;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.Modules;
import io.activej.record.Record;
import io.activej.record.RecordScheme;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.RecordComponent;
import java.net.InetSocketAddress;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.activej.common.Checks.checkState;
import static io.activej.dataflow.helper.StreamMergeSorterStorageStub.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.proto.ProtobufUtils.ofObject;
import static io.activej.dataflow.stream.AbstractCalciteTest.MatchType.TYPE_1;
import static io.activej.dataflow.stream.AbstractCalciteTest.MatchType.TYPE_2;
import static io.activej.dataflow.stream.DataflowTest.createCommon;
import static io.activej.dataflow.stream.DataflowTest.getFreeListenAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractCalciteTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static final String STUDENT_TABLE_NAME = "student";
	public static final String DEPARTMENT_TABLE_NAME = "department";
	public static final String REGISTRY_TABLE_NAME = "registry";
	public static final String USER_PROFILES_TABLE_NAME = "profiles";

	protected static final List<Student> STUDENT_LIST = List.of(
			new Student(1, "John", "Doe", 1),
			new Student(2, "Bob", "Black", 2),
			new Student(3, "John", "Truman", 2));
	protected static final List<Department> DEPARTMENT_LIST = List.of(
			new Department(1, "Math"),
			new Department(2, "Language"),
			new Department(3, "History"));
	protected static final List<Registry> REGISTRY_LIST = List.of(
			new Registry(1, Map.of("John", 1, "Jack", 2, "Kevin", 7), List.of("google.com", "amazon.com")),
			new Registry(2, Map.of(), List.of("wikipedia.org")),
			new Registry(3, Map.of("Sarah", 53, "Rob", 7564, "John", 18, "Kevin", 7), List.of("yahoo.com", "ebay.com", "netflix.com")),
			new Registry(4, Map.of("Kevin", 8), List.of("google.com")));
	protected static final List<UserProfile> USER_PROFILES_LIST = List.of(
			new UserProfile("user1", new UserProfilePojo("test1", 1), Map.of(
					1, new UserProfileIntent(1, "test1", TYPE_1, new Limits(new float[]{1.2f, 3.4f}, System.currentTimeMillis())),
					2, new UserProfileIntent(2, "test2", TYPE_2, new Limits(new float[]{5.6f, 7.8f}, System.currentTimeMillis()))),
					System.currentTimeMillis()),
			new UserProfile("user2", new UserProfilePojo("test2", 2), Map.of(
					2, new UserProfileIntent(2, "test2", TYPE_2, new Limits(new float[]{4.3f, 2.1f}, System.currentTimeMillis())),
					3, new UserProfileIntent(3, "test3", TYPE_1, new Limits(new float[]{6.5f, 8.7f}, System.currentTimeMillis()))),
					System.currentTimeMillis())
	);

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	protected ExecutorService executor;
	protected ExecutorService sortingExecutor;
	protected SqlDataflow sqlDataflow;
	protected DataflowServer server;
	protected Injector serverInjector;

	@Before
	public final void setUp() throws Exception {
		executor = Executors.newSingleThreadExecutor();
		sortingExecutor = Executors.newSingleThreadExecutor();

		InetSocketAddress address = getFreeListenAddress();

		List<Partition> partitions = List.of(new Partition(address));
		Module common = createCommon(executor, sortingExecutor, temporaryFolder.newFolder().toPath(), partitions)
				.install(new SerializersModule())
				.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
				.bind(new Key<List<Partition>>() {}).toInstance(partitions)
				.bind(DataflowSchema.class).to(() -> DataflowSchema.create(
						Map.of(STUDENT_TABLE_NAME, DataflowTable.create(Student.class, new StudentToRecord(), ofObject(StudentToRecord::new)),
								DEPARTMENT_TABLE_NAME, DataflowTable.create(Department.class, new DepartmentToRecord(), ofObject(DepartmentToRecord::new)),
								REGISTRY_TABLE_NAME, DataflowTable.create(Registry.class, new RegistryToRecord(), ofObject(RegistryToRecord::new)),
								USER_PROFILES_TABLE_NAME, DataflowTable.create(UserProfile.class, new UserProfileToRecord(), ofObject(UserProfileToRecord::new)))))
				.build();

		Module clientModule = Modules.combine(common, new CalciteModule());
		sqlDataflow = Injector.of(clientModule).getInstance(SqlDataflow.class);


		Module serverModule = ModuleBuilder.create()
				.install(common)
				.install(new CalciteServerModule())
				.bind(datasetId(STUDENT_TABLE_NAME)).toInstance(STUDENT_LIST)
				.bind(datasetId(DEPARTMENT_TABLE_NAME)).toInstance(DEPARTMENT_LIST)
				.bind(datasetId(REGISTRY_TABLE_NAME)).toInstance(REGISTRY_LIST)
				.bind(datasetId(USER_PROFILES_TABLE_NAME)).toInstance(USER_PROFILES_LIST)
				.build()
				.overrideWith(getAdditionalServerModule());

		serverInjector = Injector.of(serverModule);
		server = serverInjector.getInstance(DataflowServer.class).withListenAddress(address);

		onSetUp();
	}

	protected void onSetUp() throws Exception {
	}

	@After
	public final void tearDown() {
		executor.shutdownNow();
		sortingExecutor.shutdownNow();

		onTearDown();
	}

	protected void onTearDown() {
	}

	protected Module getAdditionalServerModule() {
		return Module.empty();
	}


	@SerializeRecord
	public record Student(int id, String firstName, String lastName, int dept) {
	}

	@SerializeRecord
	public record Department(int id, String departmentName) {
	}

	@SerializeRecord
	public record Registry(int id, Map<String, Integer> counters, List<String> domains) {
	}

	@SerializeRecord
	public record UserProfile(String id, UserProfilePojo pojo, Map<Integer, UserProfileIntent> intents,
							  long timestamp) {
	}

	@SerializeRecord
	public record UserProfilePojo(String value1, int value2) {
	}

	@SerializeRecord
	public record UserProfileIntent(int campaignId, String keyword, MatchType matchType, Limits limits) {
	}

	public enum MatchType {
		TYPE_1, TYPE_2
	}

	@SerializeRecord
	public record Limits(float[] buckets, long timestamp) {
	}

	@Test
	public void testSelectAllStudents() {
		QueryResult result = query("SELECT * FROM student");

		QueryResult expected = studentsToQueryResult(STUDENT_LIST);

		assertEquals(expected, result);
	}

	@Test
	public void testSelectAllDepartments() {
		QueryResult result = query("SELECT * FROM department");

		QueryResult expected = departmentsToQueryResult(DEPARTMENT_LIST);

		assertEquals(expected, result);
	}

	@Test
	public void testSelectDepartmentsFields() {
		QueryResult result = query("SELECT departmentName FROM department");

		List<Object[]> columnValues = new ArrayList<>();
		for (Department department : DEPARTMENT_LIST) {
			columnValues.add(new Object[]{department.departmentName});
		}

		QueryResult expected = new QueryResult(List.of("departmentName"), columnValues);

		assertEquals(expected, result);
	}

	@Test
	public void testSelectStudentsFields() {
		QueryResult result = query("SELECT dept, firstName FROM student");

		List<Object[]> columnValues = new ArrayList<>();
		for (Student student : STUDENT_LIST) {
			columnValues.add(new Object[]{student.dept, student.firstName});
		}

		QueryResult expected = new QueryResult(List.of("dept", "firstName"), columnValues);

		assertEquals(expected, result);
	}

	@Test
	public void testSubSelect() {
		QueryResult result = query("""
				SELECT firstName, id
				FROM (SELECT id, firstName, dept from student)
				""");

		List<Object[]> columnValues = new ArrayList<>();
		for (Student student : STUDENT_LIST) {
			columnValues.add(new Object[]{student.firstName, student.id});
		}

		QueryResult expected = new QueryResult(List.of("firstName", "id"), columnValues);

		assertEquals(expected, result);
	}

	// region WhereEqualTrue
	@Test
	public void testWhereEqualTrue() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE 1 = 1
				""");

		assertWhereEqualTrue(result);
	}

	@Test
	public void testWhereEqualTruePrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE 1 = ?
						""",
				stmt -> stmt.setInt(1, 1));

		assertWhereEqualTrue(result);
	}

	private void assertWhereEqualTrue(QueryResult result) {
		QueryResult expected = studentsToQueryResult(STUDENT_LIST);

		assertEquals(expected, result);
	}
	// endregion

	// region WhereEqualFalse
	@Test
	public void testWhereEqualFalse() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE 1 = 2
				""");

		assertTrue(result.isEmpty());
	}

	@Test
	public void testWhereEqualFalsePrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE 1 = ?
						""",
				stmt -> stmt.setInt(1, 2));

		assertWhereEqualFalse(result);
	}

	private void assertWhereEqualFalse(QueryResult result) {
		assertTrue(result.isEmpty());
	}
	// endregion

	// region WhereEqualScalar
	@Test
	public void testWhereEqualScalar() {
		QueryResult result = query("""
				SELECT firstName, lastName FROM student
				WHERE id = 2
				""");

		assertWhereEqualScalar(result);
	}

	@Test
	public void testWhereEqualScalarPrepared() {
		QueryResult result = queryPrepared("""
						SELECT firstName, lastName FROM student
						WHERE id = ?
						""",
				stmt -> stmt.setInt(1, 2));

		assertWhereEqualScalar(result);
	}

	private void assertWhereEqualScalar(QueryResult result) {
		Student student = STUDENT_LIST.get(1);
		QueryResult expected = new QueryResult(List.of("firstName", "lastName"),
				List.<Object[]>of(new Object[]{student.firstName, student.lastName}));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testWhereEqualField() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id = dept
				""");

		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(0, 2));

		assertEquals(expected, result);
	}

	// region WhereNotEqualTrue
	@Test
	public void testWhereNotEqualTrue() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE 1 <> 1
				""");

		assertWhereNotEqualTrue(result);
	}

	@Test
	public void testWhereNotEqualTruePrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE 1 <> ?
						""",
				stmt -> stmt.setInt(1, 1));

		assertWhereNotEqualTrue(result);
	}

	private void assertWhereNotEqualTrue(QueryResult result) {
		assertTrue(result.isEmpty());
	}
	// endregion

	// region WhereNotEqualFalse
	@Test
	public void testWhereNotEqualFalse() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE 1 <> 2
				""");

		assertWhereNotEqualFalse(result);
	}

	@Test
	public void testWhereNotEqualFalsePrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE 1 <> ?
						""",
				stmt -> stmt.setInt(1, 2));

		assertWhereNotEqualFalse(result);
	}

	private void assertWhereNotEqualFalse(QueryResult result) {
		QueryResult expected = studentsToQueryResult(STUDENT_LIST);

		assertEquals(expected, result);
	}
	// endregion

	// region WhereNotEqualScalar
	@Test
	public void testWhereNotEqualScalar() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id <> 2
				""");

		assertWhereNotEqualScalar(result);
	}

	@Test
	public void testWhereNotEqualScalarPrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE id <> ?
						""",
				stmt -> stmt.setInt(1, 2));

		assertWhereNotEqualScalar(result);
	}

	private void assertWhereNotEqualScalar(QueryResult result) {
		QueryResult expected = studentsToQueryResult(List.of(STUDENT_LIST.get(0), STUDENT_LIST.get(2)));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testWhereNotEqualField() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id <> dept
				""");

		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(2, 3));

		assertEquals(expected, result);
	}

	// region WhereGreaterThanScalar
	@Test
	public void testWhereGreaterThanScalar() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id > 1
				""");

		assertWhereGreaterThanScalar(result);
	}

	@Test
	public void testWhereGreaterThanScalarPrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE id > ?
						""",
				stmt -> stmt.setInt(1, 1));

		assertWhereGreaterThanScalar(result);
	}

	private void assertWhereGreaterThanScalar(QueryResult result) {
		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(1, 3));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testWhereGreaterThanField() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id > dept
				""");

		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(2, 3));

		assertEquals(expected, result);
	}

	// region WhereGreaterThanOrEqualScalar
	@Test
	public void testWhereGreaterThanOrEqualScalar() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id >= 2
				""");

		assertWhereGreaterThanOrEqualScalar(result);
	}

	@Test
	public void testWhereGreaterThanOrEqualScalarPrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE id >= ?
						""",
				stmt -> stmt.setInt(1, 2));

		assertWhereGreaterThanOrEqualScalar(result);
	}

	private void assertWhereGreaterThanOrEqualScalar(QueryResult result) {
		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(1, 3));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testWhereGreaterThanOrEqualField() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE dept >= id
				""");

		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(0, 2));

		assertEquals(expected, result);
	}

	// region WhereLessThanScalar
	@Test
	public void testWhereLessThanScalar() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id < 2
				""");

		assertWhereLessThanScalar(result);
	}

	@Test
	public void testWhereLessThanScalarPrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE id < ?
						""",
				stmt -> stmt.setInt(1, 2));

		assertWhereLessThanScalar(result);
	}

	private void assertWhereLessThanScalar(QueryResult result) {
		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(0, 1));

		assertEquals(expected, result);
	}
	// endregion

	// region WhereLessThanOrEqualScalar
	@Test
	public void testWhereLessThanOrEqualScalar() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id <= 2
				""");

		assertWhereLessThanOrEqualScalar(result);
	}

	@Test
	public void testWhereLessThanOrEqualScalarPrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE id <= ?
						""",
				stmt -> stmt.setInt(1, 2));

		assertWhereLessThanOrEqualScalar(result);
	}

	private void assertWhereLessThanOrEqualScalar(QueryResult result) {
		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(0, 2));

		assertEquals(expected, result);
	}
	// endregion

	// region WhereAndEqual
	@Test
	public void testWhereAndEqual() {
		QueryResult result = query("""
				SELECT id FROM student
				WHERE firstName = 'John' AND dept = 1
				""");

		assertWhereAndEqual(result);
	}

	@Test
	public void testWhereAndEqualPrepared() {
		QueryResult result = queryPrepared("""
						SELECT id FROM student
						WHERE firstName = ? AND dept = ?
						""",
				stmt -> {
					stmt.setString(1, "John");
					stmt.setInt(2, 1);
				});

		assertWhereAndEqual(result);
	}

	private void assertWhereAndEqual(QueryResult result) {
		QueryResult expected = new QueryResult(List.of("id"),
				List.<Object[]>of(new Object[]{STUDENT_LIST.get(0).id}));

		assertEquals(expected, result);
	}
	// endregion

	// region WhereOrEqual
	@Test
	public void testWhereOrEqual() {
		QueryResult result = query("""
				SELECT id FROM student
				WHERE firstName = 'Bob' OR dept = 2
				""");

		assertWhereOrEqual(result);
	}

	@Test
	public void testWhereOrEqualPrepared() {
		QueryResult result = queryPrepared("""
						SELECT id FROM student
						WHERE firstName = ? OR dept = ?
						""",
				stmt -> {
					stmt.setString(1, "Bob");
					stmt.setInt(2, 2);
				});

		assertWhereOrEqual(result);
	}

	private void assertWhereOrEqual(QueryResult result) {
		QueryResult expected = new QueryResult(List.of("id"),
				List.of(
						new Object[]{STUDENT_LIST.get(1).id},
						new Object[]{STUDENT_LIST.get(2).id}
				));

		assertEquals(expected, result);
	}
	// endregion

	// region WhereBetween
	@Test
	public void testWhereBetween() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id BETWEEN 1 AND 2
				""");

		assertWhereBetween(result);
	}

	@Test
	public void testWhereBetweenPrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE id BETWEEN ? AND ?
						""",
				stmt -> {
					stmt.setInt(1, 1);
					stmt.setInt(2, 2);
				});

		assertWhereBetween(result);
	}

	private void assertWhereBetween(QueryResult result) {
		QueryResult expected = studentsToQueryResult(STUDENT_LIST.subList(0, 2));

		assertEquals(expected, result);
	}
	// endregion

	// region WhereIn
	@Test
	public void testWhereIn() {
		QueryResult result = query("""
				SELECT * FROM student
				WHERE id IN (1, 3, 5)
				""");

		assertWhereIn(result);
	}

	@Test
	public void testWhereInPrepared() {
		QueryResult result = queryPrepared("""
						SELECT * FROM student
						WHERE id IN (?, ?, ?)
						""",
				stmt -> {
					stmt.setInt(1, 1);
					stmt.setInt(2, 3);
					stmt.setInt(3, 5);
				});

		assertWhereIn(result);
	}

	private void assertWhereIn(QueryResult result) {
		QueryResult expected = studentsToQueryResult(List.of(STUDENT_LIST.get(0), STUDENT_LIST.get(2)));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testWhereLikeStartsWithJ() {
		doTestWhereLike("J%", 1, 3);
	}

	@Test
	public void testWhereLikeEndsWithHn() {
		doTestWhereLike("%hn", 1, 3);
	}

	@Test
	public void testWhereStartsWithB() {
		doTestWhereLike("B%", 2);
	}

	@Test
	public void testWhereSecondLetterIsO() {
		doTestWhereLike("_o%", 1, 2, 3);
	}

	@Test
	public void testWhereMatchAll() {
		doTestWhereLike("%", 1, 2, 3);
	}

	private void doTestWhereLike(String firstNamePattern, int... expectedIds) {
		QueryResult result = query("SELECT * FROM student " +
				"WHERE firstName LIKE '" + firstNamePattern + '\'');

		QueryResult expected = studentsToQueryResult(Arrays.stream(expectedIds).mapToObj(id -> STUDENT_LIST.get(id - 1)).toList());

		assertEquals(expected, result);
	}

	@Test
	public void testWhereNoMatch() {
		QueryResult result = query("SELECT * FROM student " +
				"WHERE firstName LIKE '" + "A" + '\'');

		assertTrue(result.isEmpty());
	}

	@Test
	public void testMapGetQuery() {
		QueryResult result = query("""
				SELECT id, MAP_GET(counters, 'John') FROM registry
				""");

		List<Object[]> columnValues = new ArrayList<>(result.columnValues.size());
		for (Registry registry : REGISTRY_LIST) {
			columnValues.add(new Object[]{registry.id, registry.counters.get("John")});
		}

		QueryResult expected = new QueryResult(List.of("id", "counters.get(John)"), columnValues);

		assertEquals(expected, result);
	}

	// region MapGetInWhereClause
	@Test
	public void testMapGetInWhereClause() {
		QueryResult result = query("""
				SELECT id FROM registry
				WHERE MAP_GET(counters, 'Kevin') = 7
				""");

		assertMapGetInWhereClause(result);
	}

	@Test
	public void testMapGetInWhereClausePrepared() {
		QueryResult result = queryPrepared("""
						SELECT id FROM registry
						WHERE MAP_GET(counters, ?) = ?
						""",
				stmt -> {
					stmt.setString(1, "Kevin");
					stmt.setInt(2, 7);
				});

		assertMapGetInWhereClause(result);
	}

	private void assertMapGetInWhereClause(QueryResult result) {
		QueryResult expected = new QueryResult(List.of("id"),
				List.of(
						new Object[]{REGISTRY_LIST.get(0).id},
						new Object[]{REGISTRY_LIST.get(2).id}
				));

		assertEquals(result, expected);
	}
	// endregion

	// region ListGetQuery
	@Test
	public void testListGetQuery() {
		QueryResult result = query("""
				SELECT id, LIST_GET(domains, 1) FROM registry
				""");

		assertListGetQuery(result, "1");
	}

	@Test
	public void testListGetQueryPrepared() {
		QueryResult result = queryPrepared("""
						SELECT id, LIST_GET(domains, ?) FROM registry
						""",
				stmt -> stmt.setInt(1, 1));

		assertListGetQuery(result, "?");
	}

	private void assertListGetQuery(QueryResult result, String index) {
		List<Object[]> columnValues = new ArrayList<>(result.columnValues.size());
		for (Registry registry : REGISTRY_LIST) {
			String domain = registry.domains.size() > 1 ? registry.domains.get(1) : null;
			columnValues.add(new Object[]{registry.id, domain});
		}

		QueryResult expected = new QueryResult(List.of("id", "domains.get(" + index + ')'), columnValues);

		assertEquals(expected, result);
	}
	// endregion

	// region ListGetInWhereClause
	@Test
	public void testListGetInWhereClause() {
		QueryResult result = query("""
				SELECT id FROM registry
				WHERE LIST_GET(domains, 0) = 'google.com'
				""");

		assertListGetInWhereClause(result);
	}

	@Test
	public void testListGetInWhereClausePrepared() {
		QueryResult result = queryPrepared("""
						SELECT id FROM registry
						WHERE LIST_GET(domains, ?) = ?
						""",
				stmt -> {
					stmt.setInt(1, 0);
					stmt.setString(2, "google.com");
				});

		assertListGetInWhereClause(result);
	}

	private void assertListGetInWhereClause(QueryResult result) {
		QueryResult expected = new QueryResult(List.of("id"),
				List.of(
						new Object[]{REGISTRY_LIST.get(0).id},
						new Object[]{REGISTRY_LIST.get(3).id}
				));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testJoin() {
		QueryResult result = query("""
				SELECT * FROM student
				JOIN department
				ON student.dept = department.id
				""");

		List<Object[]> expectedColumnValues = new ArrayList<>(3);

		Student firstStudent = STUDENT_LIST.get(0);
		Department firstDepartment = DEPARTMENT_LIST.get(0);
		Student secondStudent = STUDENT_LIST.get(1);
		Department secondDepartment = DEPARTMENT_LIST.get(1);
		Student thirdStudent = STUDENT_LIST.get(2);

		expectedColumnValues.add(new Object[]{
				firstStudent.id, firstStudent.firstName, firstStudent.lastName, firstStudent.dept,
				firstDepartment.id, firstDepartment.departmentName
		});
		expectedColumnValues.add(new Object[]{
				secondStudent.id, secondStudent.firstName, secondStudent.lastName, secondStudent.dept,
				secondDepartment.id, secondDepartment.departmentName
		});
		expectedColumnValues.add(new Object[]{
				thirdStudent.id, thirdStudent.firstName, thirdStudent.lastName, thirdStudent.dept,
				secondDepartment.id, secondDepartment.departmentName
		});

		QueryResult expected = new QueryResult(List.of("left.id", "left.firstName", "left.lastName", "left.dept", "right.id", "right.departmentName"), expectedColumnValues);

		assertEquals(expected, result);
	}

	@Test
	public void testJoinByFields() {
		QueryResult result = query("""
				SELECT student.id, department.departmentName FROM student
				JOIN department
				ON student.dept = department.id
				""");

		Student firstStudent = STUDENT_LIST.get(0);
		Department firstDepartment = DEPARTMENT_LIST.get(0);
		Student secondStudent = STUDENT_LIST.get(1);
		Department secondDepartment = DEPARTMENT_LIST.get(1);
		Student thirdStudent = STUDENT_LIST.get(2);

		QueryResult expected = new QueryResult(List.of("left.id", "right.departmentName"),
				List.of(
						new Object[]{firstStudent.id, firstDepartment.departmentName},
						new Object[]{secondStudent.id, secondDepartment.departmentName},
						new Object[]{thirdStudent.id, secondDepartment.departmentName}
				));

		assertEquals(expected, result);
	}

	@Test
	public void testOrderBy() {
		QueryResult result = query("""
				SELECT * FROM student
				ORDER BY firstName ASC, id DESC
				""");

		QueryResult expected = studentsToQueryResult(List.of(STUDENT_LIST.get(1), STUDENT_LIST.get(2), STUDENT_LIST.get(0)));

		assertEquals(expected, result);
	}

	@Test
	public void testPojoFieldSelect() {
		QueryResult result = query("""
				SELECT id, profiles.pojo.value1, profiles.pojo.value2 FROM profiles
				""");

		List<Object[]> expectedColumnValues = new ArrayList<>(USER_PROFILES_LIST.size());

		for (UserProfile userProfile : USER_PROFILES_LIST) {
			expectedColumnValues.add(new Object[]{
					userProfile.id,
					userProfile.pojo.value1,
					userProfile.pojo.value2
			});
		}

		QueryResult expected = new QueryResult(List.of("id", "pojo.value1", "pojo.value2"), expectedColumnValues);

		assertEquals(expected, result);
	}

	// region PojoFieldInWhereClause
	@Test
	public void testPojoFieldInWhereClause() {
		QueryResult result = query("""
				SELECT id
				FROM profiles
				WHERE profiles.pojo.value1 = 'test1'
				""");

		assertPojoFieldInWhereClause(result);
	}

	@Test
	public void testPojoFieldInWhereClausePrepared() {
		QueryResult result = queryPrepared("""
						SELECT id
						FROM profiles
						WHERE profiles.pojo.value1 = ?
						""",
				stmt -> stmt.setString(1, "test1"));

		assertPojoFieldInWhereClause(result);
	}

	private void assertPojoFieldInWhereClause(QueryResult result) {
		QueryResult expected = new QueryResult(List.of("id"), List.<Object[]>of(new Object[]{USER_PROFILES_LIST.get(0).id}));

		assertEquals(expected, result);
	}
	// endregion

	// region UserProfilesSelect
	@Test
	public void testUserProfilesSelect() {
		QueryResult result = query("""
				SELECT id, MAP_GET(intents, 1).keyword, MAP_GET(intents, 1).matchType FROM profiles
				""");

		assertUserProfilesSelect(result, "1");
	}

	@Test
	public void testUserProfilesSelectPrepared() {
		QueryResult result = queryPrepared("""
						SELECT id, MAP_GET(intents, ?).keyword, MAP_GET(intents, ?).matchType FROM profiles
						""",
				stmt -> {
					stmt.setInt(1, 1);
					stmt.setInt(2, 1);
				});

		assertUserProfilesSelect(result, "?");
	}

	private void assertUserProfilesSelect(QueryResult result, String key) {
		List<Object[]> expectedColumnValues = new ArrayList<>(USER_PROFILES_LIST.size());

		for (UserProfile userProfile : USER_PROFILES_LIST) {
			UserProfileIntent intent = userProfile.intents.get(1);
			expectedColumnValues.add(new Object[]{
					userProfile.id,
					intent == null ? null : intent.keyword,
					intent == null ? null : intent.matchType
			});
		}

		QueryResult expected = new QueryResult(List.of("id", "intents.get(" + key + ").keyword", "intents.get(" + key + ").matchType"), expectedColumnValues);

		assertEquals(expected, result);
	}
	// endregion

	// region UserProfilesInWhereClause
	@Test
	public void testUserProfilesInWhereClause() {
		QueryResult result = query("""
				SELECT id
				FROM profiles
				WHERE MAP_GET(intents, 1).keyword = 'test1'
				""");

		assertUserProfilesInWhereClause(result);
	}

	@Test
	public void testUserProfilesInWhereClausePrepared() {
		QueryResult result = queryPrepared("""
						SELECT id
						FROM profiles
						WHERE MAP_GET(intents, ?).keyword = ?
						""",
				stmt -> {
					stmt.setInt(1, 1);
					stmt.setString(2, "test1");
				});

		assertUserProfilesInWhereClause(result);
	}

	private void assertUserProfilesInWhereClause(QueryResult result) {
		QueryResult expected = new QueryResult(List.of("id"), List.<Object[]>of(new Object[]{USER_PROFILES_LIST.get(0).id}));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testCountAllStudents() {
		QueryResult result = query("SELECT COUNT(*) FROM student");

		QueryResult expected = new QueryResult(List.of("COUNT(*)"), List.<Object[]>of(new Object[]{3L}));

		assertEquals(expected, result);
	}

	// region CountMapGet
	@Test
	public void testCountMapGet() {
		QueryResult result = query("SELECT COUNT(MAP_GET(counters, 'John')) FROM registry");

		assertCountMapGet(result, "John");
	}

	@Test
	public void testCountMapGetPrepared() {
		QueryResult result = queryPrepared("SELECT COUNT(MAP_GET(counters, ?)) FROM registry",
				stmt -> stmt.setString(1, "John"));

		assertCountMapGet(result, "?");
	}

	private void assertCountMapGet(QueryResult result, String key) {
		QueryResult expected = new QueryResult(List.of("COUNT(counters.get(" + key + "))"), List.<Object[]>of(new Object[]{2L}));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testSumStudentsId() {
		QueryResult result = query("SELECT SUM(id) FROM student");

		QueryResult expected = new QueryResult(List.of("SUM(id)"), List.<Object[]>of(new Object[]{6L}));

		assertEquals(expected, result);
	}

	// region SumPojoValues
	@Test
	public void testSumPojoValues() {
		QueryResult result = query("SELECT SUM(MAP_GET(intents, 2).campaignId) FROM profiles");

		assertSumPojoValues(result, "2");
	}

	@Test
	public void testSumPojoValuesPrepared() {
		QueryResult result = queryPrepared("SELECT SUM(MAP_GET(intents, ?).campaignId) FROM profiles",
				stmt -> stmt.setInt(1, 2));

		assertSumPojoValues(result, "?");
	}

	private void assertSumPojoValues(QueryResult result, String key) {
		QueryResult expected = new QueryResult(List.of("SUM(intents.get(" + key + ").campaignId)"), List.<Object[]>of(new Object[]{4L}));

		assertEquals(expected, result);
	}
	// endregion

	@Test
	public void testAvgStudentsDepts() {
		QueryResult result = query("SELECT AVG(dept) FROM student");

		QueryResult expected = new QueryResult(List.of("AVG(dept)"), List.<Object[]>of(new Object[]{5d / 3}));

		assertEquals(expected, result);
	}

	@Test
	public void testMinStudentsId() {
		QueryResult result = query("SELECT MIN(id) FROM student");

		QueryResult expected = new QueryResult(List.of("MIN(id)"), List.<Object[]>of(new Object[]{1}));

		assertEquals(expected, result);
	}

	@Test
	public void testMaxStudentsId() {
		QueryResult result = query("SELECT MAX(id) FROM student");

		QueryResult expected = new QueryResult(List.of("MAX(id)"), List.<Object[]>of(new Object[]{3}));

		assertEquals(expected, result);
	}

	// region SelectPojo
	@Test
	public void testSelectPojo() {
		QueryResult result = query("""
				SELECT pojo
				FROM profiles
				WHERE id = 'user1'
				""");

		assertSelectPojo(result);
	}

	@Test
	public void testSelectPojoPrepared() {
		QueryResult result = queryPrepared("""
				SELECT pojo
				FROM profiles
				WHERE id = ?
				""",
				stmt -> stmt.setString(1, "user1"));

		assertSelectPojo(result);
	}

	private void assertSelectPojo(QueryResult result) {
		QueryResult expected = new QueryResult(List.of("pojo"), List.<Object[]>of(new Object[]{new UserProfilePojo("test1", 1)}));

		assertEquals(expected, result);
	}
	// endregion

	protected abstract QueryResult query(String sql);

	protected abstract QueryResult queryPrepared(String sql, ParamsSetter paramsSetter);

	public static final class QueryResult {
		private static final QueryResult EMPTY = new QueryResult(Collections.emptyList(), Collections.emptyList());

		private final List<String> columnNames;
		private final List<Object[]> columnValues;

		public QueryResult(List<String> columnNames, List<Object[]> columnValues) {
			if (!columnValues.isEmpty()) {
				int size = columnNames.size();
				for (Object[] columnValue : columnValues) {
					assertEquals(size, columnValue.length);
				}
			}

			this.columnNames = columnNames;
			this.columnValues = new ArrayList<>(columnValues.size());

			for (Object[] columnValue : columnValues) {
				Object[] newColumnValue = null;

				for (int i = 0; i < columnValue.length; i++) {
					Object o = columnValue[i];
					if (o instanceof Enum<?> anEnum) {
						// JDBC returns enums as Strings
						if (newColumnValue == null) {
							newColumnValue = Arrays.copyOf(columnValue, columnValue.length);
						}
						newColumnValue[i] = anEnum.name();
					}
				}

				this.columnValues.add(newColumnValue == null ? columnValue : newColumnValue);
			}
		}

		public static QueryResult empty() {
			return EMPTY;
		}

		public boolean isEmpty() {
			return columnValues.isEmpty();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			QueryResult that = (QueryResult) o;

			if (!columnNames.equals(that.columnNames)) return false;

			if (columnValues.size() != that.columnValues.size()) return false;

			for (int i = 0; i < columnValues.size(); i++) {
				Object[] columnValue = columnValues.get(i);
				Object[] thatColumnValue = that.columnValues.get(i);

				if (!Arrays.equals(columnValue, thatColumnValue)) return false;
			}

			return true;
		}

		@Override
		public String toString() {
			return "QueryResult{" +
					"columnNames=" + columnNames +
					", columnValues=" + columnValues.stream()
					.map(Arrays::toString)
					.toList() +
					'}';
		}
	}

	public static final class StudentToRecord implements RecordFunction<Student> {
		private final RecordScheme scheme = ofJavaRecord(Student.class);

		@Override
		public Record apply(Student student) {
			Record record = scheme.record();
			record.set("id", student.id);
			record.set("firstName", student.firstName);
			record.set("lastName", student.lastName);
			record.set("dept", student.dept);
			return record;
		}

		@Override
		public RecordScheme getScheme() {
			return scheme;
		}
	}

	public static final class DepartmentToRecord implements RecordFunction<Department> {
		private final RecordScheme scheme = ofJavaRecord(Department.class);

		@Override
		public Record apply(Department department) {
			Record record = scheme.record();
			record.set("id", department.id);
			record.set("departmentName", department.departmentName);
			return record;
		}

		@Override
		public RecordScheme getScheme() {
			return scheme;
		}
	}

	public static final class RegistryToRecord implements RecordFunction<Registry> {
		private final RecordScheme scheme = ofJavaRecord(Registry.class);

		@Override
		public Record apply(Registry registry) {
			Record record = scheme.record();
			record.set("id", registry.id);
			record.set("counters", registry.counters);
			record.set("domains", registry.domains);
			return record;
		}

		@Override
		public RecordScheme getScheme() {
			return scheme;
		}
	}

	public static final class UserProfileToRecord implements RecordFunction<UserProfile> {
		private final RecordScheme scheme = ofJavaRecord(UserProfile.class);

		@Override
		public Record apply(UserProfile userProfile) {
			Record record = scheme.record();
			record.set("id", userProfile.id);
			record.set("pojo", userProfile.pojo);
			record.set("intents", userProfile.intents);
			record.set("timestamp", userProfile.timestamp);
			return record;
		}

		@Override
		public RecordScheme getScheme() {
			return scheme;
		}
	}

	private static QueryResult departmentsToQueryResult(List<Department> departments) {
		List<String> columnNames = Arrays.asList("id", "departmentName");
		List<Object[]> columnValues = new ArrayList<>(departments.size());

		for (Department department : departments) {
			columnValues.add(new Object[]{department.id, department.departmentName});
		}

		return new QueryResult(columnNames, columnValues);
	}

	private static QueryResult studentsToQueryResult(List<Student> students) {
		List<String> columnNames = Arrays.asList("id", "firstName", "lastName", "dept");
		List<Object[]> columnValues = new ArrayList<>(students.size());

		for (Student student : students) {
			columnValues.add(new Object[]{student.id, student.firstName, student.lastName, student.dept});
		}

		return new QueryResult(columnNames, columnValues);
	}

	private static RecordScheme ofJavaRecord(Class<?> recordClass) {
		checkState(recordClass.isRecord());

		RecordScheme recordScheme = RecordScheme.create();
		RecordComponent[] recordComponents = recordClass.getRecordComponents();

		for (RecordComponent recordComponent : recordComponents) {
			recordScheme.addField(recordComponent.getName(), recordComponent.getGenericType());
		}

		return recordScheme.build();
	}

	protected interface ParamsSetter {
		void setValues(PreparedStatement stmt) throws SQLException;
	}
}
