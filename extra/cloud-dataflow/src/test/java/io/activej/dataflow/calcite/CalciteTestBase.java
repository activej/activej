package io.activej.dataflow.calcite;

import io.activej.codegen.DefiningClassLoader;
import io.activej.dataflow.DataflowServer;
import io.activej.dataflow.ISqlDataflow;
import io.activej.dataflow.calcite.inject.CalciteClientModule;
import io.activej.dataflow.calcite.inject.CalciteServerModule;
import io.activej.dataflow.calcite.operand.Operand;
import io.activej.dataflow.calcite.operand.impl.RecordField;
import io.activej.dataflow.calcite.operand.impl.Scalar;
import io.activej.dataflow.calcite.table.AbstractDataflowTable;
import io.activej.dataflow.calcite.table.DataflowPartitionedTable;
import io.activej.dataflow.calcite.table.DataflowTable;
import io.activej.dataflow.calcite.where.WherePredicate;
import io.activej.dataflow.calcite.where.impl.*;
import io.activej.dataflow.graph.Partition;
import io.activej.dataflow.inject.DatasetIdModule;
import io.activej.dataflow.node.StreamSorterStorageFactory;
import io.activej.datastream.processor.reducer.BinaryAccumulatorReducer;
import io.activej.datastream.supplier.StreamSuppliers;
import io.activej.inject.Injector;
import io.activej.inject.Key;
import io.activej.inject.module.Module;
import io.activej.inject.module.ModuleBuilder;
import io.activej.inject.module.Modules;
import io.activej.record.Record;
import io.activej.serializer.annotations.SerializeRecord;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.ClassBuilderConstantsRule;
import io.activej.test.rules.EventloopRule;
import io.activej.types.TypeT;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.LongStream;

import static io.activej.dataflow.calcite.CalciteTestBase.MatchType.TYPE_1;
import static io.activej.dataflow.calcite.CalciteTestBase.MatchType.TYPE_2;
import static io.activej.dataflow.calcite.CalciteTestBase.State.OFF;
import static io.activej.dataflow.calcite.CalciteTestBase.State.ON;
import static io.activej.dataflow.helper.MergeStubStreamSorterStorage.FACTORY_STUB;
import static io.activej.dataflow.inject.DatasetIdImpl.datasetId;
import static io.activej.dataflow.stream.DataflowTest.*;

public abstract class CalciteTestBase {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public static final String STUDENT_TABLE_NAME = "student";
	public static final String STUDENT_DUPLICATES_TABLE_NAME = "student_duplicates";
	public static final String STUDENT_DUPLICATES_NO_PRIMARY_TABLE_NAME = "student_duplicates_no_primary";
	public static final String DEPARTMENT_TABLE_NAME = "department";
	public static final String REGISTRY_TABLE_NAME = "registry";
	public static final String USER_PROFILE_TABLE_NAME = "profiles";
	public static final String LARGE_TABLE_NAME = "large_table";
	public static final String SUBJECT_SELECTION_TABLE_NAME = "subject_selection";
	public static final String FILTERABLE_TABLE_NAME = "filterable";
	public static final String CUSTOM_TABLE_NAME = "custom";
	public static final String CUSTOM_PARTITIONED_TABLE_NAME = "custom_partitioned";
	public static final String TEMPORAL_VALUES_TABLE_NAME = "temporal_values";
	public static final String ENROLLMENT_TABLE_NAME = "enrollment";
	public static final String PAYMENT_TABLE_NAME = "payment";
	public static final String STATES_TABLE_NAME = "states";

	protected static final List<Student> STUDENT_LIST_1 = List.of(
		new Student(4, "Mark", null, 3),
		new Student(1, "John", "Doe", 1),
		new Student(5, "Travis", "Moore", 10));
	protected static final List<Student> STUDENT_LIST_2 = List.of(
		new Student(3, "John", "Truman", 2),
		new Student(2, "Bob", "Black", 2));

	protected static final List<Student> STUDENT_DUPLICATES_LIST_1 = List.of(
		new Student(4, "Mark", null, 3),
		new Student(1, "John", "Doe", 1),
		new Student(5, "Jack", "Dawson", 2));
	protected static final List<Student> STUDENT_DUPLICATES_LIST_2 = List.of(
		new Student(3, "John", "Truman", 2),
		new Student(2, "Bob", "Black", 2),
		new Student(5, "Jack", "Dawson", 3));

	protected static final List<Student> STUDENT_DUPLICATES_NO_PRIMARY_LIST_1 = List.of(
		new Student(4, "Mark", null, 3),
		new Student(1, "John", "Doe", 1),
		new Student(5, "Jack", "Dawson", 2));
	protected static final List<Student> STUDENT_DUPLICATES_NO_PRIMARY_LIST_2 = List.of(
		new Student(1, "John", "Doe", 1),
		new Student(5, "Jack", "Dawson", 3));

	protected static final List<Department> DEPARTMENT_LIST_1 = List.of(
		new Department(2, "Language", Set.of("Linguistics", "Grammar", "Morphology")));
	protected static final List<Department> DEPARTMENT_LIST_2 = List.of(
		new Department(3, "History", Set.of()),
		new Department(1, "Math", Set.of("Algebra", "Geometry", "Calculus")));

	protected static final List<Registry> REGISTRY_LIST_1 = List.of(
		new Registry(4, Map.of("Kevin", 8), List.of("google.com")),
		new Registry(1, Map.of("John", 1, "Jack", 2, "Kevin", 7), List.of("google.com", "amazon.com")));
	protected static final List<Registry> REGISTRY_LIST_2 = List.of(
		new Registry(3, Map.of("Sarah", 53, "Rob", 7564, "John", 18, "Kevin", 7), List.of("yahoo.com", "ebay.com", "netflix.com")),
		new Registry(2, Map.of("John", 999), List.of("wikipedia.org")));

	protected static final List<UserProfile> USER_PROFILES_LIST_1 = List.of(
		new UserProfile("user2", new UserProfilePojo("test2", 2), Map.of(
			2, new UserProfileIntent(2, "test2", TYPE_2, new Limits(new float[]{4.3f, 2.1f}, System.currentTimeMillis())),
			3, new UserProfileIntent(3, "test3", TYPE_1, new Limits(new float[]{6.5f, 8.7f}, System.currentTimeMillis()))),
			System.currentTimeMillis()));
	protected static final List<UserProfile> USER_PROFILES_LIST_2 = List.of(
		new UserProfile("user1", new UserProfilePojo("test1", 1), Map.of(
			1, new UserProfileIntent(1, "test1", TYPE_1, new Limits(new float[]{1.2f, 3.4f}, System.currentTimeMillis())),
			2, new UserProfileIntent(2, "test2", TYPE_2, new Limits(new float[]{5.6f, 7.8f}, System.currentTimeMillis()))),
			System.currentTimeMillis()));

	protected static final List<Large> LARGE_LIST_1 = LongStream.range(0, 1_000_000)
		.filter(value -> value % 2 == 0)
		.mapToObj(Large::new)
		.toList();
	protected static final List<Large> LARGE_LIST_2 = LongStream.range(0, 1_000_000)
		.filter(value -> value % 2 == 1)
		.mapToObj(Large::new)
		.toList();

	protected static final List<SubjectSelection> SUBJECT_SELECTION_LIST_1 = List.of(
		new SubjectSelection("ITB001", 1, "John"),
		new SubjectSelection("ITB001", 1, "Mickey"),
		new SubjectSelection("ITB001", 2, "James"),
		new SubjectSelection("MKB114", 1, "Erica"),
		new SubjectSelection("MKB114", 2, "Steve")
	);
	protected static final List<SubjectSelection> SUBJECT_SELECTION_LIST_2 = List.of(
		new SubjectSelection("ITB001", 1, "Bob"),
		new SubjectSelection("ITB001", 2, "Jenny"),
		new SubjectSelection("MKB114", 1, "John"),
		new SubjectSelection("MKB114", 1, "Paul")
	);
	protected static final TreeSet<Filterable> FILTERABLE_1 = new TreeSet<>(Comparator.comparing(Filterable::created));
	protected static final TreeSet<Filterable> FILTERABLE_2 = new TreeSet<>(Comparator.comparing(Filterable::created));

	static {
		FILTERABLE_1.add(new Filterable(43, 65L));
		FILTERABLE_1.add(new Filterable(13, 78L));
		FILTERABLE_1.add(new Filterable(76, 12L));

		FILTERABLE_2.add(new Filterable(33, 15L));
		FILTERABLE_2.add(new Filterable(64, 42L));
		FILTERABLE_2.add(new Filterable(45, 22L));
	}

	protected static final List<Custom> CUSTOM_LIST_1 = List.of(
		new Custom(1, BigDecimal.valueOf(32.4), "abc"),
		new Custom(4, BigDecimal.valueOf(102.42), "def")
	);
	protected static final List<Custom> CUSTOM_LIST_2 = List.of(
		new Custom(3, BigDecimal.valueOf(9.4343), "geh"),
		new Custom(2, BigDecimal.valueOf(43.53), "ijk")
	);

	protected static final List<Custom> CUSTOM_PARTITIONED_LIST_1 = List.of(
		new Custom(1, BigDecimal.valueOf(32.4), "abc"),
		new Custom(4, BigDecimal.valueOf(102.42), "def"),
		new Custom(2, BigDecimal.valueOf(43.53), "ijk")
	);
	protected static final List<Custom> CUSTOM_PARTITIONED_LIST_2 = List.of(
		new Custom(3, BigDecimal.valueOf(9.4343), "geh"),
		new Custom(1, BigDecimal.valueOf(32.4), "abc"),
		new Custom(2, BigDecimal.valueOf(43.53), "ijk"),
		new Custom(5, BigDecimal.valueOf(231.424), "lmn")
	);

	private static final LocalDateTime INITIAL_DATE_TIME = LocalDateTime.of(2022, 6, 15, 12, 0, 0);
	private static final LocalDateTime INITIAL_REGISTERED_AT = INITIAL_DATE_TIME;
	private static final LocalDate INITIAL_DATE = INITIAL_DATE_TIME.toLocalDate();
	private static final LocalTime INITIAL_TIME = INITIAL_DATE_TIME.toLocalTime();

	protected static final List<TemporalValues> TEMPORAL_VALUES_LIST_1 = List.of(
		new TemporalValues(
			1,
			INITIAL_REGISTERED_AT,
			INITIAL_DATE.minusYears(20),
			INITIAL_TIME),
		new TemporalValues(
			3,
			INITIAL_REGISTERED_AT.minusDays(15),
			INITIAL_DATE.minusYears(31),
			INITIAL_TIME.minusHours(5))
	);

	protected static final List<TemporalValues> TEMPORAL_VALUES_LIST_2 = List.of(
		new TemporalValues(
			2,
			INITIAL_REGISTERED_AT.minusDays(20),
			INITIAL_DATE.minusYears(39),
			INITIAL_TIME.minusMinutes(20)),
		new TemporalValues(
			5,
			INITIAL_REGISTERED_AT.minusDays(7),
			INITIAL_DATE.minusYears(43),
			INITIAL_TIME.minusMinutes(30))
	);

	protected static final List<Enrollment> ENROLLMENT_LIST_1 = List.of(
		new Enrollment(1, "AA01", true, LocalDate.parse("2022-04-24")),
		new Enrollment(2, "SK53", true, LocalDate.parse("2022-05-12"))
	);
	protected static final List<Enrollment> ENROLLMENT_LIST_2 = List.of(
		new Enrollment(1, "PX41", false, LocalDate.parse("2021-11-30")),
		new Enrollment(3, "RJ67", true, LocalDate.parse("2022-01-03"))
	);

	protected static final List<Payment> PAYMENT_LIST_1 = List.of(
		new Payment(1, "AA01", true, 340),
		new Payment(2, "SK53", false, 200)
	);
	protected static final List<Payment> PAYMENT_LIST_2 = List.of(
		new Payment(1, "PX41", false, 415),
		new Payment(3, "RJ67", true, 210)
	);

	protected static final List<States> STATES_LIST_1 = List.of(
		new States(1, ON, new InnerState(OFF)),
		new States(4, ON, new InnerState(ON))
	);
	protected static final List<States> STATES_LIST_2 = List.of(
		new States(3, OFF, new InnerState(OFF)),
		new States(2, OFF, new InnerState(ON))
	);

	@Rule
	public final ClassBuilderConstantsRule classBuilderConstantsRule = new ClassBuilderConstantsRule();

	protected ExecutorService executor;
	protected ExecutorService sortingExecutor;
	protected ISqlDataflow sqlDataflow;
	protected DataflowServer server1;
	protected DataflowServer server2;
	protected Injector server1Injector;
	protected Injector server2Injector;

	@Before
	public final void setUp() throws Exception {
		executor = Executors.newSingleThreadExecutor();
		sortingExecutor = Executors.newSingleThreadExecutor();

		InetSocketAddress address1 = getFreeListenAddress();
		InetSocketAddress address2 = getFreeListenAddress();

		List<Partition> partitions = List.of(new Partition(address1), new Partition(address2));
		Key<Set<AbstractDataflowTable<?>>> setTableKey = new Key<>() {};
		Module common = createCommon(partitions)
			.bind(setTableKey).to(classLoader -> Set.of(createStudentTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createDepartmentTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createRegistryTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createUserProfileTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createLargeTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createSubjectSelectionTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createFilterableTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createPartitionedStudentTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createPartitionedStudentTableNoPrimary(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createCustomTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createPartitionedCustomTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createTemporalValuesTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createEnrollmentTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createPaymentTable(classLoader)), DefiningClassLoader.class)
			.bind(setTableKey).to(classLoader -> Set.of(createStatesTable(classLoader)), DefiningClassLoader.class)
			.multibindToSet(new Key<AbstractDataflowTable<?>>() {})
			.build();
		Module serverCommon = createCommonServer(common, executor, sortingExecutor);

		Module clientModule = Modules.combine(createCommonClient(common), CalciteClientModule.create());
		Injector clientInjector = Injector.of(clientModule);
		clientInjector.createEagerInstances();
		sqlDataflow = clientInjector.getInstance(ISqlDataflow.class);

		Module serverModule = ModuleBuilder.create()
			.bind(StreamSorterStorageFactory.class).toInstance(FACTORY_STUB)
			.install(serverCommon)
			.install(DatasetIdModule.create())
			.install(CalciteServerModule.create())
			.build()
			.overrideWith(getAdditionalServerModule());

		Module server1Module = Modules.combine(serverModule,
			ModuleBuilder.create()
				.bind(Integer.class, "dataflowPort").toInstance(address1.getPort())
				.bind(datasetId(STUDENT_TABLE_NAME)).toInstance(STUDENT_LIST_1)
				.bind(datasetId(DEPARTMENT_TABLE_NAME)).toInstance(DEPARTMENT_LIST_1)
				.bind(datasetId(REGISTRY_TABLE_NAME)).toInstance(REGISTRY_LIST_1)
				.bind(datasetId(USER_PROFILE_TABLE_NAME)).toInstance(USER_PROFILES_LIST_1)
				.bind(datasetId(LARGE_TABLE_NAME)).toInstance(LARGE_LIST_1)
				.bind(datasetId(SUBJECT_SELECTION_TABLE_NAME)).toInstance(SUBJECT_SELECTION_LIST_1)
				.bind(datasetId(FILTERABLE_TABLE_NAME)).toInstance(createFilterableSupplier(FILTERABLE_1))
				.bind(datasetId(STUDENT_DUPLICATES_TABLE_NAME)).toInstance(STUDENT_DUPLICATES_LIST_1)
				.bind(datasetId(STUDENT_DUPLICATES_NO_PRIMARY_TABLE_NAME)).toInstance(STUDENT_DUPLICATES_NO_PRIMARY_LIST_1)
				.bind(datasetId(CUSTOM_TABLE_NAME)).toInstance(CUSTOM_LIST_1)
				.bind(datasetId(CUSTOM_PARTITIONED_TABLE_NAME)).toInstance(CUSTOM_PARTITIONED_LIST_1)
				.bind(datasetId(TEMPORAL_VALUES_TABLE_NAME)).toInstance(TEMPORAL_VALUES_LIST_1)
				.bind(datasetId(ENROLLMENT_TABLE_NAME)).toInstance(ENROLLMENT_LIST_1)
				.bind(datasetId(PAYMENT_TABLE_NAME)).toInstance(PAYMENT_LIST_1)
				.bind(datasetId(STATES_TABLE_NAME)).toInstance(STATES_LIST_1)
				.build());
		Module server2Module = Modules.combine(serverModule,
			ModuleBuilder.create()
				.bind(Integer.class, "dataflowPort").toInstance(address2.getPort())
				.bind(datasetId(STUDENT_TABLE_NAME)).toInstance(STUDENT_LIST_2)
				.bind(datasetId(DEPARTMENT_TABLE_NAME)).toInstance(DEPARTMENT_LIST_2)
				.bind(datasetId(REGISTRY_TABLE_NAME)).toInstance(REGISTRY_LIST_2)
				.bind(datasetId(USER_PROFILE_TABLE_NAME)).toInstance(USER_PROFILES_LIST_2)
				.bind(datasetId(LARGE_TABLE_NAME)).toInstance(LARGE_LIST_2)
				.bind(datasetId(SUBJECT_SELECTION_TABLE_NAME)).toInstance(SUBJECT_SELECTION_LIST_2)
				.bind(datasetId(FILTERABLE_TABLE_NAME)).toInstance(createFilterableSupplier(FILTERABLE_2))
				.bind(datasetId(STUDENT_DUPLICATES_TABLE_NAME)).toInstance(STUDENT_DUPLICATES_LIST_2)
				.bind(datasetId(STUDENT_DUPLICATES_NO_PRIMARY_TABLE_NAME)).toInstance(STUDENT_DUPLICATES_NO_PRIMARY_LIST_2)
				.bind(datasetId(CUSTOM_TABLE_NAME)).toInstance(CUSTOM_LIST_2)
				.bind(datasetId(CUSTOM_PARTITIONED_TABLE_NAME)).toInstance(CUSTOM_PARTITIONED_LIST_2)
				.bind(datasetId(TEMPORAL_VALUES_TABLE_NAME)).toInstance(TEMPORAL_VALUES_LIST_2)
				.bind(datasetId(ENROLLMENT_TABLE_NAME)).toInstance(ENROLLMENT_LIST_2)
				.bind(datasetId(PAYMENT_TABLE_NAME)).toInstance(PAYMENT_LIST_2)
				.bind(datasetId(STATES_TABLE_NAME)).toInstance(STATES_LIST_2)
				.build());

		server1Injector = Injector.of(server1Module);
		server2Injector = Injector.of(server2Module);

		server1Injector.createEagerInstances();
		server2Injector.createEagerInstances();
		server1 = server1Injector.getInstance(DataflowServer.class);
		server2 = server2Injector.getInstance(DataflowServer.class);

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

	public record Student(int id, String firstName, String lastName, int dept) {
	}

	public record Department(int id, String departmentName, Set<String> aliases) {
	}

	public record Registry(int id, Map<String, Integer> counters, List<String> domains) {
	}

	public record UserProfile(
		String id, UserProfilePojo pojo, Map<Integer, UserProfileIntent> intents, long timestamp
	) {
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

	public record Large(long id) {
	}

	public record SubjectSelection(String subject, int semester, String attendee) {
	}

	public record Filterable(int id, long created) {
	}

	public record Custom(int id, BigDecimal price, String description) {
	}

	public record TemporalValues(int userId, LocalDateTime registeredAt, LocalDate dateOfBirth, LocalTime timeOfBirth) {
	}

	public record Enrollment(int studentId, String courseCode, boolean active, LocalDate startDate) {
	}

	public record Payment(int studentId, String courseCode, boolean status, int amount) {
	}

	@SerializeRecord
	public record States(int id, State state, InnerState innerState) {
	}

	public enum State {
		OFF, ON
	}

	@SerializeRecord
	public record InnerState(State state) {
	}

	private static DataflowTable<Student> createStudentTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, STUDENT_TABLE_NAME, Student.class)
			.withColumn("id", int.class, Student::id)
			.withColumn("firstName", String.class, Student::firstName)
			.withColumn("lastName", String.class, Student::lastName)
			.withColumn("dept", int.class, Student::dept)
			.build();
	}

	private static DataflowPartitionedTable<Student> createPartitionedStudentTable(DefiningClassLoader classLoader) {
		return DataflowPartitionedTable.builder(classLoader, STUDENT_DUPLICATES_TABLE_NAME, Student.class)
			.withKeyColumn("id", int.class, Student::id)
			.withColumn("firstName", String.class, Student::firstName)
			.withColumn("lastName", String.class, Student::lastName)
			.withColumn("dept", int.class, Student::dept)
			.withReducer(new StudentReducer())
			.build();
	}

	private static DataflowPartitionedTable<Student> createPartitionedStudentTableNoPrimary(DefiningClassLoader classLoader) {
		return DataflowPartitionedTable.builder(classLoader, STUDENT_DUPLICATES_NO_PRIMARY_TABLE_NAME, Student.class)
			.withColumn("id", int.class, Student::id)
			.withColumn("firstName", String.class, Student::firstName)
			.withColumn("lastName", String.class, Student::lastName)
			.withColumn("dept", int.class, Student::dept)
			.withReducer(new StudentReducer())
			.build();
	}

	private static DataflowTable<Department> createDepartmentTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, DEPARTMENT_TABLE_NAME, Department.class)
			.withColumn("id", int.class, Department::id)
			.withColumn("departmentName", String.class, Department::departmentName)
			.withColumn("aliases", new TypeT<>() {}, Department::aliases)
			.build();
	}

	private static DataflowTable<Registry> createRegistryTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, REGISTRY_TABLE_NAME, Registry.class)
			.withColumn("id", int.class, Registry::id)
			.withColumn("counters", new TypeT<>() {}, Registry::counters)
			.withColumn("domains", new TypeT<>() {}, Registry::domains)
			.build();
	}

	private static DataflowTable<UserProfile> createUserProfileTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, USER_PROFILE_TABLE_NAME, UserProfile.class)
			.withColumn("id", String.class, UserProfile::id)
			.withColumn("pojo", UserProfilePojo.class, UserProfile::pojo)
			.withColumn("intents", new TypeT<>() {}, UserProfile::intents)
			.withColumn("timestamp", long.class, UserProfile::timestamp)
			.build();
	}

	private static DataflowTable<Large> createLargeTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, LARGE_TABLE_NAME, Large.class)
			.withColumn("id", long.class, Large::id)
			.build();
	}

	private static DataflowTable<SubjectSelection> createSubjectSelectionTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, SUBJECT_SELECTION_TABLE_NAME, SubjectSelection.class)
			.withColumn("subject", String.class, SubjectSelection::subject)
			.withColumn("semester", int.class, SubjectSelection::semester)
			.withColumn("attendee", String.class, SubjectSelection::attendee)
			.build();
	}

	private static DataflowTable<Filterable> createFilterableTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, FILTERABLE_TABLE_NAME, Filterable.class)
			.withColumn("id", int.class, Filterable::id)
			.withColumn("created", long.class, Filterable::created)
			.build();
	}

	private static DataflowTable<Custom> createCustomTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, CUSTOM_TABLE_NAME, Custom.class)
			.withColumn("id", int.class, Custom::id)
			.withColumn("price", double.class, custom -> custom.price().doubleValue())
			.withColumn("description", String.class, Custom::description)
			.build();
	}

	private static DataflowPartitionedTable<Custom> createPartitionedCustomTable(DefiningClassLoader classLoader) {
		return DataflowPartitionedTable.builder(classLoader, CUSTOM_PARTITIONED_TABLE_NAME, Custom.class)
			.withKeyColumn("id", int.class, Custom::id)
			.withColumn("price", double.class, custom -> custom.price().doubleValue())
			.build();
	}

	private static DataflowTable<TemporalValues> createTemporalValuesTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, TEMPORAL_VALUES_TABLE_NAME, TemporalValues.class)
			.withColumn("userId", int.class, TemporalValues::userId)
			.withColumn("registeredAt", LocalDateTime.class, TemporalValues::registeredAt)
			.withColumn("dateOfBirth", LocalDate.class, TemporalValues::dateOfBirth)
			.withColumn("timeOfBirth", LocalTime.class, TemporalValues::timeOfBirth)
			.build();
	}

	private static DataflowTable<Enrollment> createEnrollmentTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, ENROLLMENT_TABLE_NAME, Enrollment.class)
			.withColumn("studentId", int.class, Enrollment::studentId)
			.withColumn("courseCode", String.class, Enrollment::courseCode)
			.withColumn("active", boolean.class, Enrollment::active)
			.withColumn("startDate", LocalDate.class, Enrollment::startDate)
			.build();
	}

	private static DataflowTable<Payment> createPaymentTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, PAYMENT_TABLE_NAME, Payment.class)
			.withColumn("studentId", int.class, Payment::studentId)
			.withColumn("courseCode", String.class, Payment::courseCode)
			.withColumn("status", boolean.class, Payment::status)
			.withColumn("amount", int.class, Payment::amount)
			.build();
	}

	private static DataflowTable<States> createStatesTable(DefiningClassLoader classLoader) {
		return DataflowTable.builder(classLoader, STATES_TABLE_NAME, States.class)
			.withColumn("id", int.class, States::id)
			.withColumn("state", State.class, States::state)
			.withColumn("innerState", InnerState.class, States::innerState)
			.build();
	}

	private static FilteredDataflowSupplier<Filterable> createFilterableSupplier(NavigableSet<Filterable> filterableData) {
		return predicate -> {
			DateRange dateRange = predicateToDateRange(predicate);

			if (dateRange == null) {
				return StreamSuppliers.ofIterable(filterableData);
			}

			return StreamSuppliers.ofIterable(filterableData.subSet(
				new Filterable(-1, dateRange.from), dateRange.fromInclusive,
				new Filterable(-1, dateRange.to), dateRange.toInclusive
			));
		};
	}

	private record DateRange(long from, boolean fromInclusive, long to, boolean toInclusive) {
	}

	private static @Nullable DateRange predicateToDateRange(WherePredicate predicate) {
		if (predicate instanceof Or orPredicate) {
			List<WherePredicate> predicates = orPredicate.predicates;
			List<DateRange> ranges = new ArrayList<>(predicates.size());
			for (WherePredicate wherePredicate : predicates) {
				DateRange dateRange = predicateToDateRange(wherePredicate);
				if (dateRange == null) return null;
				ranges.add(dateRange);
			}
			return orRanges(ranges);
		}
		if (predicate instanceof And andPredicate) {
			List<WherePredicate> predicates = andPredicate.predicates;
			List<DateRange> ranges = new ArrayList<>(predicates.size());
			for (WherePredicate wherePredicate : predicates) {
				DateRange dateRange = predicateToDateRange(wherePredicate);
				if (dateRange == null) continue;
				ranges.add(dateRange);
			}
			return andRanges(ranges);
		}
		if (predicate instanceof Eq eqPredicate) {
			if (isDate(eqPredicate.left)) {
				Long date = extractDate(eqPredicate.right);
				if (date != null) {
					return new DateRange(date, true, date, true);
				}
			} else if (isDate(eqPredicate.right)) {
				Long date = extractDate(eqPredicate.left);
				if (date != null) {
					return new DateRange(date, true, date, true);
				}
			}
		}
		if (predicate instanceof NotEq notEqPredicate) {
			if (isDate(notEqPredicate.left) || isDate(notEqPredicate.right)) {
				return new DateRange(Long.MIN_VALUE, true, Long.MAX_VALUE, true);
			}
		}
		if (predicate instanceof Gt gtPredicate) {
			if (isDate(gtPredicate.left)) {
				Long date = extractDate(gtPredicate.right);
				if (date != null) {
					return new DateRange(date, false, Long.MAX_VALUE, true);
				}
			} else if (isDate(gtPredicate.right)) {
				Long date = extractDate(gtPredicate.left);
				if (date != null) {
					return new DateRange(Long.MIN_VALUE, true, date, false);
				}
			}
		}
		if (predicate instanceof Ge gePredicate) {
			if (isDate(gePredicate.left)) {
				Long date = extractDate(gePredicate.right);
				if (date != null) {
					return new DateRange(date, true, Long.MAX_VALUE, true);
				}
			} else if (isDate(gePredicate.right)) {
				Long date = extractDate(gePredicate.left);
				if (date != null) {
					return new DateRange(Long.MIN_VALUE, true, date, true);
				}
			}
		}
		if (predicate instanceof Lt ltPredicate) {
			if (isDate(ltPredicate.left)) {
				Long date = extractDate(ltPredicate.right);
				if (date != null) {
					return new DateRange(Long.MIN_VALUE, true, date, false);
				}
			} else if (isDate(ltPredicate.right)) {
				Long date = extractDate(ltPredicate.left);
				if (date != null) {
					return new DateRange(date, false, Long.MAX_VALUE, true);
				}
			}
		}
		if (predicate instanceof Le lePredicate) {
			if (isDate(lePredicate.left)) {
				Long date = extractDate(lePredicate.right);
				if (date != null) {
					return new DateRange(Long.MIN_VALUE, true, date, true);
				}
			} else if (isDate(lePredicate.right)) {
				Long date = extractDate(lePredicate.left);
				if (date != null) {
					return new DateRange(date, true, Long.MAX_VALUE, true);
				}
			}
		}
		if (predicate instanceof Between betweenPredicate) {
			if (isDate(betweenPredicate.value)) {
				Long from = extractDate(betweenPredicate.from);
				from = from == null ? Long.MIN_VALUE : from;
				Long to = extractDate(betweenPredicate.to);
				to = to == null ? Long.MAX_VALUE : to;
				return new DateRange(from, true, to, true);
			}
		}
		if (predicate instanceof In inPredicate) {
			if (isDate(inPredicate.value)) {
				Collection<Operand<?>> options = inPredicate.options;
				List<Long> dates = new ArrayList<>(options.size());
				for (Operand<?> option : options) {
					Long date = extractDate(option);
					if (date == null) {
						return null;
					}
					dates.add(date);
				}
				return new DateRange(Collections.min(dates), true, Collections.max(dates), true);
			}
		}
		if (predicate instanceof IsNull isNotNullPredicate) {
			if (isDate(isNotNullPredicate.value)) {
				return new DateRange(0, false, 0, false);
			}
		}
		if (predicate instanceof IsNotNull isNotNullPredicate) {
			if (isDate(isNotNullPredicate.value)) {
				return new DateRange(Long.MIN_VALUE, true, Long.MAX_VALUE, true);
			}
		}
		return null;
	}

	private static boolean isDate(Operand<?> operand) {
		if (!(operand instanceof RecordField recordField)) return false;

		return recordField.index == 1;
	}

	private static @Nullable Long extractDate(Operand<?> operand) {
		if (!(operand instanceof Scalar scalar)) return null;

		BigDecimal value = (BigDecimal) scalar.value.getValue();
		return value == null ? null : value.longValue();
	}

	private static DateRange andRanges(List<DateRange> ranges) {
		if (ranges.isEmpty()) return null;
		DateRange result = ranges.get(0);
		for (int i = 1; i < ranges.size(); i++) {
			DateRange current = ranges.get(i);
			if (current.from == result.from) {
				result = new DateRange(current.from, current.fromInclusive & result.fromInclusive, result.to, result.toInclusive);
			} else if (current.from > result.from) {
				result = new DateRange(current.from, current.fromInclusive, result.to, result.toInclusive);
			}

			if (current.to == result.to) {
				result = new DateRange(result.from, result.fromInclusive, result.to, result.toInclusive || current.toInclusive);
			} else if (current.to < result.to) {
				result = new DateRange(result.from, result.fromInclusive, current.to, current.toInclusive);
			}
		}

		return result;
	}

	private static DateRange orRanges(List<DateRange> ranges) {
		if (ranges.isEmpty()) return null;
		DateRange result = ranges.get(0);
		for (int i = 1; i < ranges.size(); i++) {
			DateRange current = ranges.get(i);
			if (current.from == result.from) {
				result = new DateRange(current.from, current.fromInclusive || result.fromInclusive, result.to, result.toInclusive);
			} else if (current.from < result.from) {
				result = new DateRange(current.from, current.fromInclusive, result.to, result.toInclusive);
			}

			if (current.to == result.to) {
				result = new DateRange(result.from, result.fromInclusive, result.to, result.toInclusive || current.toInclusive);
			} else if (current.to > result.to) {
				result = new DateRange(result.from, result.fromInclusive, current.to, current.toInclusive);
			}
		}

		return result;
	}

	public static final class StudentReducer extends BinaryAccumulatorReducer<Record, Record> {
		@Override
		protected Record combine(Record key, Record nextValue, Record accumulator) {
			return nextValue.getInt("dept") > accumulator.getInt("dept") ? nextValue : accumulator;
		}
	}
}
