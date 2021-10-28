/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.jmx;

import io.activej.common.collection.Either;
import io.activej.common.initializer.WithInitializer;
import io.activej.common.ref.Ref;
import io.activej.common.reflection.ReflectionUtils;
import io.activej.jmx.api.ConcurrentJmxBeanAdapter;
import io.activej.jmx.api.JmxBeanAdapter;
import io.activej.jmx.api.JmxBeanAdapterWithRefresh;
import io.activej.jmx.api.JmxRefreshable;
import io.activej.jmx.api.attribute.JmxAttribute;
import io.activej.jmx.api.attribute.JmxOperation;
import io.activej.jmx.api.attribute.JmxParameter;
import io.activej.jmx.api.attribute.JmxReducer;
import io.activej.jmx.api.attribute.JmxReducers.JmxReducerDistinct;
import io.activej.jmx.stats.JmxRefreshableStats;
import io.activej.jmx.stats.JmxStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Checks.checkNotNull;
import static io.activej.common.Utils.first;
import static io.activej.common.Utils.nonNullElse;
import static io.activej.common.reflection.ReflectionUtils.*;
import static io.activej.jmx.Utils.findAdapterClass;
import static io.activej.jmx.stats.StatsUtils.isJmxStats;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("rawtypes")
public final class DynamicMBeanFactory implements WithInitializer<DynamicMBeanFactory> {
	private static final Logger logger = LoggerFactory.getLogger(DynamicMBeanFactory.class);

	// refreshing jmx
	public static final Duration DEFAULT_REFRESH_PERIOD_IN_SECONDS = Duration.ofSeconds(1);
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 500;
	private int maxJmxRefreshesPerOneCycle;
	private Duration specifiedRefreshPeriod;
	private final Map<Class<? extends JmxBeanAdapter>, JmxBeanAdapter> adapters = new HashMap<>();

	private static final JmxReducer<?> DEFAULT_REDUCER = new JmxReducerDistinct();

	// JmxStats creator methods
	private static final String CREATE = "create";
	private static final String CREATE_ACCUMULATOR = "createAccumulator";

	private static final DynamicMBeanFactory INSTANCE_WITH_DEFAULT_REFRESH_PERIOD = new DynamicMBeanFactory(DEFAULT_REFRESH_PERIOD_IN_SECONDS, MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT);

	// region constructor and factory methods
	private DynamicMBeanFactory(@NotNull Duration refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		this.specifiedRefreshPeriod = refreshPeriod;
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
	}

	public static DynamicMBeanFactory create() {
		return INSTANCE_WITH_DEFAULT_REFRESH_PERIOD;
	}

	public static DynamicMBeanFactory create(Duration refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		return new DynamicMBeanFactory(refreshPeriod, maxJmxRefreshesPerOneCycle);
	}
	// endregion

	// region exportable stats for JmxRegistry
	public Duration getSpecifiedRefreshPeriod() {
		return specifiedRefreshPeriod;
	}

	public void setRefreshPeriod(Duration refreshPeriod) {
		this.specifiedRefreshPeriod = refreshPeriod;
		updateAdaptersRefreshParameters();
	}

	public int getMaxJmxRefreshesPerOneCycle() {
		return maxJmxRefreshesPerOneCycle;
	}

	public void setMaxJmxRefreshesPerOneCycle(int maxJmxRefreshesPerOneCycle) {
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
		updateAdaptersRefreshParameters();
	}

	public String[] getRefreshStats() {
		return adapters.values().stream()
				.filter(adapter -> adapter instanceof JmxBeanAdapterWithRefresh)
				.map(JmxBeanAdapterWithRefresh.class::cast)
				.map(JmxBeanAdapterWithRefresh::getRefreshStats)
				.flatMap(Collection::stream)
				.toArray(String[]::new);
	}
	// endregion

	/**
	 * Creates Jmx MBean for beans with operations and attributes.
	 */
	public DynamicMBean createDynamicMBean(@NotNull List<?> beans, @NotNull JmxBeanSettings setting, boolean enableRefresh) {
		checkArgument(!beans.isEmpty(), "List of beans should not be empty");
		checkArgument(beans.stream().noneMatch(Objects::isNull), "Bean can not be null");
		checkArgument(beans.stream().map(Object::getClass).collect(toSet()).size() == 1, "Beans should be of the same type");

		Class<?> beanClass = beans.get(0).getClass();

		JmxBeanAdapter adapter = ensureAdapter(beanClass);
		if (adapter.getClass().equals(ConcurrentJmxBeanAdapter.class)) {
			checkArgument(beans.size() == 1, "ConcurrentJmxBeans cannot be used in pool");
		}

		AttributeNodeForPojo rootNode = createAttributesTree(beanClass, setting.getCustomTypes());
		rootNode.hideNullPojos(beans);

		for (String included : setting.getIncludedOptionals()) {
			rootNode.setVisible(included);
		}

		// TODO(vmykhalko): check in JmxRegistry that modifiers are applied only once in case of workers and pool registration
		for (String attrName : setting.getModifiers().keySet()) {
			AttributeModifier<?> modifier = setting.getModifiers().get(attrName);
			try {
				rootNode.applyModifier(attrName, modifier, beans);
			} catch (ClassCastException e) {
				throw new IllegalArgumentException(format("Cannot apply modifier \"%s\" for attribute \"%s\": %s",
						modifier.getClass().getName(), attrName, e));
			}
		}

		MBeanInfo mBeanInfo = createMBeanInfo(rootNode, beanClass);
		Map<OperationKey, Either<Method, AttributeNode>> opkeyToMethodOrNode = fetchOpkeyToMethodOrNode(beanClass, setting.getCustomTypes());

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(mBeanInfo, adapter, beans, rootNode, opkeyToMethodOrNode);

		// TODO(vmykhalko): maybe try to get all attributes and log warn message in case of exception? (to prevent potential errors during viewing jmx stats using jconsole)
//		tryGetAllAttributes(mbean);

		if (enableRefresh && adapter instanceof JmxBeanAdapterWithRefresh) {
			for (Object bean : beans) {
				((JmxBeanAdapterWithRefresh) adapter).registerRefreshableBean(bean, rootNode.getAllRefreshables(bean));
			}
		}
		return mbean;
	}

	JmxBeanAdapter ensureAdapter(Class<?> beanClass) {
		Class<? extends JmxBeanAdapter> adapterClass = findAdapterClass(beanClass)
				.orElseThrow(() -> new NoSuchElementException("Class or its superclass or any of implemented interfaces should be annotated with @JmxBean annotation"));
		return adapters.computeIfAbsent(adapterClass, $ -> {
			try {
				JmxBeanAdapter jmxBeanAdapter = adapterClass.getDeclaredConstructor().newInstance();
				if (jmxBeanAdapter instanceof JmxBeanAdapterWithRefresh) {
					((JmxBeanAdapterWithRefresh) jmxBeanAdapter).setRefreshParameters(specifiedRefreshPeriod, maxJmxRefreshesPerOneCycle);
				}
				return jmxBeanAdapter;
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException(e);
			}
		});
	}

	// region building tree of AttributeNodes
	private List<AttributeNode> createNodesFor(Class<?> clazz, Class<?> beanClass,
			String[] includedOptionalAttrs, @Nullable Method getter,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {

		Set<String> includedOptionals = new HashSet<>(asList(includedOptionalAttrs));
		List<AttributeDescriptor> attrDescriptors = fetchAttributeDescriptors(clazz, customTypes);
		List<AttributeNode> attrNodes = new ArrayList<>();
		for (AttributeDescriptor descriptor : attrDescriptors) {
			checkNotNull(descriptor.getGetter(), "@JmxAttribute \"%s\" does not have getter", descriptor.getName());

			String attrName;
			Method attrGetter = descriptor.getGetter();
			JmxAttribute attrAnnotation = attrGetter.getAnnotation(JmxAttribute.class);
			String attrAnnotationName = attrAnnotation.name();
			if (attrAnnotationName.equals(JmxAttribute.USE_GETTER_NAME)) {
				attrName = extractFieldNameFromGetter(attrGetter);
			} else {
				attrName = attrAnnotationName;
			}
			checkArgument(!attrName.contains("_"), "@JmxAttribute with name \"%s\" contains underscores", attrName);

			String attrDescription = null;
			if (!attrAnnotation.description().equals(JmxAttribute.NO_DESCRIPTION)) {
				attrDescription = attrAnnotation.description();
			}

			boolean included = !attrAnnotation.optional() || includedOptionals.contains(attrName);
			includedOptionals.remove(attrName);

			Type type = attrGetter.getGenericReturnType();
			Method attrSetter = descriptor.getSetter();
			AttributeNode attrNode = createAttributeNodeFor(attrName, attrDescription, type, included,
					attrAnnotation, attrGetter, attrSetter, beanClass,
					customTypes);
			attrNodes.add(attrNode);
		}

		if (!includedOptionals.isEmpty()) {
			assert getter != null; // in this case getter cannot be null
			throw new RuntimeException(format("Error in \"extraSubAttributes\" parameter in @JmxAnnotation" +
							" on %s.%s(). There is no field \"%s\" in %s.",
					getter.getDeclaringClass().getName(), getter.getName(),
					first(includedOptionals), getter.getReturnType().getName()));
		}

		return attrNodes;
	}

	private List<AttributeDescriptor> fetchAttributeDescriptors(Class<?> clazz, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Map<String, AttributeDescriptor> nameToAttr = new HashMap<>();
		for (Method method : getAllMethods(clazz)) {
			if (method.isAnnotationPresent(JmxAttribute.class)) {
				validateJmxMethod(method, JmxAttribute.class);
				if (isGetter(method)) {
					processGetter(nameToAttr, method);
				} else if (isSetter(method)) {
					processSetter(nameToAttr, method, customTypes);
				} else {
					throw new RuntimeException(format("Method \"%s\" of class \"%s\" is annotated with @JmxAnnotation "
							+ "but is neither getter nor setter", method.getName(), method.getClass().getName())
					);
				}
			}
		}
		return new ArrayList<>(nameToAttr.values());
	}

	private static void validateJmxMethod(Method method, Class<? extends Annotation> annotationClass) {
		if (!isPublic(method)) {
			throw new IllegalStateException(format("A method \"%s\" in class '%s' annotated with @%s should be declared public",
					method.getName(), method.getDeclaringClass().getName(), annotationClass.getSimpleName()));
		}
		if (!isPublic(method.getDeclaringClass())) {
			throw new IllegalStateException(format("A class '%s' containing methods annotated with @%s should be declared public",
					method.getDeclaringClass().getName(), annotationClass.getSimpleName()));
		}
	}

	private static void processGetter(Map<String, AttributeDescriptor> nameToAttr, Method getter) {
		String name = extractFieldNameFromGetter(getter);
		Type attrType = getter.getReturnType();
		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			checkArgument(previousDescriptor.getGetter() == null,
					"More than one getter with name \"%s\"", getter.getName());
			checkArgument(previousDescriptor.getType().equals(attrType),
					"Getter with name \"%s\" has different type than appropriate setter", getter.getName());

			nameToAttr.put(name, new AttributeDescriptor(name, attrType, getter, previousDescriptor.getSetter()));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, getter, null));
		}
	}

	private void processSetter(Map<String, AttributeDescriptor> nameToAttr, Method setter, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Class<?> attrType = setter.getParameterTypes()[0];
		checkArgument(ReflectionUtils.isSimpleType(attrType) || customTypes.containsKey(attrType), "Setters are allowed only on SimpleType attributes."
				+ " But setter \"%s\" is not SimpleType setter", setter.getName());

		String name = extractFieldNameFromSetter(setter);

		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			checkArgument(previousDescriptor.getSetter() == null,
					"More than one setter with name \"%s\"", setter.getName());
			checkArgument(previousDescriptor.getType().equals(attrType),
					"Setter with name \"%s\" has different type than appropriate getter", setter.getName());

			nameToAttr.put(name, new AttributeDescriptor(
					name, attrType, previousDescriptor.getGetter(), setter));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, null, setter));
		}
	}

	private void updateAdaptersRefreshParameters() {
		for (JmxBeanAdapter adapter : adapters.values()) {
			if (adapter instanceof JmxBeanAdapterWithRefresh) {
				((JmxBeanAdapterWithRefresh) adapter).setRefreshParameters(specifiedRefreshPeriod, maxJmxRefreshesPerOneCycle);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private AttributeNode createAttributeNodeFor(
			String attrName,
			@Nullable String attrDescription,
			Type attrType,
			boolean included,
			@Nullable JmxAttribute attrAnnotation,
			@Nullable Method getter,
			@Nullable Method setter,
			Class<?> beanClass,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		if (attrType instanceof Class) {
			ValueFetcher defaultFetcher = createAppropriateFetcher(getter);
			// 4 cases: custom-type, simple-type, JmxRefreshableStats, POJO
			Class<? extends JmxStats> returnClass = (Class<? extends JmxStats>) attrType;

			if (customTypes.containsKey(attrType)) {
				JmxCustomTypeAdapter<?> customTypeAdapter = customTypes.get(attrType);
				return new AttributeNodeForConverterType(attrName, attrDescription, included,
						defaultFetcher, setter, customTypeAdapter.to, customTypeAdapter.from);

			} else if (ReflectionUtils.isSimpleType(returnClass)) {
				JmxReducer<?> reducer;
				if (attrAnnotation == null) {
					reducer = DEFAULT_REDUCER;
				} else {
					try {
						reducer = fetchReducerFrom(getter);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
				return new AttributeNodeForSimpleType(
						attrName, attrDescription, included, defaultFetcher, setter, returnClass, reducer
				);

			} else if (isThrowable(returnClass)) {
				return new AttributeNodeForThrowable(attrName, attrDescription, included, defaultFetcher);

			} else if (returnClass.isArray()) {
				Class<?> elementType = returnClass.getComponentType();
				checkNotNull(getter, "Arrays can be used only directly in POJO, JmxRefreshableStats or JmxMBeans");
				ValueFetcher fetcher = new ValueFetcherFromGetterArrayAdapter(getter);
				return createListAttributeNodeFor(attrName, attrDescription, included, fetcher, elementType, beanClass, customTypes);

			} else if (isJmxStats(returnClass)) {
				// JmxRefreshableStats case

				checkJmxStatsAreValid(returnClass, beanClass, getter);

				String[] extraSubAttributes =
						attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
						createNodesFor(returnClass, beanClass, extraSubAttributes, getter, customTypes);

				if (subNodes.isEmpty()) {
					throw new IllegalArgumentException(format(
							"JmxRefreshableStats of type \"%s\" does not have JmxAttributes",
							returnClass.getName()));
				}

				return new AttributeNodeForPojo(attrName, attrDescription, included, defaultFetcher,
						createReducerForJmxStats(returnClass), subNodes);

			} else {
				String[] extraSubAttributes =
						attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
						createNodesFor(returnClass, beanClass, extraSubAttributes, getter, customTypes);

				if (subNodes.isEmpty()) {
					throw new IllegalArgumentException(format("Unrecognized type of Jmx attribute: %s", attrType.getTypeName()));
				} else {
					// POJO case

					JmxReducer<?> reducer;
					try {
						reducer = fetchReducerFrom(getter);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}

					return new AttributeNodeForPojo(
							attrName, attrDescription, included, defaultFetcher, reducer == DEFAULT_REDUCER ? null : reducer, subNodes);
				}
			}
		} else if (attrType instanceof ParameterizedType) {
			return createNodeForParametrizedType(
					attrName, attrDescription, (ParameterizedType) attrType, included, getter, setter, beanClass, customTypes);
		} else {
			throw new IllegalArgumentException(format("Unrecognized type of Jmx attribute: %s", attrType.getTypeName()));
		}
	}

	@SuppressWarnings("unchecked")
	private static JmxReducer<?> createReducerForJmxStats(Class<? extends JmxStats> jmxStatsClass) {
		return (JmxReducer<Object>) sources -> {
			JmxStats accumulator = createJmxAccumulator(jmxStatsClass);
			for (Object pojo : sources) {
				JmxStats jmxStats = (JmxStats) pojo;
				if (jmxStats != null) {
					accumulator.add(jmxStats);
				}
			}
			return accumulator;
		};
	}

	private static JmxStats createJmxAccumulator(Class<? extends JmxStats> jmxStatsClass) {
		JmxStats jmxStats = ReflectionUtils.tryToCreateInstanceWithFactoryMethods(jmxStatsClass, CREATE_ACCUMULATOR, CREATE);
		if (jmxStats == null) {
			throw new RuntimeException(format("Cannot create JmxStats accumulator instance: %s", jmxStatsClass.getName()));
		}
		return jmxStats;
	}

	@SuppressWarnings("unchecked")
	private static JmxReducer<?> fetchReducerFrom(@Nullable Method getter) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
		if (getter == null) {
			return DEFAULT_REDUCER;
		}
		JmxAttribute attrAnnotation = getter.getAnnotation(JmxAttribute.class);
		Class<?> reducerClass = attrAnnotation.reducer();
		if (reducerClass == DEFAULT_REDUCER.getClass()) {
			return DEFAULT_REDUCER;
		}
		return ((Class<? extends JmxReducer<?>>) reducerClass).getDeclaredConstructor().newInstance();
	}

	private static void checkJmxStatsAreValid(Class<?> returnClass, Class<?> beanClass, @Nullable Method getter) {
		if (JmxRefreshableStats.class.isAssignableFrom(returnClass) &&
				!findAdapterClass(beanClass).filter(JmxBeanAdapterWithRefresh.class::isAssignableFrom).isPresent()
		) {
			logger.warn("JmxRefreshableStats won't be refreshed when Bean adapter does not implement JmxBeanAdapterWithRefresh. " +
					"MBean class: {}", beanClass.getName());
		}

		if (returnClass.isInterface()) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}

		if (Modifier.isAbstract(returnClass.getModifiers())) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}

		if (!canBeCreated(returnClass, CREATE_ACCUMULATOR, CREATE)) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}
	}

	private static String createErrorMessageForInvalidJmxStatsAttribute(@Nullable Method getter) {
		String msg = "Return type of JmxStats attribute must be a concrete class that implements" +
				" JmxStats interface and contains" +
				" static factory \"" + CREATE_ACCUMULATOR + "()\" method or" +
				" static factory \"" + CREATE + "()\" method or" +
				" public no-arg constructor";

		if (getter != null) {
			msg += format(". Error at %s.%s()", getter.getDeclaringClass().getName(), getter.getName());
		}
		return msg;
	}

	private AttributeNode createNodeForParametrizedType(String attrName, @Nullable String attrDescription,
			ParameterizedType pType, boolean included,
			@Nullable Method getter, @Nullable Method setter, Class<?> beanClass,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		ValueFetcher fetcher = createAppropriateFetcher(getter);
		Class<?> rawType = (Class<?>) pType.getRawType();

		if (rawType == List.class) {
			Type listElementType = pType.getActualTypeArguments()[0];
			return createListAttributeNodeFor(
					attrName, attrDescription, included, fetcher, listElementType, beanClass, customTypes);
		} else if (rawType == Map.class) {
			Type valueType = pType.getActualTypeArguments()[1];
			return createMapAttributeNodeFor(attrName, attrDescription, included, fetcher, valueType, beanClass, customTypes);
		} else if (customTypes.containsKey(rawType)) {
			return createConverterAttributeNodeFor(attrName, attrDescription, pType, included, fetcher, setter, customTypes);
		} else {
			throw new IllegalArgumentException(format("There is no support for generic class %s", pType.getTypeName()));
		}
	}

	@SuppressWarnings("unchecked")
	private AttributeNodeForConverterType createConverterAttributeNodeFor(
			String attrName,
			@Nullable String attrDescription,
			ParameterizedType type, boolean included,
			ValueFetcher fetcher, @Nullable Method setter,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Type[] actualTypes = type.getActualTypeArguments();
		for (Type genericType : actualTypes) {
			if (!customTypes.containsKey(genericType)) {
				throw new IllegalArgumentException(format("There is no support for generic type %s", type.getTypeName()));
			}
		}
		JmxCustomTypeAdapter<?> t = customTypes.get(type.getRawType());
		return new AttributeNodeForConverterType(attrName, attrDescription, fetcher, included, setter, t.to, t.from);
	}

	private AttributeNodeForList createListAttributeNodeFor(
			String attrName,
			@Nullable String attrDescription,
			boolean included,
			ValueFetcher fetcher,
			Type listElementType, Class<?> beanClass,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		if (listElementType instanceof Class<?>) {
			Class<?> listElementClass = (Class<?>) listElementType;
			boolean isListOfJmxRefreshable = JmxRefreshable.class.isAssignableFrom(listElementClass);
			return new AttributeNodeForList(
					attrName,
					attrDescription,
					included,
					fetcher,
					createAttributeNodeFor("", attrDescription, listElementType, true, null, null, null, beanClass, customTypes),
					isListOfJmxRefreshable
			);
		} else if (listElementType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) listElementType).getRawType()).getSimpleName();
			return new AttributeNodeForList(
					attrName,
					attrDescription,
					included,
					fetcher,
					createNodeForParametrizedType(
							typeName, attrDescription, (ParameterizedType) listElementType, true, null, null, beanClass,
							customTypes
					),
					false
			);
		} else {
			throw new IllegalArgumentException(format("Can't create list attribute node for List<%s>", listElementType.getTypeName()));
		}
	}

	private AttributeNodeForMap createMapAttributeNodeFor(
			String attrName,
			@Nullable String attrDescription,
			boolean included, ValueFetcher fetcher,
			Type valueType, Class<?> beanClass,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		boolean isMapOfJmxRefreshable = false;
		AttributeNode node;
		if (valueType instanceof Class<?>) {
			Class<?> valueClass = (Class<?>) valueType;
			isMapOfJmxRefreshable = JmxRefreshable.class.isAssignableFrom(valueClass);
			node = createAttributeNodeFor("", attrDescription, valueType, true, null, null, null, beanClass, customTypes);
		} else if (valueType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) valueType).getRawType()).getSimpleName();
			node = createNodeForParametrizedType(typeName, attrDescription, (ParameterizedType) valueType, true, null, null, beanClass,
					customTypes);
		} else {
			throw new IllegalArgumentException(format("Can't create map attribute node for %s", valueType.getTypeName()));
		}
		return new AttributeNodeForMap(attrName, attrDescription, included, fetcher, node, isMapOfJmxRefreshable);
	}

	private static ValueFetcher createAppropriateFetcher(@Nullable Method getter) {
		return getter != null ? new ValueFetcherFromGetter(getter) : new ValueFetcherDirect();
	}
	// endregion

	/**
	 * Creates attribute tree of Jmx attributes for clazz.
	 */
	private AttributeNodeForPojo createAttributesTree(Class<?> clazz, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		List<AttributeNode> subNodes = createNodesFor(clazz, clazz, new String[0], null, customTypes);
		return new AttributeNodeForPojo("", null, true, new ValueFetcherDirect(), null, subNodes);
	}

	// region creating jmx metadata - MBeanInfo
	private static MBeanInfo createMBeanInfo(AttributeNodeForPojo rootNode, Class<?> beanClass) {
		String beanName = "";
		String beanDescription = "";
		MBeanAttributeInfo[] attributes = rootNode != null ?
				fetchAttributesInfo(rootNode) :
				new MBeanAttributeInfo[0];
		MBeanOperationInfo[] operations = fetchOperationsInfo(beanClass);
		return new MBeanInfo(
				beanName,
				beanDescription,
				attributes,
				null,  // constructors
				operations,
				null); //notifications
	}

	private static MBeanAttributeInfo[] fetchAttributesInfo(AttributeNodeForPojo rootNode) {
		Set<String> visibleAttrs = rootNode.getVisibleAttributes();
		Map<String, OpenType<?>> nameToType = rootNode.getOpenTypes();
		Map<String, Map<String, String>> nameToDescriptions = rootNode.getDescriptions();
		List<MBeanAttributeInfo> attrsInfo = new ArrayList<>();
		for (String attrName : visibleAttrs) {
			String description = createDescription(attrName, nameToDescriptions.get(attrName));
			OpenType<?> attrType = nameToType.get(attrName);
			boolean writable = rootNode.isSettable(attrName);
			boolean isIs = attrType.equals(SimpleType.BOOLEAN);
			attrsInfo.add(new MBeanAttributeInfo(attrName, attrType.getClassName(), description, true, writable, isIs));
		}

		return attrsInfo.toArray(new MBeanAttributeInfo[0]);
	}

	private static String createDescription(String name, Map<String, String> groupDescriptions) {
		if (groupDescriptions.isEmpty()) {
			return name;
		}

		if (!name.contains("_")) {
			assert groupDescriptions.size() == 1;
			return first(groupDescriptions.values());
		}

		return groupDescriptions.entrySet().stream()
				.map(entry -> String.format("\"%s\": %s", entry.getKey(), entry.getValue()))
				.collect(joining("  |  "));
	}

	private static MBeanOperationInfo[] fetchOperationsInfo(Class<?> beanClass) {
		List<MBeanOperationInfo> operations = new ArrayList<>();
		List<Method> methods = getAllMethods(beanClass);
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxOperation.class)) {
				validateJmxMethod(method, JmxOperation.class);
				JmxOperation annotation = method.getAnnotation(JmxOperation.class);
				String opName = annotation.name();
				if (opName.equals("")) {
					opName = method.getName();
				}

				Class<?>[] parameterTypes = method.getParameterTypes();
				Annotation[][] parameterAnnotations = method.getParameterAnnotations();
				MBeanParameterInfo[] parameterInfos = new MBeanParameterInfo[parameterTypes.length];
				for (int i = 0; i < parameterTypes.length; i++) {
					parameterInfos[i] = new MBeanParameterInfo(
							Arrays.stream(parameterAnnotations[i])
									.filter(a -> a.annotationType() == JmxParameter.class)
									.map(JmxParameter.class::cast)
									.map(JmxParameter::value)
									.findFirst()
									.orElse(String.format("arg%d", i)),
							parameterTypes[i].getName(),
							"");
				}

				MBeanOperationInfo operationInfo = new MBeanOperationInfo(
						opName, annotation.description(), parameterInfos, method.getReturnType().getName(), MBeanOperationInfo.ACTION);
				operations.add(operationInfo);
			}
		}

		return operations.toArray(new MBeanOperationInfo[0]);
	}
	// endregion

	// region jmx operations fetching
	private Map<OperationKey, Either<Method, AttributeNode>> fetchOpkeyToMethodOrNode(Class<?> beanClass, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Map<OperationKey, Either<Method, AttributeNode>> opkeyToMethod = new HashMap<>();
		List<Method> methods = getAllMethods(beanClass);
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxOperation.class)) {
				JmxOperation annotation = method.getAnnotation(JmxOperation.class);
				String opName = annotation.name();
				if (opName.equals("")) {
					opName = method.getName();
				}
				Class<?>[] paramTypes = method.getParameterTypes();
				Annotation[][] paramAnnotations = method.getParameterAnnotations();

				assert paramAnnotations.length == paramTypes.length;

				String[] paramTypesNames = new String[paramTypes.length];
				for (int i = 0; i < paramTypes.length; i++) {
					paramTypesNames[i] = paramTypes[i].getName();
				}

				Either<Method, AttributeNode> either;
				if (isGetter(method)) {
					String name = extractFieldNameFromGetter(method);
					AttributeNode node = createAttributeNodeFor(name, null, method.getGenericReturnType(), true, null, method, null, beanClass, customTypes);
					either = Either.right(node);
				} else {
					either = Either.left(method);
				}

				opkeyToMethod.put(new OperationKey(opName, paramTypesNames), either);
			}
		}
		return opkeyToMethod;
	}
	// endregion

	// region helper classes
	private static final class AttributeDescriptor {
		private final String name;
		private final Type type;
		private final Method getter;
		private final Method setter;

		public AttributeDescriptor(String name, Type type, @Nullable Method getter, @Nullable Method setter) {
			this.name = name;
			this.type = type;
			this.getter = getter;
			this.setter = setter;
		}

		public String getName() {
			return name;
		}

		public Type getType() {
			return type;
		}

		public @Nullable Method getGetter() {
			return getter;
		}

		public @Nullable Method getSetter() {
			return setter;
		}
	}

	private static final class OperationKey {
		private final String name;
		private final String[] argTypes;

		public OperationKey(@NotNull String name, String[] argTypes) {
			this.name = name;
			this.argTypes = argTypes;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof OperationKey)) return false;

			OperationKey that = (OperationKey) o;

			if (!name.equals(that.name)) return false;

			return Arrays.equals(argTypes, that.argTypes);

		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + Arrays.hashCode(argTypes);
			return result;
		}
	}

	private static final class DynamicMBeanAggregator implements DynamicMBean {
		private final MBeanInfo mBeanInfo;
		private final JmxBeanAdapter adapter;
		private final List<?> beans;
		private final AttributeNodeForPojo rootNode;
		private final Map<OperationKey, Either<Method, AttributeNode>> opKeyToMethodOrNode;

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, JmxBeanAdapter adapter, List<?> beans,
				AttributeNodeForPojo rootNode, Map<OperationKey, Either<Method, AttributeNode>> opKeyToMethodOrNode) {
			this.mBeanInfo = mBeanInfo;
			this.adapter = adapter;
			this.beans = beans;

			this.rootNode = rootNode;
			this.opKeyToMethodOrNode = opKeyToMethodOrNode;
		}

		@Override
		public Object getAttribute(String attribute) throws MBeanException {
			Object value;
			try {
				value = rootNode.aggregateAttributes(singleton(attribute), beans).get(attribute);
			} catch (Exception e) {
				logger.error("Failed to fetch attribute '{}' from beans {}", attribute, beans, e);
				propagate(e);
				throw new AssertionError("Never reached");
			}

			if (value instanceof Throwable) {
				Throwable throwable = (Throwable) value;
				logger.error("Failed to fetch attribute '{}' from beans {}", attribute, beans, throwable);
				propagate(throwable);
			}
			return value;
		}

		@Override
		public void setAttribute(Attribute attribute) throws MBeanException {
			String attrName = attribute.getName();
			Object attrValue = attribute.getValue();

			CountDownLatch latch = new CountDownLatch(beans.size());
			Ref<Exception> exceptionRef = new Ref<>();

			for (Object bean : beans) {
				adapter.execute(bean, () -> {
					try {
						rootNode.setAttribute(attrName, attrValue, singletonList(bean));
						latch.countDown();
					} catch (Exception e) {
						logger.error("Failed to set attribute '{}' of {} with value '{}'", attrName, bean, attrValue, e);
						exceptionRef.set(e);
						latch.countDown();
					}
				});
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new MBeanException(e);
			}

			Exception e = exceptionRef.get();
			if (e != null) {
				Exception actualException = e;
				if (e instanceof SetterException) {
					SetterException setterException = (SetterException) e;
					actualException = setterException.getCausedException();
				}
				propagate(actualException);
			}
		}

		@Override
		public AttributeList getAttributes(String[] attributes) {
			AttributeList attrList = new AttributeList();
			Set<String> attrNames = new HashSet<>(Arrays.asList(attributes));
			try {
				Map<String, Object> aggregatedAttrs = rootNode.aggregateAttributes(attrNames, beans);
				for (Map.Entry<String, Object> entry : aggregatedAttrs.entrySet()) {
					if (!(entry.getValue() instanceof Throwable)) {
						attrList.add(new Attribute(entry.getKey(), entry.getValue()));
					}
				}
			} catch (Exception e) {
				logger.error("Failed to get attributes {} from beans {}", attrNames, beans, e);
			}
			return attrList;
		}

		@Override
		public AttributeList setAttributes(AttributeList attributes) {
			AttributeList resultList = new AttributeList();
			for (Object attr : attributes) {
				Attribute attribute = (Attribute) attr;
				try {
					setAttribute(attribute);
					resultList.add(new Attribute(attribute.getName(), attribute.getValue()));
				} catch (MBeanException ignored) {
				}
			}
			return resultList;
		}

		@Override
		public @Nullable Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException {
			Object[] args = nonNullElse(params, new Object[0]);
			String[] argTypes = nonNullElse(signature, new String[0]);
			OperationKey opkey = new OperationKey(actionName, argTypes);
			Either<Method, AttributeNode> methodOrNode = opKeyToMethodOrNode.get(opkey);
			if (methodOrNode == null) {
				String operationName = prettyOperationName(actionName, argTypes);
				String errorMsg = format("There is no operation \"%s\"", operationName);
				throw new RuntimeOperationsException(new IllegalArgumentException("Operation not found"), errorMsg);
			}

			if (methodOrNode.isLeft()) {
				return invokeMethod(methodOrNode.getLeft(), args);
			} else {
				if (args.length != 0) {
					throw new MBeanException(new IllegalArgumentException("Passing arguments to getter operation"));
				}
				return invokeNode(extractFieldNameFromGetterName(actionName), methodOrNode.getRight());
			}
		}

		private Object invokeMethod(Method method, Object[] args) throws MBeanException {
			CountDownLatch latch = new CountDownLatch(beans.size());
			Ref<Object> lastValueRef = new Ref<>();
			Ref<Exception> exceptionRef = new Ref<>();
			for (Object bean : beans) {
				adapter.execute(bean, () -> {
					try {
						Object result = method.invoke(bean, args);
						lastValueRef.set(result);
						latch.countDown();
					} catch (Exception e) {
						logger.error("Failed to invoke method '{}' on {} with args {}", method, bean, args, e);
						exceptionRef.set(e);
						latch.countDown();
					}
				});
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new MBeanException(e);
			}

			Exception e = exceptionRef.get();
			if (e != null) {
				propagate(e);
			}

			// We don't know how to aggregate return values if there are several beans
			return beans.size() == 1 ? lastValueRef.get() : null;
		}

		private Object invokeNode(String name, AttributeNode node) throws MBeanException {
			try {
				return node.aggregateAttributes(singleton(name), beans).get(name);
			} catch (Throwable e) {
				logger.error("Failed to fetch attribute '{}' from beans {}", name, beans, e);
				propagate(e);
				return null;
			}
		}

		private void propagate(Throwable e) throws MBeanException {
			if (e instanceof InvocationTargetException) {
				Throwable targetException = ((InvocationTargetException) e).getTargetException();

				if (targetException instanceof Exception) {
					throw new MBeanException((Exception) targetException);
				} else {
					throw new MBeanException(
							new Exception(format("Throwable of type \"%s\" and message \"%s\" " +
											"was thrown during method invocation",
									targetException.getClass().getName(), targetException.getMessage())
							)
					);
				}

			} else {
				if (e instanceof Exception) {
					throw new MBeanException((Exception) e);
				} else {
					throw new MBeanException(
							new Exception(format("Throwable of type \"%s\" and message \"%s\" " +
											"was thrown",
									e.getClass().getName(), e.getMessage())
							)
					);
				}
			}
		}

		@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
		private static String prettyOperationName(String name, String[] argTypes) {
			StringBuilder operationName = new StringBuilder(name + "(");
			if (argTypes.length > 0) {
				for (int i = 0; i < argTypes.length - 1; i++) {
					operationName.append(argTypes[i] + ", ");
				}
				operationName.append(argTypes[argTypes.length - 1]);
			}
			operationName.append(")");
			return operationName.toString();
		}

		@Override
		public MBeanInfo getMBeanInfo() {
			return mBeanInfo;
		}
	}

	static class JmxCustomTypeAdapter<T> {
		public final @Nullable Function<String, T> from;
		public final Function<T, String> to;

		public JmxCustomTypeAdapter(Function<T, String> to, @Nullable Function<String, T> from) {
			this.to = to;
			this.from = from;
		}

		public JmxCustomTypeAdapter(Function<T, String> to) {
			this.to = to;
			this.from = null;
		}
	}
	// endregion
}
