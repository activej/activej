package io.activej.jmx.helper;

import javax.management.*;
import javax.management.loading.ClassLoaderRepository;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class MBeanServerStub implements MBeanServer {
	private final Map<ObjectName, Object> registeredMBeans = new HashMap<>();
	private final Set<ObjectName> unregisteredMBeans = new HashSet<>();

	@Override
	public ObjectInstance registerMBean(Object object, ObjectName name) {
		Object prev = registeredMBeans.put(name, object);
		if (prev != null) {
			throw new AssertionError("MBean already registered: " + name);
		}
		return new ObjectInstance(name, object.getClass().getName());
	}

	@Override
	public void unregisterMBean(ObjectName name) {
		if (!unregisteredMBeans.add(name)) {
			throw new AssertionError("MBean already unregistered: " + name);
		}
	}

	public Map<ObjectName, Object> getRegisteredMBeans() {
		return registeredMBeans;
	}

	public Set<ObjectName> getUnregisteredMBeans() {
		return unregisteredMBeans;
	}

	// region not used
	@Override
	public ObjectInstance createMBean(String className, ObjectName name) {
		throw new AssertionError();
	}

	@Override
	public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName) {
		throw new AssertionError();
	}

	@Override
	public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature) {
		throw new AssertionError();
	}

	@Override
	public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params, String[] signature) {
		throw new AssertionError();
	}

	@Override
	public ObjectInstance getObjectInstance(ObjectName name) {
		throw new AssertionError();
	}

	@Override
	public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
		throw new AssertionError();
	}

	@Override
	public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
		throw new AssertionError();
	}

	@Override
	public boolean isRegistered(ObjectName name) {
		throw new AssertionError();
	}

	@Override
	public Integer getMBeanCount() {
		throw new AssertionError();
	}

	@Override
	public Object getAttribute(ObjectName name, String attribute) {
		throw new AssertionError();
	}

	@Override
	public AttributeList getAttributes(ObjectName name, String[] attributes) {
		throw new AssertionError();
	}

	@Override
	public void setAttribute(ObjectName name, Attribute attribute) {
		throw new AssertionError();
	}

	@Override
	public AttributeList setAttributes(ObjectName name, AttributeList attributes) {
		throw new AssertionError();
	}

	@Override
	public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature) {
		throw new AssertionError();
	}

	@Override
	public String getDefaultDomain() {
		throw new AssertionError();
	}

	@Override
	public String[] getDomains() {
		throw new AssertionError();
	}

	@Override
	public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback) {
		throw new AssertionError();
	}

	@Override
	public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback) {
		throw new AssertionError();
	}

	@Override
	public void removeNotificationListener(ObjectName name, ObjectName listener) {
		throw new AssertionError();
	}

	@Override
	public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback) {
		throw new AssertionError();
	}

	@Override
	public void removeNotificationListener(ObjectName name, NotificationListener listener) {
		throw new AssertionError();
	}

	@Override
	public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback) {
		throw new AssertionError();
	}

	@Override
	public MBeanInfo getMBeanInfo(ObjectName name) {
		throw new AssertionError();
	}

	@Override
	public boolean isInstanceOf(ObjectName name, String className) {
		throw new AssertionError();
	}

	@Override
	public Object instantiate(String className) {
		throw new AssertionError();
	}

	@Override
	public Object instantiate(String className, ObjectName loaderName) {
		throw new AssertionError();
	}

	@Override
	public Object instantiate(String className, Object[] params, String[] signature) {
		throw new AssertionError();
	}

	@Override
	public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature) {
		throw new AssertionError();
	}

	@Override
	public ClassLoader getClassLoaderFor(ObjectName mbeanName) {
		throw new AssertionError();
	}

	@Override
	public ClassLoader getClassLoader(ObjectName loaderName) {
		throw new AssertionError();
	}

	@Override
	public ClassLoaderRepository getClassLoaderRepository() {
		throw new AssertionError();
	}
	// endregion
}
