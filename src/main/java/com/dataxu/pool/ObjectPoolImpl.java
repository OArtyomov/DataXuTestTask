package com.dataxu.pool;


import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ObjectPoolImpl<T> implements ObjectPool<T> {

    private CopyOnWriteArraySet<T> objects;

    private ConcurrentHashMap<T, ArrayBlockingQueue<T>> busyObjects;

    private ArrayBlockingQueue<T> freeObjects;

    private AtomicBoolean isOpened;

    private Object busyResourcesMonitor = new Object();

    public ObjectPoolImpl(int maxPoolSize) {
        //Need to think about optimization
        objects = new CopyOnWriteArraySet<T>();
        busyObjects = new ConcurrentHashMap<T, ArrayBlockingQueue<T>>();
        freeObjects = new ArrayBlockingQueue<T>(maxPoolSize, true);
        isOpened = new AtomicBoolean(false);
    }

    private void markResourceAsBusy(T result) {
        ArrayBlockingQueue<T> previousResult = markBusyObjectIfAbsent(result, new ArrayBlockingQueue<T>(1));
        if (previousResult != null) {
            throw new RuntimeException("Error mark object as busy");
        }
    }


    private void checkPoolIsOpened() {
        if (!isOpen()) {
            throw new RuntimeException("Pool must be opened");
        }
    }

    public void open() {
        if (!isOpened.compareAndSet(false, true)) {
            throw new RuntimeException("Pool must be closed");
        }
    }

    public boolean isOpen() {
        return isOpened.get();
    }

    public void close() {
        //Here must be waiting untill all resources are released
        if (!isOpened.compareAndSet(true, false)) {
            throw new RuntimeException("Pool must be opened");
        }
    }

    public void closeNow() {
        if (!isOpened.compareAndSet(true, false)) {
            throw new RuntimeException("Pool must be opened");
        }
    }

    public T acquire() {
        checkPoolIsOpened();
        try {
            T result = freeObjects.take();
            markResourceAsBusy(result);
            return result;
        } catch (InterruptedException e) {
            log.error("Error", e);
        }
        return null;
    }

    private ArrayBlockingQueue<T> markBusyObjectIfAbsent(T result, ArrayBlockingQueue<T> arrayBlockingQueue) {
        synchronized (busyResourcesMonitor) {
            return busyObjects.putIfAbsent(result, arrayBlockingQueue);
        }
    }

    public T acquire(long timeout, TimeUnit timeUnit) {
        checkPoolIsOpened();
        try {
            T result = freeObjects.poll(timeout, timeUnit);
            if (result != null) {
                markResourceAsBusy(result);
            }
            return result;
        } catch (InterruptedException e) {
            log.error("Error", e);
        }
        return null;
    }

    public void release(T resource) {
        checkPoolIsOpened();
        ArrayBlockingQueue<T> ts;
        synchronized (busyResourcesMonitor) {
            ts = busyObjects.remove(resource);
        }
        if (ts == null) {
            throw new RuntimeException("Object is not mapped as busy");
        }
        ts.add(resource);
        synchronized (this) {
            if (objects.contains(resource)) {
                freeObjects.add(resource);
            }
        }
    }

    public boolean add(T resource) {
        checkPoolIsOpened();
        synchronized (this) {
            if (objects.add(resource)) {
                freeObjects.add(resource);
                return true;
            }
        }
        return false;
    }

    public boolean remove(T resource) {
        checkPoolIsOpened();
        ArrayBlockingQueue<T> ts = busyObjects.get(resource);
        T result = resource;
        if (ts != null) {
            try {
                result = ts.take();
            } catch (InterruptedException e) {
                log.error("Error", e);
            }
        }
        synchronized (this) {
            busyObjects.remove(resource);
            freeObjects.remove(result);
            return objects.remove(result);
        }
    }

    public boolean removeNow(T resource) {
        return false;
    }

    @VisibleForTesting
    public CopyOnWriteArraySet<T> getObjects() {
        return objects;
    }

    @VisibleForTesting
    public ConcurrentHashMap<T, ArrayBlockingQueue<T>> getBusyObjects() {
        return busyObjects;
    }

    @VisibleForTesting
    public ArrayBlockingQueue<T> getFreeObjects() {
        return freeObjects;
    }

}
