package com.dataxu.pool;


import lombok.extern.slf4j.Slf4j;

import java.util.Set;
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

    public ObjectPoolImpl(Set<T> setOfObjectsInPool) {
        //Need to think about optimization
        objects = new CopyOnWriteArraySet<T>(setOfObjectsInPool);
        busyObjects = new ConcurrentHashMap<T, ArrayBlockingQueue<T>>();
        freeObjects = new ArrayBlockingQueue<T>(setOfObjectsInPool.size() + 1, true);
        freeObjects.addAll(setOfObjectsInPool);
        isOpened = new AtomicBoolean(false);
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
        close();
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

    private void markResourceAsBusy(T result) {
        ArrayBlockingQueue<T> arrayBlockingQueue = new ArrayBlockingQueue<T>(1);
        busyObjects.putIfAbsent(result, arrayBlockingQueue);
    }

    public T acquire(long timeout, TimeUnit timeUnit) {
        checkPoolIsOpened();
        try {
            T result = freeObjects.poll(timeout, timeUnit);
            markResourceAsBusy(result);
            return result;
        } catch (InterruptedException e) {
            log.error("Error", e);
        }
        return null;
    }

    public void release(T resource) {
        checkPoolIsOpened();
        ArrayBlockingQueue<T> ts = busyObjects.remove(resource);
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
        ArrayBlockingQueue<T> ts = busyObjects.remove(resource);
        T result = resource;
        if (ts != null) {
            try {
                result = ts.take();
            } catch (InterruptedException e) {
                log.error("Error", e);
            }
        }
        synchronized (this) {
            objects.remove(result);
            freeObjects.remove(result);
        }
        return true;
    }

    public boolean removeNow(T resource) {
        return false;
    }
}
