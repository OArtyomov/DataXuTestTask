package com.dataxu.pool;

public interface ObjectPool<T> {

    void open();

    boolean isOpen();

    void close();

    void closeNow();

    T acquire();

    T acquire(long timeout,
              java.util.concurrent.TimeUnit timeUnit) ;

    void release(T resource);

    boolean add(T resource);

    boolean remove(T resource);

    boolean removeNow(T resource);


}
