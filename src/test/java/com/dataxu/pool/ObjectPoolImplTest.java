package com.dataxu.pool;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.rules.ExpectedException.none;

public class ObjectPoolImplTest {

    private static final int MAX_POOL_SIZE = 1000;

    private ObjectPoolImpl<Person> pool = new ObjectPoolImpl<Person>(MAX_POOL_SIZE);

    @Rule
    public ExpectedException expectedException = none();

    @Test
    public void testOpen() {
        pool.open();
        assertTrue(pool.isOpen());
    }


    private Person buildPerson(String name, String surname) {
        final Person person = new Person();
        person.setName(name);
        person.setSurname(surname);
        return person;
    }

    @Test
    public void testOpenOnAlreadyOpenedPool() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Pool must be closed");
        pool.open();
        pool.open();
    }


    @Test
    public void isOpen() {
        pool.open();
        assertTrue(pool.isOpen());
        pool.close();
        assertFalse(pool.isOpen());
    }

    @Test
    public void testClose() {
        pool.open();
        pool.close();
        assertFalse(pool.isOpen());
    }

    @Test
    public void testCloseOnAlreadyClosedPool() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Pool must be opened");
        pool.open();
        pool.close();
        pool.close();
    }


    @Test
    public void testAcquire() throws Exception {
        pool.open();
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = newFixedThreadPool(threadCount);
        final Person person = buildPerson("AA", "BB");
        final List<Person> acquiredObjects = newArrayList();
        Callable<Void> appendTask = new Callable<Void>() {
            public Void call() {
                pool.add(person);
                return null;
            }
        };
        Callable<Void> acquireTask = new Callable<Void>() {
            public Void call() {
                Person acquire = pool.acquire();
                acquiredObjects.add(acquire);
                return null;
            }
        };

        List<Future<Void>> futures = executorService.invokeAll(newArrayList(appendTask, acquireTask));
        for (Future<Void> future : futures) {
            future.get();
        }
        assertEquals(person, acquiredObjects.get(0));
    }

    @Test
    public void acquireWithTimeout() {
        pool.open();
        Person acquire = pool.acquire(2, TimeUnit.SECONDS);
        assertNull(acquire);
    }


    @Test
    public void testCloseNow() {
        pool.open();
        final Person person = buildPerson("AA", "BB");
        pool.add(person);
        pool.acquire();
        pool.closeNow();
        assertFalse(pool.isOpen());
    }

    @Test
    public void testReleaseIsFalseAfterCloseNow() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Pool must be opened");
        pool.open();
        final Person person = buildPerson("AA", "BB");
        pool.add(person);
        Person acquiredResource = pool.acquire();
        pool.closeNow();
        pool.release(acquiredResource);
    }


    @Test
    public void release() {
    }

    @Test
    public void testSimpleAdd() throws Exception {
        pool.open();
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = newFixedThreadPool(threadCount);
        List<Callable<Void>> items = newArrayList();
        int countOfAddedObjects = 100;
        for (int i = 0; i < countOfAddedObjects; i++) {
            final int index = i;
            Callable<Void> appendTask = new Callable<Void>() {
                public Void call() {
                    final Person person = buildPerson("AA" + index, "BB" + index);
                    pool.add(person);
                    return null;
                }
            };
            items.add(appendTask);
        }
        List<Future<Void>> futures = executorService.invokeAll(items);
        for (Future<Void> future : futures) {
            future.get();
        }
        assertEquals(countOfAddedObjects, pool.getObjects().size());
        assertEquals(countOfAddedObjects, pool.getFreeObjects().size());
        assertEquals(0, pool.getBusyObjects().size());
    }

    @Test
    public void testSimpleRemove() throws Exception {
        pool.open();
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = newFixedThreadPool(threadCount);
        List<Callable<Void>> items = newArrayList();
        int countOfAddedObjects = 100;
        for (int i = 0; i < countOfAddedObjects; i++) {
            final Person person = buildPerson("AA" + i, "BB" + i);
            pool.add(person);
            Callable<Void> removeTask = new Callable<Void>() {
                public Void call() {
                    pool.remove(person);
                    return null;
                }
            };

            items.add(removeTask);
        }
        List<Future<Void>> futures = executorService.invokeAll(items);
        for (Future<Void> future : futures) {
            future.get();
        }
        assertEquals(0, pool.getObjects().size());
        assertEquals(0, pool.getFreeObjects().size());
        assertEquals(0, pool.getBusyObjects().size());
    }

    @Test
    public void testRemoveWhenObjectAcquired() throws Exception {
        pool.open();
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = newFixedThreadPool(threadCount);
        List<Callable<Void>> items = newArrayList();
        int countOfAddedObjects = 20;
        for (int i = 0; i < countOfAddedObjects; i++) {
            final Person person = buildPerson("AA" + i, "BB" + i);
            pool.add(person);
            Callable<Void> acquireTask = new Callable<Void>() {
                public Void call() throws InterruptedException {
                    Person resource = pool.acquire();
                    new Thread(new Runnable() {
                        public void run() {
                            pool.remove(person);
                        }
                    }).start();
                    Thread.sleep(2000);
                    pool.release(resource);
                    return null;
                }
            };
            items.add(acquireTask);
        }
        List<Future<Void>> futures = executorService.invokeAll(items);
        for (Future<Void> future : futures) {
            future.get();
        }
        assertEquals(0, pool.getObjects().size());
        assertEquals(0, pool.getFreeObjects().size());
        assertEquals(0, pool.getBusyObjects().size());
    }


    @Test
    public void testRemoveNowAndReleaseWhenObjectAcquired() throws Exception {
        pool.open();
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = newFixedThreadPool(threadCount);
        List<Callable<Void>> items = newArrayList();
        int countOfAddedObjects = 1;
        for (int i = 0; i < countOfAddedObjects; i++) {
            final Person person = buildPerson("AA" + i, "BB" + i);
            pool.add(person);
            Callable<Void> acquireTask = new Callable<Void>() {
                public Void call() throws InterruptedException {
                    Person resource = pool.acquire();
                    new Thread(new Runnable() {
                        public void run() {
                            pool.removeNow(person);
                        }
                    }).start();
                    Thread.sleep(2000);
                    pool.release(resource);
                    return null;
                }
            };
            items.add(acquireTask);
        }
        List<Future<Void>> futures = executorService.invokeAll(items);
        for (Future<Void> future : futures) {
            future.get();
        }
        assertEquals(0, pool.getObjects().size());
        assertEquals(0, pool.getFreeObjects().size());
        assertEquals(0, pool.getBusyObjects().size());
    }

    @Test
    public void tesCloseWhenObjectAcquired() throws Exception {
        pool.open();
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = newFixedThreadPool(threadCount);
        List<Callable<Void>> items = newArrayList();
        int countOfAddedObjects = 1;
        for (int i = 0; i < countOfAddedObjects; i++) {
            final Person person = buildPerson("AA" + i, "BB" + i);
            pool.add(person);
            Callable<Void> acquireTask = new Callable<Void>() {
                public Void call() throws InterruptedException {
                    Person resource = pool.acquire();
                    new Thread(new Runnable() {
                        public void run() {
                            pool.close();
                        }
                    }).start();
                    Thread.sleep(2000);
                    pool.release(resource);
                    return null;
                }
            };
            items.add(acquireTask);
        }
        List<Future<Void>> futures = executorService.invokeAll(items);
        for (Future<Void> future : futures) {
            future.get();
        }
        assertFalse(pool.isOpen());
        pool.open();
        assertEquals(1, pool.getObjects().size());
        assertEquals(1, pool.getFreeObjects().size());
        assertEquals(0, pool.getBusyObjects().size());

    }
}