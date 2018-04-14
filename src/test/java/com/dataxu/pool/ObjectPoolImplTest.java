package com.dataxu.pool;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.rules.ExpectedException.none;

public class ObjectPoolImplTest {


    private ObjectPoolImpl<Person> pool = new ObjectPoolImpl<Person>(Collections.<Person>emptySet());

    @Rule
    public ExpectedException expectedException = none();

    @Test
    public void testOpen() {
        pool.open();
        assertTrue(pool.isOpen());
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
    public void acquire() {
        pool.open();
        pool.acquire();
    }

    @Test
    public void acquire1() {
    }

    @Test
    public void release() {
    }

    @Test
    public void add() {
    }

    @Test
    public void remove() {
    }
}