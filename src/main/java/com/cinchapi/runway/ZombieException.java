package com.cinchapi.runway;

/**
 * A {@link ZombieException} is thrown when an attempt is made to load a record
 * that was accidentally saved, even though it is a Zombie
 * 
 * @author jnelson
 */
@SuppressWarnings("serial")
public class ZombieException extends RuntimeException {

    public ZombieException() {
        super("Encountered a zombie attack!");
    }

}
