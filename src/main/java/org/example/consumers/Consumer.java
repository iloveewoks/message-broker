package org.example.consumers;

import kafka.utils.ShutdownableThread;

public abstract class Consumer extends ShutdownableThread {

    public Consumer(String name, boolean isInterruptible) {
        super(name, isInterruptible);
    }

}
