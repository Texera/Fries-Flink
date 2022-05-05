package org.apache.flink.streaming.util.recovery;

import java.io.Serializable;

@FunctionalInterface
public interface ThrowingTriConsumer<A,B,C, E extends Throwable> extends Serializable {

    void accept(A a, B b, C c) throws E;
}
