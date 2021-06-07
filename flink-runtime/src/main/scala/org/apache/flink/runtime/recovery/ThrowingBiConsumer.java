package org.apache.flink.runtime.recovery;

import java.io.Serializable;

@FunctionalInterface
public interface ThrowingBiConsumer<X,Y, E extends Throwable> extends Serializable{
    /**
     * The work method.
     *
     * @throws E Exceptions may be thrown.
     */
    void accept(X x, Y y) throws E;
}
