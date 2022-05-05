package org.apache.flink.streaming.util.recovery;

import java.io.Serializable;

@FunctionalInterface
public interface ThrowingConsumer<T, E extends Throwable> extends Serializable{
    /**
     * The work method.
     *
     * @throws E Exceptions may be thrown.
     */
    void accept(T t) throws E;
}
