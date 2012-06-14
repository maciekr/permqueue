package com.heyitworks.permqueue;

/**
 * @author maciekr
 */
public interface Serializer {
    byte[] write(Object obj);

    Object read(byte[] bytes);
}
