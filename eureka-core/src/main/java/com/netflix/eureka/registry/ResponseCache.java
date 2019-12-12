package com.netflix.eureka.registry;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author David Liu
 */
public interface ResponseCache {

    //过期缓存。
    void invalidate(String appName, @Nullable String vipAddress, @Nullable String secureVipAddress);

    //这2个已经废弃
    AtomicLong getVersionDelta();

    AtomicLong getVersionDeltaWithRegions();

    /**
     * Get the cached information about applications.
     *
     * <p>
     * If the cached information is not available it is generated on the first
     * request. After the first request, the information is then updated
     * periodically by a background thread.
     * </p>
     *
     * @param key the key for which the cached information needs to be obtained.
     * @return payload which contains information about the applications.
     */
    //zty获得缓存。
     String get(Key key);

    /**
     * Get the compressed information about the applications.
     *
     * @param key the key for which the compressed cached information needs to be obtained.
     * @return compressed payload which contains information about the applications.
     */
    //获得缓存，并 GZIP 。
    byte[] getGZIP(Key key);

    /**
     * Performs a shutdown of this cache by stopping internal threads and unregistering
     * Servo monitors.
     */
    void stop();
}
