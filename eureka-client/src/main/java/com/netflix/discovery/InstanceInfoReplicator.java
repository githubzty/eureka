package com.netflix.discovery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A task for updating and replicating the local instanceinfo to the remote server. Properties of this task are:
 * - configured with a single update thread to guarantee sequential update to the remote server
 * - update tasks can be scheduled on-demand via onDemandUpdate()
 * - task processing is rate limited by burstSize
 * - a new update task is always scheduled automatically after an earlier update task. However if an on-demand task
 *   is started, the scheduled automatic update task is discarded (and a new one will be scheduled after the new
 *   on-demand update).
 *
 *   @author dliu
 */
class InstanceInfoReplicator implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(InstanceInfoReplicator.class);

    private final DiscoveryClient discoveryClient;

    /**
     * zty应用实例信息
     */
    private final InstanceInfo instanceInfo;
    /**
     * zty定时执行频率，单位：秒
     */
    private final int replicationIntervalSeconds;
    /**
     * zty定时执行器
     */
    private final ScheduledExecutorService scheduler;
    /**
     * zty定时执行任务的 Future
     */
    private final AtomicReference<Future> scheduledPeriodicRef;
    /**
     * zty是否开启调度
     */
    private final AtomicBoolean started;

    //zty限流相关三个，跳过
    private final RateLimiter rateLimiter;
    private final int burstSize;
    private final int allowedRatePerMinute;

    InstanceInfoReplicator(DiscoveryClient discoveryClient, InstanceInfo instanceInfo, int replicationIntervalSeconds, int burstSize) {
        this.discoveryClient = discoveryClient;
        this.instanceInfo = instanceInfo;
        this.scheduler = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder()
                        .setNameFormat("DiscoveryClient-InstanceInfoReplicator-%d")
                        .setDaemon(true)
                        .build());

        this.scheduledPeriodicRef = new AtomicReference<Future>();

        this.started = new AtomicBoolean(false);
        this.rateLimiter = new RateLimiter(TimeUnit.MINUTES);
        this.replicationIntervalSeconds = replicationIntervalSeconds;
        this.burstSize = burstSize;

        this.allowedRatePerMinute = 60 * this.burstSize / this.replicationIntervalSeconds;
        logger.info("InstanceInfoReplicator onDemand update allowed rate per min is {}", allowedRatePerMinute);
    }

    //zty
    //执行 instanceInfo.setIsDirty() 代码块，因为 InstanceInfo 刚被创建时，在 Eureka-Server 不存在，也会被注册。
    //调用 ScheduledExecutorService#schedule(...) 方法，延迟 initialDelayMs 毫秒执行一次任务。
    // 为什么此处设置 scheduledPeriodicRef ？在 InstanceInfoReplicator#onDemandUpdate() 方法会看到具体用途。
    public void start(int initialDelayMs) {
        if (started.compareAndSet(false, true)) {
            // zty设置 应用实例信息 数据不一致
            instanceInfo.setIsDirty();  // for initial register
            // zty提交任务，并设置该任务的 Future
            Future next = scheduler.schedule(this, initialDelayMs, TimeUnit.SECONDS);
            scheduledPeriodicRef.set(next);
        }
    }

    public void stop() {
        shutdownAndAwaitTermination(scheduler);
        started.set(false);
    }

    private void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("InstanceInfoReplicator stop interrupted");
        }
    }

    //zty
    public boolean onDemandUpdate() {
        if (rateLimiter.acquire(burstSize, allowedRatePerMinute)) {         // 限流相关，跳过
            if (!scheduler.isShutdown()) {
                scheduler.submit(new Runnable() {
                    @Override
                    public void run() {
                        logger.debug("Executing on-demand update of local InstanceInfo");
                        // zty取消任务
                        Future latestPeriodic = scheduledPeriodicRef.get();
                        if (latestPeriodic != null && !latestPeriodic.isDone()) {
                            logger.debug("Canceling the latest scheduled update, it will be rescheduled at the end of on demand update");
                            //zty调用 Future#cancel(false) 方法，取消定时任务，避免无用的注册。
                            latestPeriodic.cancel(false);
                        }
                        // zty再次调用
                        //调用 InstanceInfoReplicator#run() 方法，发起注册。
                        InstanceInfoReplicator.this.run();
                    }
                });
                return true;
            } else {
                logger.warn("Ignoring onDemand update due to stopped scheduler");
                return false;
            }
        } else {
            logger.warn("Ignoring onDemand update due to rate limiter");
            return false;
        }
    }

    //zty定时检查 InstanceInfo 的状态( status ) 属性是否发生变化。若是，发起注册。注册完设置为数据一致，等下次不一致
    //调用 DiscoveryClient#refreshInstanceInfo() 方法，刷新应用实例信息。此处可能导致应用实例信息数据不一致
    //调用 DiscoveryClient#register() 方法，Eureka-Client 向 Eureka-Server 注册应用实例。
    //调用 ScheduledExecutorService#schedule(...) 方法，再次延迟执行任务，并设置 scheduledPeriodicRef。通过这样的方式，不断循环定时执行任务。
    public void run() {
        try {
            // zty刷新 应用实例信息
            discoveryClient.refreshInstanceInfo();
            // zty判断 应用实例信息 是否数据不一致
            Long dirtyTimestamp = instanceInfo.isDirtyWithTime();
            if (dirtyTimestamp != null) {
                // zty发起注册
                discoveryClient.register();
                // zty设置 应用实例信息 数据一致
                instanceInfo.unsetIsDirty(dirtyTimestamp);
            }
        } catch (Throwable t) {
            logger.warn("There was a problem with the instance info replicator", t);
        } finally {
            // zty提交任务，并设置该任务的 Future
            Future next = scheduler.schedule(this, replicationIntervalSeconds, TimeUnit.SECONDS);
            scheduledPeriodicRef.set(next);
        }
    }

}
