package statistics.util;

import statistics.bean.CountBean;
import statistics.bean.DelayBean;
import statistics.collector.ExtGauge;
import statistics.collector.ExtSummary;
import statistics.enums.MetricType;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static statistics.enums.MetricType.*;

/**
 * @author chengengwei
 * @description 埋点工具类，推送频率需要和prometheus拉取频率一致，否则会导致数据被覆盖
 * @date 2022/9/14
 */
public class BuriedPointRecordUtil {

    private static final Logger logger = LoggerFactory.getLogger(BuriedPointRecordUtil.class);

    private static Map<String, BuriedPointRecordUtil> container = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduledThreadPool;

    private ExtGauge delayMax;
    private ExtGauge delayMin;
    private ExtGauge delayAvg;
    private ExtGauge delayVariance;
    private ExtSummary delaySummary;
    private Map<String, ExtGauge> countMap;

    private PushGateway pushGateway;
    // 推送频率
    private long period;
    private TimeUnit unit;
    private String url;
    private String instance;
    private String timeStamp;
    // tag全都相同的某一指标超过expiration时间内没有新值则从缓存中移除
    private long expiration;
    private boolean summaryOnly;
    // 每推送多少次清除一次job
    private int clearRate;

    private AtomicBoolean state;
    private AtomicLong pushCount;

    private volatile DelayBeanUtil delayBeanUtil;

    private volatile CountBeanUtil countBeanUtil;

    private BuriedPointRecordUtil(String url, String instance, long period, boolean summaryOnly) {

        this.pushGateway = new PushGateway(url);
        this.period = period;
        this.unit = TimeUnit.MILLISECONDS;
        this.url = url;
        this.instance = instance;
        this.timeStamp = String.valueOf(System.currentTimeMillis());
        this.expiration = 10000L;
        this.summaryOnly = summaryOnly;
        this.clearRate = 3600;

        this.state = new AtomicBoolean(false);
        this.pushCount = new AtomicLong(0L);
        createNewMetricBean();
        startPushTask();
    }

    public static BuriedPointRecordUtil getInstance(String url, String instance, long period) {

        BuriedPointRecordUtil ins = container.computeIfAbsent(url, key -> new BuriedPointRecordUtil(url, instance, period, false));
        return ins;
    }

    public static BuriedPointRecordUtil getInstance(String url, String instance) {

        return getInstance(url, instance, 5000L);
    }

    /**
     * 生成只统计时延summary的统计实例
     * @param url
     * @param instance
     * @return
     */
    public static BuriedPointRecordUtil getSummaryInstance(String url, String instance) {

        return getSummaryInstance(url, instance, 5000L);
    }

    public static BuriedPointRecordUtil getSummaryInstance(String url, String instance, long period) {

        BuriedPointRecordUtil ins = container.computeIfAbsent(url, key -> new BuriedPointRecordUtil(url, instance, period, true));
        return ins;
    }

    public DelayBeanUtil.BeanBuilder delayBeanBuilder() {

        if (delayBeanUtil == null) {
            synchronized (this) {
                if (delayBeanUtil == null) {
                    delayBeanUtil = new DelayBeanUtil();
                }
            }
        }
        return delayBeanUtil.newBeanBuilder(delaySummary, summaryOnly);
    }

    public CountBeanUtil.BeanBuilder countBeanBuilder() {

        if (countBeanUtil == null) {
            synchronized (this) {
                if (countBeanUtil == null) {
                    countBeanUtil = new CountBeanUtil();
                }
            }
        }
        return countBeanUtil.newBeanBuilder();
    }

    public synchronized void startPushTask() {

        if (!state.get()) {
            scheduledThreadPool = Executors.newScheduledThreadPool(1);
            scheduledThreadPool.scheduleAtFixedRate(new PushTask(), 0, period, unit);
            state.set(true);
        }
    }

    public synchronized void stopPushTask() {

        if (state.get()) {
            scheduledThreadPool.shutdown();
            state.set(false);
        }
    }

    private synchronized void createNewMetricBean() {

        String summaryName = this.summaryOnly ? this.instance + "_" + SUMMARY.getName() : SUMMARY.getName();
        this.delayMax = ExtGauge.build().name(MAX.getName()).help(MAX.getDescription()).create();
        this.delayMin = ExtGauge.build().name(MIN.getName()).help(MIN.getDescription()).create();
        this.delayAvg = ExtGauge.build().name(AVG.getName()).help(AVG.getDescription()).create();
        this.delayVariance = ExtGauge.build().name(VARIANCE.getName()).help(VARIANCE.getDescription()).create();
        this.delaySummary = ExtSummary.build().name(summaryName).help(SUMMARY.getDescription())
                .quantile(0.5, 0).quantile(0.9, 0).quantile(0.99, 0)
                .quantile(0.999, 0).maxAgeSeconds(4 * 15).ageBuckets(4).create();
        this.countMap = new ConcurrentHashMap<>();
    }

    private synchronized void refreshMetricBean() {

        this.delayMax = ExtGauge.build().name(MAX.getName()).help(MAX.getDescription()).create();
        this.delayMin = ExtGauge.build().name(MIN.getName()).help(MIN.getDescription()).create();
        this.delayAvg = ExtGauge.build().name(AVG.getName()).help(AVG.getDescription()).create();
        this.delayVariance = ExtGauge.build().name(VARIANCE.getName()).help(VARIANCE.getDescription()).create();
        this.countMap = new ConcurrentHashMap<>();
    }

    private synchronized void addMetric(DelayBean bean) {

        try {
            for (MetricType t : bean.getTypes()) {
                switch (t) {
                    case MAX:
                        delayMax.childBuilder().labelNames(bean.getTagsNameList()).labelValues(bean.getTagsValueList()).build().set(bean.getMax());
                        break;
                    case MIN:
                        delayMin.childBuilder().labelNames(bean.getTagsNameList()).labelValues(bean.getTagsValueList()).build().set(bean.getMin());
                        break;
                    case AVG:
                        delayAvg.childBuilder().labelNames(bean.getTagsNameList()).labelValues(bean.getTagsValueList()).build().set(bean.getAvg());
                        break;
                    case VARIANCE:
                        delayVariance.childBuilder().labelNames(bean.getTagsNameList()).labelValues(bean.getTagsValueList()).build().set(bean.getVariance());
                        break;
                    case SUMMARY:
                        break;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to add metric", e);
        }
    }

    private synchronized void push() {

        try {
            String jobName = String.format("%s/%s", instance, timeStamp);
            // 每推送clearRate次清空一次job数据，清除失效指标
            if (pushCount.getAndIncrement() % clearRate == 0) {
                pushGateway.delete(jobName);
            }
            pushGateway.pushAdd(delayMax, jobName);
            pushGateway.pushAdd(delayMin, jobName);
            pushGateway.pushAdd(delayAvg, jobName);
            pushGateway.pushAdd(delayVariance, jobName);
            pushGateway.pushAdd(delaySummary, jobName);
            for (Map.Entry<String, ExtGauge> entry : countMap.entrySet()) {
                pushGateway.pushAdd(entry.getValue(), jobName);
            }
        } catch (IOException e) {
            logger.error("Failed to push data to pushgateway", e);
        }
    }

    class PushTask implements Runnable {

        @Override
        public void run() {
            Long curTime = System.currentTimeMillis();
            // 处理时延指标
            if (Objects.nonNull(delayBeanUtil)) {
                Iterator<Map.Entry<DelayBeanUtil.BeanBuilder, DelayBean>> iterator = delayBeanUtil.getContainer().entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<DelayBeanUtil.BeanBuilder, DelayBean> entry = iterator.next();
                    // 如果长时间没有新值则从内存中移除
                    if (curTime - entry.getValue().getLastUpdateTime() > expiration) {
                        delaySummary.removeChild(entry.getValue().getSummaryChild());
                        iterator.remove();
                    } else {
                        addMetric(entry.getValue());
                    }
                }
            }
            // 处理计数指标
            if (Objects.nonNull(countBeanUtil)) {
                Iterator<Map.Entry<String, Map<CountBeanUtil.BeanBuilder, CountBean>>> iterator = countBeanUtil.getContainer().entrySet().iterator();
                // 处理每一种计数指标
                while (iterator.hasNext()) {
                    Map.Entry<String, Map<CountBeanUtil.BeanBuilder, CountBean>> mapEntry = iterator.next();
                    String indexName = mapEntry.getKey();
                    Map<CountBeanUtil.BeanBuilder, CountBean> beanMap = mapEntry.getValue();
                    if (Objects.nonNull(beanMap)) {
                        List<CountBean> countBeanList = new ArrayList<>();
                        Iterator<Map.Entry<CountBeanUtil.BeanBuilder, CountBean>> beanIterator = beanMap.entrySet().iterator();
                        // 处理每种计数指标下的每一条时间序列
                        while (beanIterator.hasNext()) {
                            Map.Entry<CountBeanUtil.BeanBuilder, CountBean> beanEntry = beanIterator.next();
                            // 如果长时间没有新值则从内存中移除
                            if (curTime - beanEntry.getValue().getLastUpdateTime() > expiration) {
                                beanIterator.remove();
                            } else {
                                countBeanList.add(beanEntry.getValue());
                            }
                        }
                        if (countBeanList.size() > 0) {
                            ExtGauge gauge = ExtGauge.build().name(indexName).help(indexName).create();
                            for (CountBean bean : countBeanList) {
                                gauge.childBuilder().labelNames(bean.getTagsNameList()).labelValues(bean.getTagsValueList())
                                    .build().set(bean.getCountAndReset());
                            }
                            countMap.put(indexName, gauge);
                        }
                    }
                }
            }
            push();
            // 释放内存
            refreshMetricBean();
        }
    }
}
