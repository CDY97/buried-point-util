package statistics.util;

import statistics.bean.DelayBean;
import statistics.collector.ExtSummary;
import statistics.enums.MetricType;
import statistics.executor.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static statistics.enums.MetricType.*;

/**
 * @author chengengwei
 * @description 时延统计工具类
 * @date 2022/9/14
 */
public class DelayBeanUtil {

    private static final Logger logger = LoggerFactory.getLogger(DelayBeanUtil.class);

    private Map<BeanBuilder, DelayBean> container = new ConcurrentHashMap<>();

    public static final Long containerMaxSize = 10000L;

    public BeanBuilder newBeanBuilder(ExtSummary delaySummary, boolean summaryOnly) {

        return new BeanBuilder(delaySummary, summaryOnly);
    }

    public Map<BeanBuilder, DelayBean> getContainer() {
        return container;
    }

    public class BeanBuilder {

        private ExtSummary delaySummary;
        private List<String> tagsNameList;
        private List<String> tagsValueList;
        private MetricType[] types;
        private ChronoUnit chronoUnit = ChronoUnit.MILLIS;
        private int maxSize = 10000;
        private boolean summaryOnly;

        public BeanBuilder(ExtSummary delaySummary, boolean summaryOnly) {

            this.delaySummary = delaySummary;
            this.summaryOnly = summaryOnly;
            if (summaryOnly) {
                types = new MetricType[]{SUMMARY};
            } else {
                types = new MetricType[]{MAX, MIN, SUMMARY};
            }
        }

        public BeanBuilder labelNames(String... labelNames) {

            tagsNameList = new ArrayList<>();
            for (String name : labelNames) {
                if (name == null) {
                    throw new IllegalArgumentException("Label cannot be null.");
                }
                tagsNameList.add(name);
            }
            return this;
        }

        public BeanBuilder labelValues(String... labelValues) {

            tagsValueList = new ArrayList<>();
            for (String name : labelValues) {
                if (name == null) {
                    throw new IllegalArgumentException("Label cannot be null.");
                }
                tagsValueList.add(name);
            }
            return this;
        }

        public BeanBuilder unit(ChronoUnit chronoUnit) {

            this.chronoUnit = chronoUnit;
            return this;
        }

        public BeanBuilder unit(int maxSize) {

            this.maxSize = maxSize;
            return this;
        }

        public BeanBuilder metricType(MetricType... types) {

            if (this.summaryOnly) {
                this.types = new MetricType[]{SUMMARY};
            } else {
                this.types = types;
            }
            return this;
        }

        public void record(Double duration) {

            TaskExecutor.recordThreadPool.execute(() -> {
                DelayBean bean = container.get(this);
                if (Objects.isNull(bean)) {
                    if (container.size() < containerMaxSize) {
                        bean = build();
                        bean.recordDuration(duration);
                        container.put(this, bean);
                    } else {
                        logger.warn("the size of DelayBeanUtil.container is too large, size:{}", container.size());
                    }
                } else {
                    bean.recordDuration(duration);
                }
            });
        }

        private DelayBean build() {

            return new DelayBean(delaySummary, tagsNameList, tagsValueList, chronoUnit, maxSize, types);
        }

        /**
         * 重写hashcode
         * @return
         */
        @Override
        public int hashCode() {
            int hashcode = 0;
            if (tagsNameList != null) {
                for (String str : tagsNameList) {
                    hashcode |= str.hashCode();
                }
            }
            if (tagsValueList != null) {
                for (String str : tagsValueList) {
                    hashcode |= str.hashCode();
                }
            }
            return hashcode;
        }

        /**
         * 重写equals，只要tag键值对完全相同就视为同一个Child
         * @param obj
         * @return
         */
        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof BeanBuilder)) {
                return false;
            }
            BeanBuilder builder = (BeanBuilder) obj;
            if (this == builder) {
                return true;
            }
            Map<String, String> curMap = new HashMap<>();
            for (int i = 0; i < tagsNameList.size(); i++) {
                curMap.put(tagsNameList.get(i), tagsValueList.get(i));
            }
            Map<String, String> targetMap = new HashMap<>();
            for (int i = 0; i < builder.tagsNameList.size(); i++) {
                targetMap.put(builder.tagsNameList.get(i), builder.tagsValueList.get(i));
            }
            if (curMap.size() == targetMap.size()) {
                for (Map.Entry<String, String> entry : curMap.entrySet()) {
                    if (!entry.getValue().equals(targetMap.get(entry.getKey()))) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }
    }
}
