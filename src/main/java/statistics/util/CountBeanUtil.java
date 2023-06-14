package statistics.util;

import statistics.bean.CountBean;
import statistics.executor.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chengengwei
 * @description 计数统计工具类
 * @date 2022/9/14
 */
public class CountBeanUtil {

    private static final Logger logger = LoggerFactory.getLogger(CountBeanUtil.class);

    private Map<String, Map<BeanBuilder, CountBean>> container = new ConcurrentHashMap<>();

    public static final Long containerMaxSize = 100L;

    public static final Long beanMapMaxSize = 10000L;

    public BeanBuilder newBeanBuilder() {

        return new BeanBuilder();
    }

    public Map<String, Map<BeanBuilder, CountBean>> getContainer() {
        return container;
    }

    public class BeanBuilder {

        private String indexName;
        private List<String> tagsNameList;
        private List<String> tagsValueList;

        public BeanBuilder() {

            this.indexName = "count";
        }

        public BeanBuilder indexName(String name) {

            if (name != null && name.length() != 0) {
                StringBuilder sb = new StringBuilder();
                char[] chars = name.toCharArray();
                for (char c : chars) {
                    if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
                        sb.append(c);
                    }
                }
                this.indexName = sb.toString();
            } else {
                this.indexName = name;
            }
            return this;
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

        public void increase(Double val) {

            TaskExecutor.recordThreadPool.execute(() -> {
                Map<BeanBuilder, CountBean> beanMap = container.get(indexName);
                if (Objects.isNull(beanMap)) {
                    if (container.size() < containerMaxSize) {
                        beanMap = new ConcurrentHashMap<>();
                        container.put(indexName, beanMap);
                    } else {
                        logger.warn("the size of CountBeanUtil.container is too large, size:{}", container.size());
                        return;
                    }
                }
                CountBean bean = beanMap.get(this);
                if (Objects.isNull(bean)) {
                    if (beanMap.size() < beanMapMaxSize) {
                        bean = build();
                        bean.increase(val);
                        beanMap.put(this, bean);
                    } else {
                        logger.warn("the size of CountBeanUtil.container.get('{}') is too large, size:{}", indexName, beanMap.size());
                    }
                } else {
                    bean.increase(val);
                }
            });
        }

        public void reduce(Double val) {

            TaskExecutor.recordThreadPool.execute(() -> {
                Map<BeanBuilder, CountBean> beanMap = container.get(indexName);
                if (Objects.isNull(beanMap)) {
                    if (container.size() < containerMaxSize) {
                        beanMap = new ConcurrentHashMap<>();
                        container.put(indexName, beanMap);
                    } else {
                        logger.warn("the size of CountBeanUtil.container is too large, size:{}", container.size());
                        return;
                    }
                }
                CountBean bean = beanMap.get(this);
                if (Objects.isNull(bean)) {
                    if (beanMap.size() < beanMapMaxSize) {
                        bean = build();
                        bean.reduce(val);
                        beanMap.put(this, bean);
                    } else {
                        logger.warn("the size of CountBeanUtil.container.get('{}') is too large, size:{}", indexName, beanMap.size());
                    }
                } else {
                    bean.reduce(val);
                }
            });
        }

        public void reset(Double val) {

            TaskExecutor.recordThreadPool.execute(() -> {
                Map<BeanBuilder, CountBean> beanMap = container.get(indexName);
                if (Objects.isNull(beanMap)) {
                    if (container.size() < containerMaxSize) {
                        beanMap = new ConcurrentHashMap<>();
                        container.put(indexName, beanMap);
                    } else {
                        logger.warn("the size of CountBeanUtil.container is too large, size:{}", container.size());
                        return;
                    }
                }
                CountBean bean = beanMap.get(this);
                if (Objects.isNull(bean)) {
                    if (beanMap.size() < beanMapMaxSize) {
                        bean = build();
                        bean.reset(val);
                        beanMap.put(this, bean);
                    } else {
                        logger.warn("the size of CountBeanUtil.container.get('{}') is too large, size:{}", indexName, beanMap.size());
                    }
                } else {
                    bean.reset(val);
                }
            });
        }

        private CountBean build() {

            return new CountBean(indexName, tagsNameList, tagsValueList);
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
