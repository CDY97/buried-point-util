package statistics.collector;

import io.prometheus.client.*;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author chengengwei
 * @description 自定义Summary指标，用于解决原生框架同一个job下name相同的Summary会互相覆盖，并且Summary下children标签必须相同的问题
 * @date 2022/10/18
 */
public class ExtSummary extends SimpleCollector<ExtSummary.Child> implements Counter.Describable {

    protected final ConcurrentMap<ChildBuilder, Child> children = new ConcurrentHashMap<>();
    protected final ConcurrentMap<Child, ChildBuilder> childrenInvertedIndex = new ConcurrentHashMap<>();
    final List<ExtCKMSQuantiles.Quantile> quantiles;
    final long maxAgeSeconds;
    final int ageBuckets;

    ExtSummary(Builder b) {
        super(b);
        this.quantiles = Collections.unmodifiableList(new ArrayList<>(b.quantiles));
        this.maxAgeSeconds = b.maxAgeSeconds;
        this.ageBuckets = b.ageBuckets;
    }

    public ChildBuilder childBuilder() {
        return new ChildBuilder();
    }

    public void removeChild(Child child) {

        ChildBuilder childBuilder = this.childrenInvertedIndex.remove(child);
        this.children.remove(childBuilder);
    }

    public static class Builder extends SimpleCollector.Builder<Builder, ExtSummary> {

        private final List<ExtCKMSQuantiles.Quantile> quantiles = new ArrayList<>();
        private long maxAgeSeconds = TimeUnit.MINUTES.toSeconds(10);
        private int ageBuckets = 5;

        public Builder quantile(double quantile, double error) {
            if (quantile < 0.0 || quantile > 1.0) {
                throw new IllegalArgumentException("Quantile " + quantile + " invalid: Expected number between 0.0 and 1.0.");
            }
            if (error < 0.0 || error > 1.0) {
                throw new IllegalArgumentException("Error " + error + " invalid: Expected number between 0.0 and 1.0.");
            }
            quantiles.add(new ExtCKMSQuantiles.Quantile(quantile, error));
            return this;
        }

        public Builder maxAgeSeconds(long maxAgeSeconds) {
            if (maxAgeSeconds <= 0) {
                throw new IllegalArgumentException("maxAgeSeconds cannot be " + maxAgeSeconds);
            }
            this.maxAgeSeconds = maxAgeSeconds;
            return this;
        }

        public Builder ageBuckets(int ageBuckets) {
            if (ageBuckets <= 0) {
                throw new IllegalArgumentException("ageBuckets cannot be " + ageBuckets);
            }
            this.ageBuckets = ageBuckets;
            return this;
        }

        @Override
        public ExtSummary create() {
            return new ExtSummary(this);
        }
    }

    public static Builder build(String name, String help) {
        return new Builder().name(name).help(help);
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    protected Child newChild() {
        return new Child(quantiles, maxAgeSeconds, ageBuckets);
    }

    /**
     * 增加ChildBuilder内部类，将定义tagname和tagvalue的动作推迟到设置指标值时，使同一个Summary实例下的children可以有不同的标签
     */
    public class ChildBuilder {

        private List<String> tagsNameList;
        private List<String> tagsValueList;

        /**
         * 设置标签名
         * @param labelNames
         * @return
         */
        public ChildBuilder labelNames(List<String> labelNames) {
            tagsNameList = new ArrayList<>();
            for (String name : labelNames) {
                if (name == null) {
                    throw new IllegalArgumentException("Label cannot be null.");
                }
                tagsNameList.add(name);
            }
            return this;
        }

        /**
         * 设置标签值，需要和标签名一一对应
         * @param labelValues
         * @return
         */
        public ChildBuilder labelValues(List<String> labelValues) {
            if (tagsNameList == null) {
                throw new IllegalArgumentException("LabelNames has not been assigned.");
            } else if (labelValues.size() != tagsNameList.size()) {
                throw new IllegalArgumentException("Incorrect number of labels.");
            }
            tagsValueList = new ArrayList<>();
            for (String value : labelValues) {
                if (value == null) {
                    throw new IllegalArgumentException("Label cannot be null.");
                }
                tagsValueList.add(value);
            }
            return this;
        }

        public Child build() {
            if (tagsNameList == null && tagsValueList == null ||
                    tagsNameList != null && tagsValueList != null && tagsNameList.size() == tagsValueList.size()) {
                return children.computeIfAbsent(this, (key) -> {
                    Child child = new Child(quantiles, maxAgeSeconds, ageBuckets);
                    child.tagsNameList = tagsNameList == null ? new ArrayList<>() : tagsNameList;
                    child.tagsValueList = tagsValueList == null ? new ArrayList<>() : tagsValueList;
                    childrenInvertedIndex.put(child, key);
                    return child;
                });
            } else {
                throw new IllegalArgumentException("The number of tag names and tag values are different.");
            }
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
            if(!(obj instanceof ChildBuilder)) {
                return false;
            }
            ChildBuilder builder = (ChildBuilder) obj;
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

    public static class Timer implements Closeable {
        private final Child child;
        private final long start;
        private Timer(Child child, long start) {
            this.child = child;
            this.start = start;
        }
        public double observeDuration() {
            double elapsed = SimpleTimer.elapsedSecondsFromNanos(start, ExtSimpleTimer.defaultTimeProvider.nanoTime());
            child.observe(elapsed);
            return elapsed;
        }

        @Override
        public void close() {
            observeDuration();
        }
    }

    public static class Child {

        private List<String> tagsNameList;
        private List<String> tagsValueList;

        public double time(Runnable timeable) {
            Timer timer = startTimer();

            double elapsed;
            try {
                timeable.run();
            } finally {
                elapsed = timer.observeDuration();
            }
            return elapsed;
        }

        public <E> E time(Callable<E> timeable) {
            Timer timer = startTimer();

            try {
                return timeable.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                timer.observeDuration();
            }
        }

        public static class Value {
            public final double count;
            public final double sum;
            public final SortedMap<Double, Double> quantiles;
            public final long created;

            private Value(double count, double sum, List<ExtCKMSQuantiles.Quantile> quantiles, ExtTimeWindowQuantiles quantileValues, long created) {
                this.count = count;
                this.sum = sum;
                this.quantiles = Collections.unmodifiableSortedMap(snapshot(quantiles, quantileValues));
                this.created = created;
            }

            private SortedMap<Double, Double> snapshot(List<ExtCKMSQuantiles.Quantile> quantiles, ExtTimeWindowQuantiles quantileValues) {
                SortedMap<Double, Double> result = new TreeMap<Double, Double>();
                for (ExtCKMSQuantiles.Quantile q : quantiles) {
                    result.put(q.quantile, quantileValues.get(q.quantile));
                }
                return result;
            }
        }

        private final DoubleAdder count = new DoubleAdder();
        private final DoubleAdder sum = new DoubleAdder();
        private final List<ExtCKMSQuantiles.Quantile> quantiles;
        private final ExtTimeWindowQuantiles quantileValues;
        private final long created = System.currentTimeMillis();

        private Child(List<ExtCKMSQuantiles.Quantile> quantiles, long maxAgeSeconds, int ageBuckets) {
            this.quantiles = quantiles;
            if (quantiles != null && quantiles.size() > 0) {
                quantileValues = new ExtTimeWindowQuantiles(quantiles.toArray(new ExtCKMSQuantiles.Quantile[]{}), maxAgeSeconds, ageBuckets);
            } else {
                quantileValues = null;
            }
        }

        public void observe(double amt) {
            count.add(1);
            sum.add(amt);
            if (quantileValues != null) {
                quantileValues.insert(amt);
            }
        }
        public Timer startTimer() {
            return new Timer(this, ExtSimpleTimer.defaultTimeProvider.nanoTime());
        }
        public Value get() {
            return new Value(count.sum(), sum.sum(), quantiles, quantileValues, created);
        }
    }

    public void observe(double amt) {
        noLabelsChild.observe(amt);
    }

    public Timer startTimer() {
        return noLabelsChild.startTimer();
    }

    public double time(Runnable timeable){
        return noLabelsChild.time(timeable);
    }

    public <E> E time(Callable<E> timeable){
        return noLabelsChild.time(timeable);
    }

    public Child.Value get() {
        return noLabelsChild.get();
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = new ArrayList<>();
        for(Map.Entry<ChildBuilder, Child> c: children.entrySet()) {
            Child.Value v = c.getValue().get();
            List<String> labelNamesWithQuantile = new ArrayList<>(c.getValue().tagsNameList);
            labelNamesWithQuantile.add("quantile");
            for(Map.Entry<Double, Double> q : v.quantiles.entrySet()) {
                List<String> labelValuesWithQuantile = new ArrayList<>(c.getValue().tagsValueList);
                labelValuesWithQuantile.add(doubleToGoString(q.getKey()));
                samples.add(new MetricFamilySamples.Sample(fullname, labelNamesWithQuantile, labelValuesWithQuantile, q.getValue()));
            }
        }

        return familySamplesList(Type.SUMMARY, samples);
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return Collections.<MetricFamilySamples>singletonList(new SummaryMetricFamily(fullname, help, labelNames));
    }
}
