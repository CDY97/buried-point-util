package statistics.collector;

import io.prometheus.client.Collector;
import io.prometheus.client.DoubleAdder;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.SimpleCollector;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author chengengwei
 * @description 自定义Gauge指标，用于解决原生框架同一个job下name相同的Gauge会互相覆盖，并且Gauge下children标签必须相同的问题
 * @date 2022/10/18
 */
public class ExtGauge extends SimpleCollector<ExtGauge.Child> implements Collector.Describable {

    protected final ConcurrentMap<ChildBuilder, Child> children = new ConcurrentHashMap<>();

    public ChildBuilder childBuilder() {
        return new ChildBuilder();
    }

    ExtGauge(Builder b) {
        super(b);
    }

    public static class Builder extends SimpleCollector.Builder<Builder, ExtGauge> {
        @Override
        public ExtGauge create() {
            return new ExtGauge(this);
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
        return new Child();
    }

    public static class Timer implements Closeable {
        private final Child child;
        private final long start;
        private Timer(Child child) {
            this.child = child;
            start = Child.timeProvider.nanoTime();
        }

        public double setDuration() {
            double elapsed = (Child.timeProvider.nanoTime() - start) / NANOSECONDS_PER_SECOND;
            child.set(elapsed);
            return elapsed;
        }

        @Override
        public void close() {
            setDuration();
        }
    }

    /**
     * 增加ChildBuilder内部类，将定义tagname和tagvalue的动作推迟到设置指标值时，使同一个Gauge实例下的children可以有不同的标签
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
                    Child child = new Child();
                    child.tagsNameList = tagsNameList == null ? new ArrayList<>() : tagsNameList;
                    child.tagsValueList = tagsValueList == null ? new ArrayList<>() : tagsValueList;
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

    public static class Child {

        private final DoubleAdder value = new DoubleAdder();

        private List<String> tagsNameList;
        private List<String> tagsValueList;

        static TimeProvider timeProvider = new TimeProvider();

        public void inc() {
            inc(1);
        }

        public void inc(double amt) {
            value.add(amt);
        }

        public void dec() {
            dec(1);
        }

        public void dec(double amt) {
            value.add(-amt);
        }

        public void set(double val) {
            value.set(val);
        }

        public void setToCurrentTime() {
            set(timeProvider.currentTimeMillis() / MILLISECONDS_PER_SECOND);
        }

        public Timer startTimer() {
            return new Timer(this);
        }

        public double setToTime(Runnable timeable){
            Timer timer = startTimer();

            double elapsed;
            try {
                timeable.run();
            } finally {
                elapsed = timer.setDuration();
            }

            return elapsed;
        }

        public <E> E setToTime(Callable<E> timeable){
            Timer timer = startTimer();

            try {
                return timeable.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                timer.setDuration();
            }
        }

        public double get() {
            return value.sum();
        }
    }

    public void inc() {
        inc(1);
    }

    public void inc(double amt) {
        noLabelsChild.inc(amt);
    }

    public void dec() {
        dec(1);
    }

    public void dec(double amt) {
        noLabelsChild.dec(amt);
    }

    public void set(double val) {
        noLabelsChild.set(val);
    }

    public void setToCurrentTime() {
        noLabelsChild.setToCurrentTime();
    }

    public Timer startTimer() {
        return noLabelsChild.startTimer();
    }

    public double setToTime(Runnable timeable){
        return noLabelsChild.setToTime(timeable);
    }

    public <E> E setToTime(Callable<E> timeable){
        return noLabelsChild.setToTime(timeable);
    }

    public double get() {
        return noLabelsChild.get();
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>(children.size());
        for(Map.Entry<ChildBuilder, Child> c: children.entrySet()) {
            samples.add(new MetricFamilySamples.Sample(fullname, c.getValue().tagsNameList, c.getValue().tagsValueList, c.getValue().get()));
        }
        return familySamplesList(Type.GAUGE, samples);
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return Collections.<MetricFamilySamples>singletonList(new GaugeMetricFamily(fullname, help, labelNames));
    }

    static class TimeProvider {
        long currentTimeMillis() {
            return System.currentTimeMillis();
        }
        long nanoTime() {
            return System.nanoTime();
        }
    }
}
