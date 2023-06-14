package statistics.bean;

import statistics.collection.FixedList;
import statistics.collector.ExtSummary;
import statistics.enums.MetricType;

import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * @author chengengwei
 * @description 时延统计Bean
 * @date 2022/10/10
 */
public class DelayBean {

    private ChronoUnit chronoUnit;
    private FixedList<Double> avgTable;
    private FixedList<Double> varianceTable;
    private double max;
    private double min;
    private boolean minFlag;
    private MetricType[] types;
    private ExtSummary.Child summaryChild;
    private List<String> tagsNameList;
    private List<String> tagsValueList;
    private long lastUpdateTime;

    public DelayBean(ExtSummary delaySummary, List<String> tagsNameList, List<String> tagsValueList, ChronoUnit chronoUnit, int maxSize, MetricType[] types) {

        this.chronoUnit = chronoUnit;
        this.types = types;
        this.tagsNameList = tagsNameList;
        this.tagsValueList = tagsValueList;
        this.lastUpdateTime = System.currentTimeMillis();
        for (MetricType type : types) {
            switch (type) {
                case MAX:
                    break;
                case MIN:
                    this.min = Double.MAX_VALUE;
                    minFlag = false;
                    break;
                case AVG:
                    this.avgTable = new FixedList<>(maxSize);
                    break;
                case VARIANCE:
                    this.varianceTable = new FixedList<>(maxSize);
                    break;
                case SUMMARY:
                    this.summaryChild = delaySummary.childBuilder().labelNames(tagsNameList).labelValues(tagsValueList).build();
                    break;
            }
        }
    }

    /**
     * 记录持续时间
     * @param duration
     */
    public void recordDuration(Double duration) {

        switch (chronoUnit) {
            case NANOS:
                duration /= 1000000;
                break;
        }
        for (MetricType type : types) {
            switch (type) {
                case MAX:
                    max = Math.max(max, duration);
                    break;
                case MIN:
                    min = Math.min(min, duration);
                    minFlag = true;
                    break;
                case AVG:
                    avgTable.add(duration);
                    break;
                case VARIANCE:
                    varianceTable.add(duration);
                    break;
                case SUMMARY:
                    summaryChild.observe(duration);
                    break;
            }
        }
        lastUpdateTime = System.currentTimeMillis();
        minFlag = true;
    }

    /**
     * 获取最大值
     * @return
     */
    public double getMax() {

        double res = max;
        max = 0d;
        return res;
    }

    /**
     * 获取最小值
     * @return
     */
    public double getMin() {

        double res = minFlag ? min : 0L;
        min = Double.MAX_VALUE;
        minFlag = false;
        return res;
    }

    /**
     * 获取平均值
     * @return
     */
    public double getAvg() {

        double res = avgTable.getList().stream().mapToDouble(i -> i).average().orElse(0);
        avgTable = new FixedList<>(avgTable.getMaxSize());
        return res;
    }

    /**
     * 获取方差
     * @return
     */
    public double getVariance() {

        double avg = varianceTable.getList().stream().mapToDouble(i -> i).average().orElse(0);
        double res = varianceTable.getList().stream().map(i -> Math.pow(i - avg, 2)).mapToDouble(i -> i).average().orElse(0);
        varianceTable = new FixedList<>(varianceTable.getMaxSize());
        return res;
    }

    public MetricType[] getTypes() {
        return types;
    }

    public List<String> getTagsNameList() {
        return tagsNameList;
    }

    public List<String> getTagsValueList() {
        return tagsValueList;
    }

    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public ExtSummary.Child getSummaryChild() {
        return summaryChild;
    }
}
