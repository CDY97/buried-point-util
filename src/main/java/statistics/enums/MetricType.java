package statistics.enums;

/**
 * @author chengengwei
 * @description 时延统计指标枚举类型
 * @date 2022/10/10
 */
public enum MetricType {
    MAX("delay_max", "时延最大值"),
    MIN("delay_min", "时延最小值"),
    AVG("delay_avg", "时延平均值"),
    SUM("delay_sum", "时延总和"),
    VARIANCE("delay_variance", "时延方差"),
    SUMMARY("delay_summary", "时延分布");

    private String name;

    private String description;

    MetricType(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}
