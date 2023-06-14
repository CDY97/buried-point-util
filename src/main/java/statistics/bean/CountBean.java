package statistics.bean;

import java.util.List;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * @author chengengwei
 * @description 计数统计Bean
 * @date 2022/10/31
 */
public class CountBean {

    private DoubleAdder count;
    private long lastUpdateTime;
    private String indexName;
    private List<String> tagsNameList;
    private List<String> tagsValueList;

    public CountBean(String indexName, List<String> tagsNameList, List<String> tagsValueList) {

        this.indexName = indexName;
        this.tagsNameList = tagsNameList;
        this.tagsValueList = tagsValueList;
        this.count = new DoubleAdder();
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 增加值
     * @param incVal
     */
    public void increase(double incVal) {

        count.add(incVal);
        lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 减少值
     * @param redVal
     */
    public void reduce(double redVal) {

        count.add(-redVal);
        lastUpdateTime = System.currentTimeMillis();
    }

    public void reset(double newVal) {

        count.reset();
        count.add(newVal);
        lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * 获取计数结果
     * @return
     */
    public double getCount() {

        return count.doubleValue();
    }

    /**
     * 获取计数结果并清零
     * @return
     */
    public double getCountAndReset() {

        return count.sumThenReset();
    }

    public List<String> getTagsNameList() {
        return tagsNameList;
    }

    public List<String> getTagsValueList() {
        return tagsValueList;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getIndexName() {
        return indexName;
    }
}
