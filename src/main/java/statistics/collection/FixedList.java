package statistics.collection;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author chengengwei
 * @description 定长列表
 * @date 2022/10/10
 */
public class FixedList<E> {

    private ConcurrentLinkedQueue<E> list = new ConcurrentLinkedQueue<>();
    private int maxSize;

    public FixedList(int maxSize) {

        this.maxSize = Math.max(0, maxSize);
    }

    public void add(E e) {

        list.offer(e);
        if (list.size() > maxSize) {
            list.poll();
        }
    }

    public ConcurrentLinkedQueue<E> getList() {
        return list;
    }

    public int getMaxSize() {
        return maxSize;
    }
}
