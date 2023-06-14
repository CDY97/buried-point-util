package statistics.collector;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;

public class ExtCKMSQuantiles {

    private int count = 0;

    private int compressIdx = 0;

    protected LinkedList<Item> sample;

    private double[] buffer = new double[500];

    private int bufferCount = 0;

    private final Quantile quantiles[];

    public ExtCKMSQuantiles(Quantile[] quantiles) {
        this.quantiles = quantiles;
        this.sample = new LinkedList<Item>();
    }

    public void insert(double value) {
        buffer[bufferCount] = value;
        bufferCount++;

        if (bufferCount == buffer.length) {
            insertBatch();
            compress();
        }
    }

    public double get(double q) {
        insertBatch();
        compress();

        if (sample.size() == 0) {
            return Double.NaN;
        }

        int rankMin = 0;
        int desired = (int) (q * count);

        ListIterator<Item> it = sample.listIterator();
        Item prev, cur;
        cur = it.next();
        while (it.hasNext()) {
            prev = cur;
            cur = it.next();

            rankMin += prev.g;

            if (rankMin + cur.g + cur.delta > desired
                    + (allowableError(desired) / 2)) {
                return prev.value;
            }
        }

        return sample.getLast().value;
    }

    private double allowableError(int rank) {
        int size = sample.size();
        double minError = size + 1;

        for (Quantile q : quantiles) {
            double error;
            if (rank <= q.quantile * size) {
                error = q.u * (size - rank);
            } else {
                error = q.v * rank;
            }
            if (error < minError) {
                minError = error;
            }
        }

        return minError;
    }

    private boolean insertBatch() {
        if (bufferCount == 0) {
            return false;
        }

        Arrays.sort(buffer, 0, bufferCount);

        int start = 0;
        if (sample.size() == 0) {
            Item newItem = new Item(buffer[0], 1, 0);
            sample.add(newItem);
            start++;
            count++;
        }

        ListIterator<Item> it = sample.listIterator();
        Item item = it.next();

        for (int i = start; i < bufferCount; i++) {
            double v = buffer[i];
            while (it.nextIndex() < sample.size() && item.value < v) {
                item = it.next();
            }

            if (item.value > v) {
                it.previous();
            }

            int delta;
            if (it.previousIndex() == 0 || it.nextIndex() == sample.size()) {
                delta = 0;
            }
            else {
                delta = ((int) Math.floor(allowableError(it.nextIndex()))) - 1;
            }

            Item newItem = new Item(v, 1, delta);
            it.add(newItem);
            count++;
            item = newItem;
        }

        bufferCount = 0;
        return true;
    }

    private void compress() {

        if (sample.size() < 2) {
            return;
        }

        ListIterator<Item> it = sample.listIterator();
        int removed = 0;

        Item prev = null;
        Item next = it.next();

        while (it.hasNext()) {
            prev = next;
            next = it.next();

            if (prev.g + next.g + next.delta <= allowableError(it.previousIndex())) {
                next.g += prev.g;
                it.previous();
                it.previous();
                it.remove();
                it.next();
                removed++;
            }
        }
    }

    private class Item {
        public final double value;
        public int g;
        public final int delta;

        public Item(double value, int lower_delta, int delta) {
            this.value = value;
            this.g = lower_delta;
            this.delta = delta;
        }

        @Override
        public String toString() {
            return String.format("I{val=%.3f, g=%d, del=%d}", value, g, delta);
        }
    }

    public static class Quantile {
        public final double quantile;
        public final double error;
        public final double u;
        public final double v;

        public Quantile(double quantile, double error) {
            this.quantile = quantile;
            this.error = error;
            u = 2.0 * error / (1.0 - quantile);
            v = 2.0 * error / quantile;
        }

        @Override
        public String toString() {
            return String.format("Q{q=%.3f, eps=%.3f}", quantile, error);
        }
    }
}
