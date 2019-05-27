package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * the x-axis is value-groups e.g. 1-3, 4-6
     * the y-axis is the total cnt of a group (cnt of 1 + cnt of 2 + cnt of 3)
     *
     * for example min=8 max=29 buckets=6
     *   divide into group 8-11, 12-15, 16-19, 20-23, 24-26, 27-29
     *   groups contains 4 values: 8-11, 12-15, 16-19, 20-23
     *   groups contains 3 values: 24-26, 27-29
     * Therefore
     *   width1 = 4, width2 = 3, valueMedian = 24
     * To histogram a value v, e.g. 20 24 or 27
     *   20 < valueMedian, (20 - min) / width1 = 3 -> group3 (20-23)
     *   24 = valueMedian, (valueMedian - min) / width1 = 4 -> group4 (24-26)
     *   27 > valueMedian, (27-valueMedian)/width2 + (valueMedian-min)/width1 = 5 -> group5 (27-29)
     */
    private Pillar[] histogram;
    private int width1;
    private int width2;
    private int valueMedian;

    private int min;
    private int max;
    private int totalCnt;


    /**
     * A histogram contains # buckets of pillars
     * a Pillar corresponds to a group e.g. 1-3     left = 1   right = 4
     */
    private class Pillar {
        int left;
        int right;
        int cnt;

        public Pillar(int left, int right, int cnt) {
            this.left = left;
            this.right = right;
            this.cnt = cnt;
        }

        public void add() {
            cnt++;
        }

        @Override
        public String toString() {
            String str = new String();
            str += "[";
            str += this.left + ",";
            str += this.right + ",";
            str += this.cnt + "] ";

            return str;
        }
    }


    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        totalCnt = 0;

        int diff = max - min + 1;
        assert diff >= buckets : "buckerts < diff, make no sense";
        int quotient = diff / buckets; // (29 - 8 + 1) / 6 = 3
        int remainder = diff % buckets; // (29 - 8 + 1) % 6 = 4

        // init width1 width2 and valueMedian
        if (remainder == 0) {
            width1 = quotient;
            width2 = 0;
            valueMedian = Integer.MAX_VALUE;
        } else {
            width1 = quotient + 1;
            width2 = quotient;
            valueMedian = min + remainder * width1; // 8 + 4 * (3+1)
        }

        int[] x_values = new int[buckets+1];
        x_values[0] = min;
        x_values[buckets] = max + 1;

        int val = min;
        if (width2 == 0) {
            for (int i = 1; i < buckets; i++) {
                val += width1;
                x_values[i] = val;
            }
        } else {
            for (int i = 1; i < buckets; i++) {
                if (val < valueMedian)
                    val += width1;
                else
                    val += width2;
                x_values[i] = val;
            }
        }

        // init Pillars
        histogram = new Pillar[buckets];
        for (int i = 0; i < buckets; i++) {
            Pillar pillar = new Pillar(x_values[i], x_values[i+1], 0);
            histogram[i] = pillar;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        assert v >= min && v <= max : "out of range";

        int index = whichBucket(v);
        histogram[index].add();
        totalCnt++;
    }


    /**
     * @param v
     * @return index of pillar, or -1 if out of bound
     */
    private int whichBucket(int v) {
        if (v < min || v > max)
            return -1;

        int index;

        // if all pillar are of equal width
        if (width2 == 0)
            index = (v - min) / width1;
        else
            if (v > valueMedian)
                index = (v-valueMedian) / width2 + (valueMedian-min) / width1;
            else
                index = (v - min) / width1;

        return index;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double selectivity = 0.0;
        int index = whichBucket(v);

        // if v < min || v > max
        if (index == -1) {
            switch (op) {
                case EQUALS:
                    return 0.0;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    if (v < min)
                        return 1.0;
                    else
                        return 0.0;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    if (v > max)
                        return 1.0;
                    else
                        return 0.0;
                case NOT_EQUALS:
                    return 1.0;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        switch (op) {
            case EQUALS:
                selectivity = getSelEq(index);
                break;
            case GREATER_THAN:
                selectivity = getSelGT(index, v);
                break;
            case LESS_THAN:
                selectivity = getSelLT(index, v);
                break;
            case NOT_EQUALS:
                selectivity = 1 - getSelEq(index);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = getSelEq(index) + getSelGT(index, v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = getSelEq(index) + getSelLT(index, v);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return selectivity;
    }

    private double getSelEq(int index) {
        assert index >=0 && index < histogram.length : "out of bound";
        Pillar pillar = histogram[index];
        int width = pillar.right - pillar.left;

        return pillar.cnt / (double)width / (double)totalCnt;
    }

    private double getSelGT(int index, int v) {
        double selectivity = 0;

        Pillar pillar = histogram[index];
        int width = pillar.right - pillar.left;
        int range = pillar.right - v;
        selectivity += pillar.cnt / (double)width * (double)range;

        for (int i = index + 1; i < histogram.length; i++)
            selectivity += histogram[i].cnt;

        return selectivity / (double)totalCnt;
    }


    private double getSelLT(int index, int v) {
        double selectivity = 0;

        Pillar pillar = histogram[index];
        int width = pillar.right - pillar.left;
        int range = v - pillar.left;
        selectivity += pillar.cnt / (double)width * (double)range;

        for (int i = 0; i < index; i++)
            selectivity += histogram[i].cnt;

        return selectivity / (double)totalCnt;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        int total = 0;
        for (Pillar pillar : histogram)
            total += pillar.cnt;

        return total / totalCnt;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String str = new String();

        for (Pillar pillar : histogram)
            str += pillar;
        return str;
    }
}
