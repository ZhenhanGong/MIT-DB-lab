package simpledb;

public class MyTest {
    public static void main(String[] args) {
        IntHistogram histogram = new IntHistogram(6, 8, 29);
        System.out.println(histogram);

        IntHistogram histogram2 = new IntHistogram(6, 8, 31);
        System.out.println(histogram2);

        IntHistogram histogram3 = new IntHistogram(10, 8, 15);
        System.out.println(histogram3);
    }
}
