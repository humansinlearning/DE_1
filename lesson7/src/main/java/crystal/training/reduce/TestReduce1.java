package crystal.training.reduce;

import java.util.Arrays;
import java.util.stream.Stream;

public class TestReduce1 {
  public static void main(String[] args) {
    Integer[] values = new Integer[]{1, 2, 3};
    Integer reduceResult = Arrays.stream(values)
        .reduce(
            0,
            (a, b) -> a + b);
    System.out.println("reduce:" + reduceResult);

    reduceResult = Arrays.stream(values)
        .reduce(
            0,
            (a, b) -> a + b*b);
    System.out.println("reduce:" + reduceResult);

    reduceResult = Arrays.stream(values)
        .reduce(
            Integer.MIN_VALUE,
            (a, b) -> a > b ? a : b);
    System.out.println("reduce:" + reduceResult);

    reduceResult = Arrays.stream(values)
        .reduce(
            Integer.MAX_VALUE,
            (a, b) -> a < b ? a : b);
    System.out.println("reduce:" + reduceResult);

    //  Accumulator = 0 FOR addition
    //  Accumulator = 1 FOR MULTIPLICATION
    // Accumulator = MIN VALUE OF TYPE  -  for MAXIMUM OF TYPE
    // X $ Y = Y
    // MAX(SMALLEST DOMAIN VALUE , X) = X
    // MIN(LARGEST DOMAIN VALUE , X) = X
    // SETS
    // INTERSECTION as an operator (commutative and associative) - ACC=THE FULL ELEMENT SET  - INTERSECTION(FULL SET, X) = X


    // groupBy -> reduce to Map<String, String[]>
    // Hint - ACC ?? = MAP<>

    // (A $ B) $ C $ D = A $ (B $ C $ D) - (X)  A $ B, (Y) C $ D -> X $ Y
    // PARALLEL STREAMING - A,B,C,D strean -
    //  -> A,B -> A $ B = X
    //  -> C,D -> C $ D = Y
    // -----> X $ Y = final result

    System.out.println(Arrays.stream(values)
        .reduce(
          1,
            (acc, value ) -> acc * value,
            (acc1, acc2) -> acc1 * acc2 // acc1, acc2, acc3, acc4 ,... -> (((acc1 + acc2) + acc3) + acc4)
        ));

    System.out.println(Arrays.stream(values).mapToInt(i -> i).summaryStatistics());
  }
}
