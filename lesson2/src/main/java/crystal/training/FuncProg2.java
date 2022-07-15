package crystal.training;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FuncProg2 {
  static Integer[] values = new Integer[]{1, 2, 3, 4, 5};

  static Collection<Integer> printProcessedValues(Integer[] values, Function<Integer, Integer> converter) {
    return Arrays.stream(values)
        .map(v -> converter.apply(v))
        .filter(v -> v < 10)
        .collect(Collectors.toList());
    // 1 create result collection
    //  2 iterate over input collection (stream)
    // 3 for each element calculate and save result into result collection
    // 3.1 filter out on condition
    // 4 return result collection
  }

  public static void main(String[] args) {
    System.out.println(printProcessedValues(values, v -> v * 3));
    // TBD - filter also functional argument Function<Integer,Boolean>
    Arrays.stream(new Integer[] { 1,2}).map(v -> {
      System.out.println("VALUE IS " + v);
      return null;
    });
    // WHY System.out.println("VALUE IS " + v); not executed ??
  }
}
