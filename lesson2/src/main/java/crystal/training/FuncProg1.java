package crystal.training;

import java.util.Arrays;
import java.util.function.Function;

// SQL Functions - STABLE
public class FuncProg1 {
  static int[] values = new int[]{1, 2, 3, 4, 5};

  static void printValues(int[] values) {
    for (Integer v : values) {
      System.out.println(v);
    }
  }

  static void printValueCubes(int[] values) {
    for (Integer v : values) {
      System.out.println(v * v * v);
    }
  }

  static void printProcessedValues(int[] values, Function<Integer, Double> converter) {
    Arrays.stream(values)
        .forEach(v -> System.out.println(converter.apply(v)));
  }

  public static void main(String[] args) {
    printValueCubes(values);
    printValues(values);

    printProcessedValues(values, v -> v*v*v*1.0);
    printProcessedValues(values, v -> v/2.0);



  }
}
