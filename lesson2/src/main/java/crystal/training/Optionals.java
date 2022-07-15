package crystal.training;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.Optional;

public class Optionals {
  // String = String that is NEVER NULL
  // String? = STring that might be null

  static String greeting(String name) {
    return "Hello " + (name!=null ? name:" unknown");
  }

  static String greetingO(String name) {
    return "Hello " + Optional.ofNullable(name).orElse(" unknown");
  }

  static void greeting1(String name) {
    if (Objects.nonNull(name)) { // name != null
      System.out.println("Hello " + name);
    }
  }

  static void greeting1O(String name) {
    Optional.ofNullable(name).map(n -> {
      System.out.println("Hello " + n);
      return null;
    });
  }

  static Double addDoubles(Double d1, Double d2) {
    if (d1!=null && d2!=null) {
      return d1 + d2;
    } else {
      return null;
    }
  }

  static Optional<Double> addDoublesO(Double d1, Double d2) {
    return Optional.ofNullable(d1)
        .map(v1 -> Optional.ofNullable(d2).map(v2 -> v1 + v2)).get();
  }


  public static void main(String[] args) {
    //System.out.println(greeting(null));
    //System.out.println(greetingO(null));
    greeting1(null);
    greeting1("John");
    greeting1O(null);
    greeting1O("Maria");

    Double res = addDoubles(2.2, null);
    if (res!=null) {
      System.out.println(res);
    }

    // 1.1 = a*2^N + b*2^N-1 .... + d/2 + e/4 + .. ...
    addDoublesO(2.2, 1.1).map(v -> {
      System.out.println(v);
      return null;
    });
    System.out.println(new BigDecimal("9.8").add(new BigDecimal("2.2")));
    System.out.println(new BigDecimal(9.8).add(new BigDecimal("2.2")));
    System.out.println(new BigDecimal("9.8").add(new BigDecimal(2.2)));

  }
}
