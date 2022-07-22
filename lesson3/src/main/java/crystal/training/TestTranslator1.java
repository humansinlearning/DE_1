package crystal.training;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// APPLET ???
public class TestTranslator1 extends Object {
  static Map<String, String> dictionary = new HashMap<>();

  static {
    dictionary.put("hello", "hi"); // Hello HELLO heLLO
    dictionary.put("bye", "bye bye");
  }

  static String myText = "Hello everyone how is going Bye"; // -> hi everyone  how is going ? bye bye

  int test;

  {
    test = 1;
  }

  {
    test = 2;
  }

  public TestTranslator1(int t) {
    super();
    /*
    super();
     {
    test = 1;
  }

  {
    test = 2;
  }
     */
    //test = t;
    System.out.println("Here");
  }

  // SystemClassLoader, ThreadClassLoader

  // parent classloader -> child classloader war


  public static void main(String[] args) {
    TestTranslator1 tr = new TestTranslator1(20);
    System.out.println(tr.test);
    System.out.println(Thread.currentThread().getContextClassLoader());
    System.out.println(Arrays.toString(myText.split(" ")));
    String result = Arrays.stream(myText.split(" ")) // Stream<String>
        .map(w -> w.toLowerCase()) // Stream<String> - lower case
        .map(w -> Optional.ofNullable(dictionary.get(w)).orElseGet(() -> w)) //
        /**
        .map(w -> {
          String trs = dictionary.get(w);
          if(trs != null) {
            return trs;
          } else {
            return w;
          }
        })
         */
        /*

         */

        .collect(Collectors.joining(" "))
        ;
    System.out.println(result);
    // Stream builders -
    // Transformation/filters - map, flatMap, filter
    // Terminal operations - collection , foreach, reduce, first,
  }
}
