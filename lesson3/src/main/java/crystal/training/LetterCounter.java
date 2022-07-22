package crystal.training;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LetterCounter {
  static final String VOCALS = "aeiou";

  static String myText = "There also could occur errors during search or poison application Or the search could take a very long time because the ants hide very well";

  public static void main(String[] args) {
    System.out.println(Arrays.toString(myText.split("")));
    // TO DO - find 2 other ways to create a stream of chars from a string !!!!
    // ex myText.getChars();
    long vc = Arrays.stream(myText.split(""))
        .filter(l -> VOCALS.indexOf(l) >= 0)
        .count();
    System.out.println(vc);
    System.out.println(myText.length() - vc);
    System.out.println(Arrays.stream(myText.split(""))
        .filter(l -> VOCALS.indexOf(l) < 0)
        .count());
  }
}
