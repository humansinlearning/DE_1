package crystal;

import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ApproximateStats {
  // stream all words from shakespeare operas - what are the most frequent 15 words
  // 30_000 words - 30_000 map size

  // I have a web server - I want to know how many distinct IP's are visiting the site - streaming

  // statistical approach - approximate result

  // overbooking flights .. -
  // Oversubscribing phone/data carriers
  //

  // Bloom filters -
  // Stream of items , for each item to check if item is first seen this time.

  // you can say that an item is present in a set approximately - no false negative,  a small rate of false positive

  public static void main(String[] args) {
    Stream.generate(new Supplier<Integer>() {
      @Override
      public Integer get() {
        return null;
      }
    }).distinct().collect(Collectors.toSet());

    // count min sketch
    // you get 1 billion addreses - you get messages from each of the a random number of times
    // [ a1, a2, ....., a1000_000_000]
    // [ +1, ,         , +1]
    // exercise - simulate they we randomly pick one item from 10000 distinct items, stream these events - 1000_000 times
    // want to know approximation of the counts per item ..  CountMinSketch
  }
}
