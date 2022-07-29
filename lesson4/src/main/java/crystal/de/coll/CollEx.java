package crystal.de.coll;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CollEx {
  static final Map<String, String> MY_STATIC_MAP = new HashMap<String, String>() {{
    put("X", "Y");
  }};


  @Data
  @Builder
  static class Person extends Object implements Comparable<Person> {
    String name;
    int age;

    public Person(String name, int age) {
      this.name = name;
      this.age = age;
    }


    @Override
    public int compareTo(Person o) {
      return name.compareTo(o.name);
    }


  }

  public static void main(String[] args) {
    Set<Person> myset = new HashSet<>(); // TreeSet...
    // p1 = new Person();
    // p1.setName(xx)
    // p2.setAge(11)
    Person p1 = new Person("John", 22) {
      {
        age = 55;
      }
    };
    Person p2 = new Person("John1", 22);
    System.out.println("Equals:" + p1.equals(p2));
    System.out.println("Hash1:" + p1.hashCode() + " Hash2:" + p2.hashCode());
    myset.add(p1);
    myset.add(p2);

    System.out.println("Size:" + myset.size());

    List<Person> myList = new ArrayList<>(); // LinkedList...
    myList.add(p1);
    myList.add(0, p2);
    System.out.println("List:" + myList.toString());

    // Implement push/pop using a list - offer/take for Queues using a list
    // guava

    Map<String, Person> myMap = new HashMap<>();
    // It's a set of (Key,Value) where equals and hashCode refer to Key ONLY
    // HashSet<X> - HashMap<X,X>

    Collection<String> myColl = new HashSet<>();
    //myColl.forEach();
    //myColl.stream()
    //myColl.size()
    //myColl.add()
    //myColl.remove()
    //myColl.contains()
    //myColl.isEmpty()
    //myColl.clear();
    //myColl.removeAll()
    for (String el : myColl) {
      //
    }

    for (Map.Entry<String, Person> e : myMap.entrySet()) {
      e.getValue();
      e.getKey();
    }
    //myMap.put()
    //myMap.get()
    //myMap.remove()

    //myMap.clear();
    //myMap.size()

    List<Integer> l1 = new ArrayList<Integer>() {
      {
        add(1);
        add(2);
        add(3);
      }
    };
    // l1 = new ArrayList<>();

    // l1.add(1);

    // l1.add(2);

    List<Integer> l2 = new ArrayList<Integer>() {{
      add(5);
      add(10);
      add(15);
    }};
    System.out.println("L1:" + l1);
    System.out.println("L2:" + l2);
    //l1.removeAll(l2);
    //System.out.println("L11:" + l1);

    // ZIPPING TWO LISTS
    List<IntPair.IntPairBuilder> zippedList = IntStream.range(0, Math.min(l1.size(), l2.size()))
        .mapToObj(i -> IntPair.builder().p1(l1.get(i)).p2(l2.get(i)))
        .collect(Collectors.toList());
    System.out.println(zippedList);
  }

  @Builder
  @AllArgsConstructor
  @ToString
  static class IntPair {
    int p1;
    int p2;
  }
}
