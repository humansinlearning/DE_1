package crystal.beam;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class TestSerializable {
  // SOME NON SERIALIZABLE NOT TRANSIENT FIELDS

  public static void main(String[] args) throws IOException {
    MyClass myClass = new MyClass();
    ObjectOutputStream oos = new ObjectOutputStream(new ByteArrayOutputStream());
    oos.writeObject(myClass);
  }
}

class MyClass implements Serializable {
  int xxx;
}