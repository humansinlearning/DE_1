package crystal.training.introspection;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class IntrospectionTest {


  public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    MyClass myClass = new MyClass();
    myClass.setFld1("BUBUBU");

    Set<String> fieldNames = Arrays.stream(myClass.getClass().getDeclaredFields())
        .map(f -> f.getName()).collect(Collectors.toSet());
    System.out.println(fieldNames);
    // Exercise - GET ALL FIELDS from  A extends B (A has fields a1,a2, B has fields b1,b2) - use introspection to get superclass of a givem class
    Set<String> methodNames = Arrays.stream(myClass.getClass().getDeclaredMethods())
        .map(m -> m.getName()).collect(Collectors.toSet());
    System.out.println(methodNames);
    String fldName = "fld1";
    Field f1 = myClass.getClass().getDeclaredField(fldName);
    f1.setAccessible(true); // SecurityManager
    System.out.println(f1.get(myClass));

    Method method = myClass.getClass().getDeclaredMethod("myStaticMethod");
    method.setAccessible(true);
    method.invoke(null);

  }
}

class MyClass {
  private String fld1;
  protected int fld2;



  MyClass() {

  }

  int method1() {
    return fld2;
  }

  public void setFld1(String fld1) {
    this.fld1 = fld1;
  }

  private static void myStaticMethod() {
    System.out.println("Hello from static");
  }
}
