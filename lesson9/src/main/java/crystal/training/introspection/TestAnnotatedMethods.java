package crystal.training.introspection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class TestAnnotatedMethods {
  public static void main(String[] args) {

    Class<AnnotatedMethods> annotatedMethodsClass = AnnotatedMethods.class;
    for (Method method : annotatedMethodsClass.getMethods()) {

      Annotation annotation = method.getAnnotation(Test.class);
      Test test = (Test) annotation;

      // If the annotation is not null
      if (test!=null) {

        try {
          method.invoke(annotatedMethodsClass
              .getDeclaredConstructor()
              .newInstance()); // new AnnotatedMethods()
        } catch (Throwable ex) {
          System.out.println(ex.getCause());
        }

      }
    }
  }
}
