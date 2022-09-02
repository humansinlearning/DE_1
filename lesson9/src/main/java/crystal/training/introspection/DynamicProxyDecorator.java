package crystal.training.introspection;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

public class DynamicProxyDecorator implements InvocationHandler {
  interface MyInterface {
    int method1();

    String method2(String arg);
  }

  static class MyClass implements MyInterface {
    private String value;

    public MyClass(String value) {
      this.value = value;
    }

    @Override
    public int method1() {
      return 22;
    }

    @Override
    public String method2(String arg) {
      // start time
      return arg + " result " + value;
      // end time
      // save the duration somewhere
    }
  }

  private final Map<String, Method> methods = new HashMap<>();

  private Object target;

  DynamicProxyDecorator(Object target) {
    this.target = target;
    for (Method method : target.getClass().getDeclaredMethods()) {
      this.methods.put(method.getName(), method);
    }
  }

  // CDI - context dependency injection LIKE spring!!
  // ORM - persistence frameworks
  // Security

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    System.out.println("Before calling method " + method.getName());
    Object result = methods.get(method.getName()).invoke(target, args);
    System.out.println("After calling method " + method.getName() + " - result " + result);
    return result;
  }

  public static void main(String[] args) {
    //DynamicProxyDecorator deco = new DynamicProxyDecorator(new String("Bubu"));
    MyInterface myClass = (MyInterface) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{MyInterface.class},
        new DynamicProxyDecorator(new MyClass("MyValue")));
    System.out.println(myClass.method2("m2"));
  }

  ///
  // Wait/Notify
  // Blocking Deques - offer/pull - N producers/M Consumers
  // Exchanger -1 Producer / 1 Consumer - fastest in this category ..

}
