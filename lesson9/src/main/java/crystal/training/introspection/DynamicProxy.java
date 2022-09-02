package crystal.training.introspection;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class DynamicProxy {
  public interface GetMyNumber {
    default int getNumber() {
      return -1;
    }
  }

  public static void main(String[] args) {
    GetMyNumber gmn = (GetMyNumber) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
        new Class[]{GetMyNumber.class},
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            System.out.println(method.getName() + " CALLED");
            return (int)(System.currentTimeMillis() % 1000);
          }
        });
    System.out.println(gmn.getNumber());
  }
}
