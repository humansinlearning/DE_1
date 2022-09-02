package crystal.training.introspection;

public class SecurityProxy {
  // Dynamic proxy decorator

  static class MyClass {
    @RoleAllowed("admin")
    void doSmething(int x1, String x2) {

    }
  }

  public static void main(String[] args) {
    MyClass myClass = xxxx;
    myClass.doSmething(); // should throw an exception if the current user has not admin role, oe whatever value in annotation

    // User authenticates -> Principal info (roles) are saved into a ThreadLocal variable
    // ProxyLogic
    // checkMethod annotation RoleAllowed through introspection - get the value from it
    // if no annotation or no value - anyone can execute the method
    // if value like 'admin' - CHECK THE THREADLOCAL Variable (roles) for the role name - if the role does not match throws RuntimeException

    // ThreadLocal roles = new ThreadLocal<String>();

    // roles.set("guest");
    // myClass.doSomething(1,"xxx"); - should throw exception

    // roles.set("admin");
    // myClass.doSomething(1,"xxx"); - should call the 'real' method
  }
}
