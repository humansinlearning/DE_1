package crystal.training;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;
import java.util.stream.IntStream;

public class Rsa {
  @Builder
  @ToString
  @AllArgsConstructor
  public static class PrivateKey {
    private final BigInteger d;
    private final BigInteger n;

    public BigInteger decrypt(BigInteger message) {
      return message.modPow(d, n);
    }
  }

  @Builder
  @ToString
  @AllArgsConstructor
  public static class PublicKey {
    private final BigInteger e;
    private final BigInteger n;

    public BigInteger encrypt(BigInteger message) {
      return message.modPow(e, n);
    }
  }

  @Builder
  @ToString
  @Getter
  public static class KeyPair {
    PrivateKey privateKey;
    PublicKey publicKey;
  }

  private KeyPair keyPair;

  public Rsa(int bitlen) {
    SecureRandom r = new SecureRandom();
    BigInteger p = new BigInteger(bitlen / 2, 100, r);
    BigInteger q = new BigInteger(bitlen / 2, 100, r);
    BigInteger n = p.multiply(q);
    BigInteger m = (p.subtract(BigInteger.ONE))
        .multiply(q.subtract(BigInteger.ONE));
    BigInteger e = new BigInteger("3");
    while (m.gcd(e).intValue() > 1) {
      e = e.add(new BigInteger("2"));
    }
    BigInteger d = e.modInverse(m);
    keyPair = KeyPair.builder()
        .privateKey(PrivateKey.builder()
            .d(d)
            .n(n)
            .build())
        .publicKey(PublicKey.builder()
            .e(e)
            .n(n)
            .build())
        .build();
  }

  public PublicKey getPublicKey() {
    return keyPair.getPublicKey();
  }

  public PrivateKey getPrivateKey() {
    return keyPair.getPrivateKey();
  }

  public static void main(String[] args) {
    Rsa rsa = new Rsa(40);
    PrivateKey privateKey = rsa.getPrivateKey();
    PublicKey publicKey = rsa.getPublicKey();
    IntStream.range(1,10).forEach(i -> {
      BigInteger msg = BigInteger.valueOf(new Random().nextInt(1000));
      System.out.println("Orig msg:" + msg);
      BigInteger encryptedMsg = publicKey.encrypt(msg);
      System.out.println("Encrypted msg:" + encryptedMsg);
      System.out.println("Decrypted msg:" + privateKey.decrypt(encryptedMsg));
      System.out.println();
    });
  }

}
