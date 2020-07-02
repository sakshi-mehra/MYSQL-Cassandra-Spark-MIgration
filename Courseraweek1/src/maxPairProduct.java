import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class maxPairProduct {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        String s[] = br.readLine().split(" ");
        int[] list1 = new int[n];
        int j = 0;
        for (String a : s) {
            list1[j] = Integer.parseInt(a);
            j++;
        }
        long result = maxPairProduct.maxProd(list1);
        System.out.println(result);

    }

    static long maxProd(int[] s) {
        long max = 0;
        long secmax = 0;
        int maxind=-1;
        for (int a=0;a<s.length;a++) {

            if (max < s[a])
            {
                max = s[a];
                maxind=a;
            }

        }
        for (int a=0;a<s.length;a++) {

            if (secmax < s[a] && a!=maxind) {
                secmax = s[a];
            }
        }
        long val = max * secmax;
        return val;
    }
}
