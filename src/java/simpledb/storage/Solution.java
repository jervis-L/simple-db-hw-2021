package simpledb.storage;

import java.util.*;

/**
 * @author jervisliao
 * @create 2022-08-15 23:31
 */

class Solution {
    public int[][] generateMatrix(int n) {
        int[][] res=new int[n][n];
        int x=1;
        int i=0,j=0;
        while(x<=n*n){
            System.out.println("i: "+i+" j: "+j);
            while(x<=n*n&&j<n-i-1){
                System.out.println("1: "+x);
                res[i][j++]=x++;
            }
            while(x<=n*n&&i<j){
                System.out.println("2: "+x);
                res[i++][j]=x++;
            }
            while(x<=n*n&&j>n-i-1){
                System.out.println("3: "+x);
                res[i][j--]=x++;
            }
            while(x<=n*n&&i>j){
                System.out.println("4: "+x);
                res[i--][j]=x++;
            }
            i++;
            j++;
        }
        return res;
    }
    public static void main(String[] args){
        new Solution().generateMatrix(4);
    }
}