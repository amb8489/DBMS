package common;

public class VerbosePrint {


    public static boolean Verbose = false;


    public static void print(Object obj){
        if (Verbose){System.out.print(obj);}
    }

    public static void main(String[] args) {

        VerbosePrint.print("hello");

    }


}
