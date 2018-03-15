package thejavapart;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class MainOne {

	public static void main(String[] args) throws IllegalArgumentException, IOException {

		FichierParquet test = new FichierParquet("/home/tcb/sample.parquet");
       System.out.println(test.showStructure());
       System.out.println(test.showDetails(7));
     // System.out.println(test.joinWith(0, "/home/tcb/sample1.tsv",0));
	   System.out.println(test.filter("trip_type","\""+"yellow"+"\""));
        //test.copyToHDFS();

	}

}
