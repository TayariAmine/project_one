package thejavapart;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class MainOne {

	public static void main(String[] args) throws IllegalArgumentException, IOException {

		FichierTsv test = new FichierTsv("/home/tcb/sample.csv");
       // test.showStructure();
        test.joinWith(0, "/home/tcb/sample1.csv",0);
	//	test.filter("trip_type","\""+"yellow"+"\"");
        //test.copyToHDFS();

	}

}
