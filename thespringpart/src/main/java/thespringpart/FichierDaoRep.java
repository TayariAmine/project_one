package thespringpart;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Repository;

import lombok.extern.slf4j.Slf4j;
import thejavapart.*;

@Slf4j
@Repository
public class FichierDaoRep implements FichierDao {

	public List<String> showContent(String path) throws UnsupportedEncodingException, FileNotFoundException {

		List<String> result = new ArrayList<String>();
		if (path.contains(".tsv")) {
			result = (new thejavapart.FichierTsv("/home/tcb/"+path)).showDetails(5);

		} else if (path.contains(".csv")) {
			result = (new FichierCsv("/home/tcb/"+path)).showDetails(5);
		} else if (path.contains(".parquet")) {
			result = (new thejavapart.FichierParquet("/home/tcb/"+path)).showDetails(5);
		} else {
			result.add("can't show content");

		}
		return result;
	}

	public List<String> showMoreContent(String path) throws UnsupportedEncodingException, FileNotFoundException {
		List<String> result = new ArrayList<String>();
		if (path.contains(".tsv")) {
			result = (new thejavapart.FichierTsv("/home/tcb/"+path)).showDetails(15);

		} else if (path.contains(".csv")) {
			result = (new thejavapart.FichierCsv("/home/tcb/"+path)).showDetails(15);
		} else if (path.contains(".parquet")) {
			result = (new thejavapart.FichierParquet("/home/tcb/"+path)).showDetails(15);
		} else {
			result.add("can't show more content");

		}
		return result;
	}

	public String showStructure(String path) throws UnsupportedEncodingException, FileNotFoundException {
		String result;
		if (path.contains(".tsv")) {
			result = (new thejavapart.FichierTsv("/home/tcb/"+path)).showStructure();

		} else if (path.contains(".csv")) {
			result = (new thejavapart.FichierCsv("/home/tcb/"+path)).showStructure();
		} else if (path.contains(".parquet")) {
			result = (new thejavapart.FichierParquet("/home/tcb/"+path)).showStructure();
		} else {
			result="can't show more content";

		}
		return result;
	}

	public List<String> join(String path,int field_one, String second_file,int field_two) throws FileNotFoundException {
		
		List<String> result = new ArrayList<String>();
		if (path.contains(".tsv")) {
			result = (new thejavapart.FichierTsv("/home/tcb/"+path)).joinWith(field_one, second_file, field_two);

		} else if (path.contains(".csv")) {
			result = (new thejavapart.FichierCsv("/home/tcb/"+path)).joinWith(field_one, second_file, field_two);
		} else {
			result.add("couldn't join");

		}
		return result;
	}

	public List<String> filter(String path, String field_name, String value) {
		List<String> result = (new thejavapart.FichierParquet("/home/tcb/"+path)).filter(field_name, value);
		return result;
	}

	public String writeToHdfs(String path) throws IllegalArgumentException, IOException {
		String result;
		if (path.contains(".tsv")) {
			result = (new thejavapart.FichierTsv("/home/tcb/"+path)).copyToHDFS();

		} else if (path.contains(".csv")) {
			result = (new thejavapart.FichierCsv("/home/tcb/"+path)).copyToHDFS();
		} else if (path.contains(".parquet")) {
			result = (new thejavapart.FichierParquet("/home/tcb/"+path)).copyToHDFS();
		} else {
			result="copy to hdfs";

		}
		return result;
	}

}
