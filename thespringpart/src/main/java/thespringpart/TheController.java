package thespringpart;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@CrossOrigin(origins = "http://localhost:4200")
@RestController
public class TheController {

	@Autowired
	private FichierDaoRep fichier_dao_rep;

	@RequestMapping("")
	public String all() {

		return "ok";
	}

	@RequestMapping(value = "/showcontent", method = RequestMethod.GET)

	@ResponseBody
	//@PreAuthorize("hasAuthority('ADMIN_USER')")
	public List<String> show(@RequestParam(value = "path", required = false) String path)
			throws UnsupportedEncodingException, FileNotFoundException {

		return fichier_dao_rep.showContent(path);

	}

	@RequestMapping(value = "/showmorecontent", method = RequestMethod.GET)
	@PreAuthorize("hasAuthority('ADMIN_USER')")
	@ResponseBody
	public List<String> showMore(@RequestParam(value = "path", required = false) String path)
			throws UnsupportedEncodingException, FileNotFoundException {

		return fichier_dao_rep.showMoreContent(path);

	}

	@RequestMapping(value = "/showstructure", method = RequestMethod.GET)
	@PreAuthorize("hasAuthority('ADMIN_USER') or hasAuthority('STANDARD_USER')")
	@ResponseBody
	public List<String> showStucture(@RequestParam(value = "path", required = false) String path)
			throws UnsupportedEncodingException, FileNotFoundException {
		List<String> result = new ArrayList<String>();
		result.add(fichier_dao_rep.showStructure(path));
		return result;

	}

	@RequestMapping(value = "/join", method = RequestMethod.GET)
	@ResponseBody
	public List<String> join(@RequestParam(value = "path", required = false) String path,
			@RequestParam(value = "field_one", required = false) String field_one,
			@RequestParam(value = "second_file", required = false) String second_file,
			@RequestParam(value = "field_two", required = false) String field_two)
			throws UnsupportedEncodingException, FileNotFoundException {

		return fichier_dao_rep.join(path, field_one, second_file, field_two);

	}

	@RequestMapping(value = "/filter", method = RequestMethod.GET)
	@ResponseBody
	public List<String> filter(@RequestParam(value = "path", required = false) String path, 
			@RequestParam(value = "field_name" )String field_name,
			@RequestParam(value = "value")String value) throws UnsupportedEncodingException, FileNotFoundException {

		return fichier_dao_rep.filter(path, field_name, value);

	}

	@RequestMapping(value = "/write", method = RequestMethod.GET)
	@ResponseBody
	public List<String> copy(@RequestParam(value = "path", required = false) String path)
			throws IllegalArgumentException, IOException {

		return fichier_dao_rep.writeToHdfs(path);

	}
}
