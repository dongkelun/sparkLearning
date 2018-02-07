package spark;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

class WriteJson implements FlatMapFunction<Iterator<MyPerson>, String> {
	private static final long serialVersionUID = 6843756669734017366L;

	public Iterator<String> call(Iterator<MyPerson> people) throws Exception {
		ArrayList<String> text = new ArrayList<String>();
		ObjectMapper mapper = new ObjectMapper();
		while (people.hasNext()) {
			MyPerson person = people.next();
			text.add(mapper.writeValueAsString(person));
		}
		return text.iterator();
	}
}
