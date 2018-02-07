package spark;

import java.io.Serializable;

public class MyPerson implements Serializable {

	private static final long serialVersionUID = 9043345830173637496L;

	private String name;
	private String age;
	private String birth;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getBirth() {
		return birth;
	}

	public void setBirth(String birth) {
		this.birth = birth;
	}

	@Override
	public String toString() {
		return "MyPerson [name=" + name + ", age=" + age + ", birth=" + birth + "]";
	}

}
