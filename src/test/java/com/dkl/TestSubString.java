package com.dkl;

public class TestSubString {

	public static void main(String[] args) {
		String name = "新疆维吾尔族自治区乌鲁木齐市";
		int index = name.indexOf("自治区1");
		System.out.println(index);
		System.out.println(name.substring(0,index+3));
		System.out.println(name.substring(index+3,name.length()));
		
		

	}

}
