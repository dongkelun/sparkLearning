package com.dkl;

public class TestSubString {

	public static void main(String[] args) {
		String name = "�½�ά�������������³ľ����";
		int index = name.indexOf("������1");
		System.out.println(index);
		System.out.println(name.substring(0,index+3));
		System.out.println(name.substring(index+3,name.length()));
		
		

	}

}
