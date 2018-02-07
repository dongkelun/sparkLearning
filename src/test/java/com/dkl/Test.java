package com.dkl;

import java.util.ArrayList;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String ss="ss¶­¿ÉÂ×213";
		System.out.println(ss.length());
		char[] ss_array = ss.toCharArray();
		System.out.println(ss_array.length);
		
		ArrayList arr = new ArrayList();
		
		arr.add(1);
		arr.add(2);
		arr.add(1);
		System.out.println(arr);

	}

}
