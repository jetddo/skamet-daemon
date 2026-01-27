package kama.daemon.main.test;

import java.awt.GraphicsEnvironment;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.Configuration;

import kama.daemon.common.db.DatabaseManager;

public class GetSystemFont {
	

	public static void main(String[] args) throws Exception {
		
		GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
		
		String[] fontFamilies = ge.getAvailableFontFamilyNames();
		
		for(String font : fontFamilies) {
			System.out.println("[" + font + "]");
		}
	}

}
