package kama.daemon.main.test;

import java.awt.image.BufferedImage;
import java.io.File;

import javax.imageio.ImageIO;

public class SubImageTest {

	public SubImageTest() throws Exception {
		
		int x1 = 2799; // 시작 x 좌표
		int y1 = 465; // 시작 y 좌표
		int x2 = 3369; // 끝 x 좌표
		int y2 = 737; // 끝 y 좌표
		
		int width = x2 - x1; // 자를 이미지의 너비
		int height = y2 - y1; // 자를 이미지의 높이
		
		BufferedImage gktgBaseImg = ImageIO.read(new File("F:/data/cdm_gktg_base2.png"));
		
		BufferedImage bi = ImageIO.read(new File("F:/data/amo_kimg_gktg_max_f00_2026030418_001.png"));
		
		bi = bi.getSubimage(x1, y1, width, height);
		
		gktgBaseImg.getGraphics().drawImage(bi, 0, 0, gktgBaseImg.getWidth(), gktgBaseImg.getHeight(), null);
		
		ImageIO.write(gktgBaseImg, "png", new File("F:/data/cdm_gktg_test.png"));
		
	}

	public static void main(String[] args) throws Exception {
		
		new SubImageTest();
	}

}
