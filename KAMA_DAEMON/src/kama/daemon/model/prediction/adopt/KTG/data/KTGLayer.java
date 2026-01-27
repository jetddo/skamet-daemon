package kama.daemon.model.prediction.adopt.KTG.data;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.FloatBuffer;

import javax.imageio.ImageIO;

public final class KTGLayer extends Layer2D implements Cloneable {

	private static final Color[] color_table = {
			new Color(255, 255, 255, 0),	// NIL
			new Color(0, 255, 0),			// LGT
			new Color(255, 204, 0),			// MOD
			new Color(255, 0, 0)			// SEV
	};

	public KTGLayer(final int width, final int height) {
		super(width, height);
	}

	public KTGLayer(final int width, final int height, final float[] buffer) {
		super(width, height, buffer);
	}

	public KTGLayer(final Layer2D layer2D) {
		super(layer2D);
	}

	public static KTGLayer fromBinary(final int width, final int height, final float[] buffer) {
        return new KTGLayer(width, height, buffer);
    }

	public void saveAs(final File file) throws IOException {
		final int imWidth = getSizeX();
		final int imHeight = getSizeY();

		final BufferedImage im = new BufferedImage(imWidth, imHeight, BufferedImage.TYPE_4BYTE_ABGR);

		for (int y = 0; y < imHeight; ++y) {
			for (int x = 0; x < imWidth; ++x) {
				Color color = getColor(getValue(x, y));
				im.setRGB(x, imHeight - y - 1, color.getRGB());
            }
        }

        ImageIO.write(im, "png", file);
    }
    
	public KTGLayer performRegriding(final FloatBuffer latitudeBuffer, final FloatBuffer longitudeBuffer) {
		/**
		 * KTG 격자 데이터를 위경도 좌표를 적용하여 직각좌표계로 변환하면 아래가 좁고 위가 넓은 부채꼴 형태의 이미지로 변환이 이루어지기 때문에
		 * 사각형 영역만 추출하기 위해서 최소 위경도 좌표를 구한다.
		 * 
		 * ※ KTG 격자 데이터에 대한 위경도 좌표는 고정되어 있으므로 아래의 루틴을 이용하여 최소/최대 위경도 좌표를 구한 값을 하드코딩한다.
		 */
		final float lonMinValue = 101.6161f;
		final float lonMaxValue = 150.3839f;
		final float latMinValue = 15.94464f;
		final float latMaxValue = 52.87738f;
/*
		// 추출된 이미지는 아래가 좁기 때문에 경도의 최소/최대값은 0번째 위도의 가장왼쪽/오른쪽 값으로 한다.
		float lonMinValue = longitudeBuffer.get(0);
		float lonMaxValue = longitudeBuffer.get(getSizeX() - 1);

		// 0번째 줄 격자 데이터의 위도 좌표중에서 가장 큰 값을 최소 위도값으로 한다.
		float latMinValue = -999;
		for (int x = 0; x < getSizeX(); ++x) {
			float val = latitudeBuffer.get(x);
			if (latMinValue < val) {
				latMinValue = val;
			}
		}

		// 마지막 줄 격자 데이터의 위도 좌표중에서 가장 작은 값을 최대 위도값으로 한다.
		float latMaxValue = 999;
		for (int x = 0; x < getSizeX(); ++x) {
			float val = latitudeBuffer.get(x + ((getSizeY() - 1) * getSizeX()));
			if (latMaxValue > val) {
				latMaxValue = val;
			}
		}

		System.out.println("# 경도 최소값:" + lonMinValue + ", 경도 최대값:" + lonMaxValue);
		System.out.println("# 위도 최소값:" + latMinValue + ", 위도 최대값:" + latMaxValue);
 */

		/**
		 * 새로 구해진 최소/최대 위경도 좌표에 해당하는 격자 개수를 구한다.
		 */
/*
		float minLat = 999, maxLat = -999;
		float minLon = 999, maxLon = -999;
		for (int y = 0; y < getSizeY(); ++y) {
			for (int x = 0; x < getSizeX(); ++x) {
				float lat = latitudeBuffer.get(x + (y * getSizeX()));
				float lon = longitudeBuffer.get(x + (y * getSizeX()));

				if (minLat > lat) {
					minLat = lat;
				}
				if (maxLat < lat) {
					maxLat = lat;
				}
				if (minLon > lon) {
					minLon = lon;
				}
				if (maxLon < lon) {
					maxLon = lon;
				}
			}
		}

		final float lonValuePerGridX = (maxLon - minLon) / getSizeX();
		final float latValuePerGridY = (maxLat - minLat) / getSizeY();
		final int newLayerWidth = Math.round((lonMaxValue - lonMinValue) / lonValuePerGridX);
		final int newLayerHeight = Math.round((latMaxValue - latMinValue) / latValuePerGridY);

		System.out.println("## 격자개수 X:" + newLayerWidth + ", Y:" + newLayerHeight);
 */
		final int newLayerWidth = 251;
		final int newLayerHeight = 316;

		// 새로운 레이어를 생성하고 값을 초기화한다.
		KTGLayer newLayer = new KTGLayer(newLayerWidth, newLayerHeight);
		for (int y = 0; y < newLayer.getSizeY(); ++y) {
			for (int x = 0; x < newLayer.getSizeX(); ++x) {
				newLayer.setValue(x, y, -1);
			}
		}

		// 새로운 레이어의 픽셀당 간격을 구한다.
		final float lonIntervalPerPixel = (lonMaxValue - lonMinValue) / (newLayer.getSizeX() - 1);
		final float latIntervalPerPixel = (latMaxValue - latMinValue) / (newLayer.getSizeY() - 1);

		// 새로운 레이어에 KTG 데이터를 입힌다.
		for (int y = 0; y < getSizeY(); ++y) {
			for (int x = 0; x < getSizeX(); ++x) {
				final double latValue = latitudeBuffer.get(x + (y * getSizeX()));
				final double lonValue = longitudeBuffer.get(x + (y * getSizeX()));
				if (latValue < latMinValue || latValue > latMaxValue || lonValue < lonMinValue || lonValue > lonMaxValue) {
					continue;
				}

				int x2 = (int) ((lonValue - lonMinValue) / lonIntervalPerPixel);
				int y2 = (int) ((latValue - latMinValue) / latIntervalPerPixel);

				if (x2 >= 0 && x2 < newLayer.getSizeX() && y2 >= 0 && y2 < newLayer.getSizeY()) {
					newLayer.setValue(x2, y2, getValue(x, y));
				}
			}
		}

		// 값이 비어있는 픽셀이 존재하는 경우 주위값의 최저값으로 변경한다.
		for (int y = 1; y < (newLayer.getSizeY() - 1); ++y) {
			for (int x = 1; x < (newLayer.getSizeX() - 1); ++x) {
				float value = newLayer.getValue(x, y);

				if (value < 0) {
					final float f1 = newLayer.getValue(x - 1, y - 1);
					final float f2 = newLayer.getValue(x, y - 1);
					final float f3 = newLayer.getValue(x + 1, y - 1);
					final float f4 = newLayer.getValue(x - 1, y);
					final float f5 = newLayer.getValue(x + 1, y);
					final float f6 = newLayer.getValue(x + 1, y - 1);
					final float f7 = newLayer.getValue(x + 1, y);
					final float f8 = newLayer.getValue(x + 1, y + 1);

					newLayer.setValue(x, y,
							Math.min(Math.min(Math.min(f1, f2), Math.min(f3, f4)), 
									 Math.min(Math.min(f5, f6), Math.min(f7, f8))));
				}
			}
		}

        return newLayer;
    }

	private Color getColor(final float value) {
		if (value < 1.e+032f/* Missing Value */) {
			if (value >= 0.3 && value < 0.475) {			// LGT
				return color_table[1];
			} else if (value >= 0.475 && value < 0.75) {	// MOD
				return color_table[2];
			} else if (value >= 0.75) {						// SEV
				return color_table[3];
			}
		}

		return new Color(0, 0, 0, 0);
    }

	public KTGLayer clone() {
		return new KTGLayer(super.clone());
	}

}
