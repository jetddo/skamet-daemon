package kama.daemon.main.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class OsmTileCollector {

    // 내부 타일 서버 주소로 변경
    // 예:
    // http://10.10.10.10/tiles/{z}/{x}/{y}.png
    private static final String TILE_URL_TEMPLATE =
            "http://172.26.56.52/osm_tiles2/{z}/{x}/{y}.png";

    // 저장 경로
    private static final String OUTPUT_DIR = "C:/data/amo_tiles";

    // 한국 주변 범위
    private static final double MIN_LAT = 20.0;
    private static final double MAX_LAT = 50.0;
    private static final double MIN_LON = 110.0;
    private static final double MAX_LON = 150.0;

    // 줌레벨
    private static final int MIN_ZOOM = 7;
    private static final int MAX_ZOOM = 9;

    public static void main(String[] args) throws Exception {

        for (int z = MIN_ZOOM; z <= MAX_ZOOM; z++) {

            int xMin = lonToTileX(MIN_LON, z);
            int xMax = lonToTileX(MAX_LON, z);

            int yMin = latToTileY(MAX_LAT, z);
            int yMax = latToTileY(MIN_LAT, z);

            System.out.println(
                    "ZOOM " + z +
                    " x=" + xMin + "~" + xMax +
                    " y=" + yMin + "~" + yMax
            );

            for (int x = xMin; x <= xMax; x++) {
                for (int y = yMin; y <= yMax; y++) {

                    downloadTile(z, x, y);

                    // 너무 빠른 요청 방지
                    Thread.sleep(30);
                }
            }
        }

        System.out.println("DONE");
    }

    private static void downloadTile(int z, int x, int y) {

        InputStream in = null;
        FileOutputStream out = null;

        try {

            String urlStr = TILE_URL_TEMPLATE
                    .replace("{z}", String.valueOf(z))
                    .replace("{x}", String.valueOf(x))
                    .replace("{y}", String.valueOf(y));

            File dir = new File(
                    OUTPUT_DIR + File.separator + z + File.separator + x
            );

            if (!dir.exists()) {
                dir.mkdirs();
            }

            File file = new File(dir, y + ".png");

            // 이미 존재하면 skip
            if (file.exists() && file.length() > 0) {
                return;
            }

            URL url = new URL(urlStr);

            HttpURLConnection conn =
                    (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(10000);

            int code = conn.getResponseCode();

            if (code != 200) {
                System.out.println(
                        "FAIL z=" + z +
                        " x=" + x +
                        " y=" + y +
                        " code=" + code
                );
                return;
            }

            in = conn.getInputStream();
            out = new FileOutputStream(file);

            byte[] buffer = new byte[8192];
            int len;

            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }

            System.out.println(
                    "OK z=" + z +
                    " x=" + x +
                    " y=" + y
            );

        } catch (Exception e) {

            System.out.println(
                    "ERROR z=" + z +
                    " x=" + x +
                    " y=" + y +
                    " msg=" + e.getMessage()
            );

        } finally {

            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e) {
            }

            try {
                if (out != null) {
                    out.close();
                }
            } catch (Exception e) {
            }
        }
    }

    // 경도 -> 타일 X
    private static int lonToTileX(double lon, int zoom) {
        return (int) Math.floor(
                (lon + 180.0) / 360.0 * (1 << zoom)
        );
    }

    // 위도 -> 타일 Y
    private static int latToTileY(double lat, int zoom) {

        double latRad = Math.toRadians(lat);

        return (int) Math.floor(
                (
                        1.0 -
                        Math.log(
                                Math.tan(latRad) +
                                (1 / Math.cos(latRad))
                        ) / Math.PI
                ) / 2.0 * (1 << zoom)
        );
    }
}