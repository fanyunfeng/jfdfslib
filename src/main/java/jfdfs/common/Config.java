package jfdfs.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class Config {
    private Properties properties = null;

    public Config(String resource) {
        try {
            properties = loadProperties(resource);
        } catch (IOException e) {

        }
    }

    private Properties loadProperties(String resource) throws IOException {
        InputStream is = null;
        String[] v = resource.split(":");

        try {
            if (v.length == 1) {
                is = new FileInputStream(resource);
            } else {
                if (v.length == 2) {
                    String prefix = v[0].toLowerCase();

                    if (prefix.equals("classpath")) {
                        is = ClassLoader.getSystemResourceAsStream(v[1]);
                    } else {
                        URL url = new URL(resource);
                        is = url.openStream();
                    }
                }
            }

            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);

                return properties;
            }
        } finally {
            if (is != null) {
                is.close();
            }
        }

        return null;
    }

    public int getIntValue(String name, int def) {
        try {
            if (properties == null) {
                return def;
            }

            String value = properties.getProperty(name);
            if (value == null) {
                return def;
            }

            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    public String getStringValue(String name) {
        return getStringValue(name, null);
    }

    public String getStringValue(String name, String def) {
        if (properties == null) {
            return def;
        }

        String value = properties.getProperty(name);
        if (value == null) {
            return def;
        }

        return value;

    }

    public boolean getBooleanValue(String name, boolean def) {
        if (properties == null) {
            return def;
        }

        String value = properties.getProperty(name);
        if (value == null) {
            return def;
        }

        value = value.toLowerCase();

        if (value.equals("true") || value.equals("yes") || value.equals("on") || value.equals("1")) {
            return true;
        }

        return false;
    }
}
