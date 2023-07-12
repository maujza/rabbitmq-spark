package com.github.maujza.schema;

import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashMap;

public class SerializableCaseInsensitiveStringMap implements java.io.Serializable {
    private HashMap<String, String> map;
    private transient CaseInsensitiveStringMap caseInsensitiveStringMap;

    public SerializableCaseInsensitiveStringMap(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        this.map = new HashMap<>();
        for (String key : caseInsensitiveStringMap.keySet()) {
            map.put(key.toLowerCase(), caseInsensitiveStringMap.get(key));
        }
    }

    public String get(String key) {
        return map.get(key.toLowerCase());
    }
}
