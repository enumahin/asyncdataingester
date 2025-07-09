package com.alienworkspace.cdr.asyndataingester.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public enum EncounterType {
    CLINICAL(Set.of(1, 6, 22, 32, 33, 34, 37, 9, 10, 16, 18, 27, 29, 35, 36, 44, 45,
            2, 14, 17, 19, 20, 25, 40, 41, 42, 43, 69, 3, 4, 5, 7, 8, 12, 15,
            21, 24, 26, 30, 38, 39)),
    PHARMACY(Set.of(13)),
    LAB(Set.of(11, 31, 51, 56, 59)),
    TB(Set.of(23, 46, 47, 48, 49, 50, 52, 53, 54, 55, 57, 58, 60, 61, 62, 63, 64, 65, 66, 67, 68)),
    TS(Set.of(28));

    private final Set<Integer> typeIds;

    private static final Map<Integer, EncounterType> lookup = new HashMap<>();

    static {
        for (EncounterType type : values()) {
            for (Integer id : type.typeIds) {
                lookup.put(id, type);
            }
        }
    }

    EncounterType(Set<Integer> typeIds) {
        this.typeIds = typeIds;
    }

    public static EncounterType fromValue(int typeId) {
        return lookup.getOrDefault(typeId, null);
    }
}
