/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dataone.cn.batch.proto.packager.types;

/**
 *
 * @author waltz
 */
public enum DataPersistenceKeys {
    SKIP_IN_LOG_FIELD("skipInLogFile"),
    DATE_TIME_LAST_ACCESSED_FIELD("dateTimeLastAccessed"),
    SCIMETA("SCIMETA"),
    SYSMETA("SYSMETA");
        private final String value;

    private DataPersistenceKeys(String value) {
        this.value = value;
    }

    public String toString() {
        return value;
    }

    public static DataPersistenceKeys convert(String value) {
        for (DataPersistenceKeys inst : values()) {
            if (inst.toString().equals(value)) {
                return inst;
            }
        }
        return null;
    }
}
