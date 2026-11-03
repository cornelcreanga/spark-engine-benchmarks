package dev.sparkbench.events;

public enum EventType {
    DNS_REQUEST("dnsrequest"),
    PROCESS_START("prstart"),
    PROCESS_END("prend"),
    FILE_CREATED("filecreated"),
    FILE_MODIFIED("filemodify"),
    FILE_RENAMED("filerename"),
    FILE_DELETED("filedelete");

    private final String tableValue;

    EventType(String tableValue) {
        this.tableValue = tableValue;
    }

    public String tableValue() {
        return tableValue;
    }
}
