package kz.talgat.extensions.databases;

public class StaticVariables {
    public static Long rowsCount = 0L;
    public static int groupsCount = 0;

    public static void setRowsCount(Long rowsCount) {
        StaticVariables.rowsCount = rowsCount;
    }

    public static Long getRowsCount() {
        return rowsCount;
    }

    public static int getGroupsCount() {
        return groupsCount;
    }

    public static void setGroupsCount(int groupsCount) {
        StaticVariables.groupsCount = groupsCount;
    }

    public synchronized static void incRowsCount(Long rowsCount) {
        StaticVariables.rowsCount += rowsCount;
    }

    public synchronized static void incGroupCount(int groupCount) {
        StaticVariables.groupsCount += groupCount;
    }
}
