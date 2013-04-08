package azkaban.jobtype.hiveutils.azkaban.hive.actions;

class DropPartitionHQL implements HQL {
  private final String table;
  private final String partition;
  private final String value;
  private final boolean ifExists;

  DropPartitionHQL(String table, String partition, String value, boolean ifExists) {
    // @TODO: Null checks
    this.table = table;
    this.partition = partition;
    this.value = value;
    this.ifExists = ifExists;
  }


  @Override
  public String toHQL() {
    String exists = ifExists ? "IF EXISTS " : "";

    return "ALTER TABLE " + table + " DROP " + exists + "PARTITION (" + partition + "='" + value + "');";
  }
}

