package azkaban.jobtype.hiveutils.azkaban.hive.actions;

class AlterTableLocationQL implements HQL {
  // alter table zoip set location 'hdfs://eat1-magicnn01.grid.linkedin.com:9000/user/jhoman/b';
  private final String table;
  private final String newLocation;

  public AlterTableLocationQL(String table, String newLocation) {
    // @TODO: Null checks
    this.table = table;
    this.newLocation = newLocation;
  }

  @Override
  public String toHQL() {
    return "ALTER TABLE " + table + " SET LOCATION '" + newLocation + "';";
  }
}

