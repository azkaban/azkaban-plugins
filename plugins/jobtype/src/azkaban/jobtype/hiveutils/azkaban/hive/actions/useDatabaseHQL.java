package azkaban.jobtype.hiveutils.azkaban.hive.actions;

class UseDatabaseHQL implements HQL {
  private final String database;

  UseDatabaseHQL(String database) {
    this.database = database;
  }

  @Override
  public String toHQL() {
    return "USE " + database + ";";
  }
}

