package dataset_method

import org.apache.spark.sql.SparkSession

object DatasetMethod {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Hockey")
      .master("local[*]")
      .getOrCreate()

    val pathAwards = "resources\\AwardsPlayers.csv"
    val pathMaster = "resources\\Master.csv"
    val pathScoring = "resources\\Scoring.csv"
    val pathTeams = "resources\\Teams.csv"

    import spark.implicits._

    val awards = spark.read.option("header", "true")
      .csv(pathAwards)
      .select("playerID", "award")
      .as[Awards]

    val master = spark.read.option("header", "true").
      csv(pathMaster)
      .select("playerID", "firstName", "lastName")
      .as[Master]

    val teams = spark.read.option("header", "true")
      .csv(pathTeams)
      .select("tmID", "name")
      .as[Teams]

    val scoring = spark.read.option("header", "true")
      .csv(pathScoring)
      .select("playerID", "tmID", "stint")
      .as[Scoring]


    val plTeams = scoring.joinWith(teams, scoring.col("tmID") === teams.col("tmId"))
      .map(f => (f._1.playerID, f._2.name))
      .distinct().groupByKey(_._1)
      .reduceGroups((a, b) => (a._1, a._2 + ", " + b._2)).map(_._2)
      .withColumnRenamed("_1", "playerId").withColumnRenamed("_2", "teams")
      .as[PlayerTeams]

    val playerGoals = scoring.map(g => {
      var goals = g.stint.toInt
      (g.playerID, goals)
    })
      .groupByKey(_._1).reduceGroups((a, b) => (a._1, a._2 + b._2)).map(_._2)
      .withColumnRenamed("_1", "playerId").withColumnRenamed("_2", "goals")
      .as[PlayerGoals]


    val playerNameAwards = master.joinWith(awards, master.col("playerID") === awards.col("playerID"), "left")
      .map(d => {
        if (d._2 != null) {
          (d._1, 1)
        }
        else {
          (d._1, 0)
        }
      }).groupByKey(_._1.playerID).reduceGroups((a, b) => (a._1, a._2 + b._2)).map(_._2)
      .map(m => (m._1.playerID, m._1.firstName + " " + m._1.lastName, m._2))
      .withColumnRenamed("_1", "playerID")
      .withColumnRenamed("_2", "name")
      .withColumnRenamed("_3", "awards")
      .as[NameAwards]

    val pNameAwGoals = playerNameAwards
      .joinWith(playerGoals, playerGoals.col("playerID") === playerNameAwards.col("playerID"))
      .map(m => (m._1.playerId, m._1.name, m._1.awards, m._2.goals))
      .withColumnRenamed("_1", "playerID")
      .withColumnRenamed("_2", "name")
      .withColumnRenamed("_3", "awards")
      .withColumnRenamed("_4", "goals")
      .as[NameAwardsGoals]


    val allData =
      pNameAwGoals.joinWith(plTeams, plTeams.col("playerID") === pNameAwGoals.col("playerID"))
        .map(all => (all._1.name, all._1.awards, all._1.goals, all._2.teams))


    allData.orderBy(allData.col("_3").desc)
      .limit(10)
      .write.csv("results/dataset")
//      .write.format("csv").option("header", "true").save("results/dataset")

  }

  case class Scoring(playerID: String, tmID: String, stint: String)

  case class Teams(tmID: String, name: String)

  case class Master(playerID: String, firstName: String, lastName: String)

  case class Awards(playerID: String, award: String)

  case class PlayerTeams(playerId: String, teams: String)

  case class PlayerGoals(playerId: String, goals: Int)

  case class NameAwards(playerId: String, name: String, awards: Int)

  case class NameAwardsGoals(playerId: String, name: String, awards: Int, goals: Int)

}
