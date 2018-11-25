package org.apache.beam.examples.complete.game.injector;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

class Injector {
  private static Pubsub pubsub;
  private static Random random = new Random();
  private static String topic;
  private static String project;
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

  private static final int MIN_QPS = 800;
  private static final int QPS_RANGE = 200;
  private static final int THREAD_SLEEP_MS = 500;

  private static final ArrayList<String> COLORS =
      new ArrayList<String>(Arrays.asList(
         "Magenta", "AliceBlue", "Almond", "Amaranth", "Amber",
         "Amethyst", "AndroidGreen", "AntiqueBrass", "Fuchsia", "Ruby", "AppleGreen",
         "Apricot", "Aqua", "ArmyGreen", "Asparagus", "Auburn", "Azure", "Banana",
         "Beige", "Bisque", "BarnRed", "BattleshipGrey"));

  private static final ArrayList<String> ANIMALS =
      new ArrayList<String>(Arrays.asList(
         "Echidna", "Koala", "Wombat", "Marmot", "Quokka", "Kangaroo", "Dingo", "Numbat", "Emu",
         "Wallaby", "CaneToad", "Bilby", "Possum", "Cassowary", "Kookaburra", "Platypus",
         "Bandicoot", "Cockatoo", "Antechinus"));

  private static ArrayList<TeamInfo> liveTeams = new ArrayList<TeamInfo>();

  private static DateTimeFormatter fmt =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));


  private static final int NUM_ROBOTS = 20;
  private static final int ROBOT_PROBABILITY = 3;
  private static final int NUM_LIVE_TEAMS = 15;
  private static final int BASE_MEMBERS_PER_TEAM = 5;
  private static final int MEMBERS_PER_TEAM = 15;
  private static final int MAX_SCORE = 20;
  private static final int LATE_DATA_RATE = 5 * 60 * 2;       
  private static final int BASE_DELAY_IN_MILLIS = 5 * 60 * 1000;  
  private static final int FUZZY_DELAY_IN_MILLIS = 5 * 60 * 1000;

  private static final int BASE_TEAM_EXPIRATION_TIME_IN_MINS = 20;
  private static final int TEAM_EXPIRATION_TIME_IN_MINS = 20;

  private static class TeamInfo {
    String teamName;
    long startTimeInMillis;
    int expirationPeriod;
    String robot;
    int numMembers;

    private TeamInfo(String teamName, long startTimeInMillis, String robot) {
      this.teamName = teamName;
      this.startTimeInMillis = startTimeInMillis;
      this.expirationPeriod = random.nextInt(TEAM_EXPIRATION_TIME_IN_MINS)
        + BASE_TEAM_EXPIRATION_TIME_IN_MINS;
      this.robot = robot;
      numMembers = random.nextInt(MEMBERS_PER_TEAM) + BASE_MEMBERS_PER_TEAM;
    }

    String getTeamName() {
      return teamName;
    }
    String getRobot() {
      return robot;
    }

    long getStartTimeInMillis() {
      return startTimeInMillis;
    }
    long getEndTimeInMillis() {
      return startTimeInMillis + (expirationPeriod * 60L * 1000L);
    }
    String getRandomUser() {
      int userNum = random.nextInt(numMembers);
      return "user" + userNum + "_" + teamName;
    }

    int numMembers() {
      return numMembers;
    }

    @Override
    public String toString() {
      return "(" + teamName + ", num members: " + numMembers() + ", starting at: "
        + startTimeInMillis + ", expires in: " + expirationPeriod + ", robot: " + robot + ")";
    }
  }

  private static String randomElement(ArrayList<String> list) {
    int index = random.nextInt(list.size());
    return list.get(index);
  }

  private static TeamInfo randomTeam(ArrayList<TeamInfo> list) {
    int index = random.nextInt(list.size());
    TeamInfo team = list.get(index);
    long currTime = System.currentTimeMillis();
    if ((team.getEndTimeInMillis() < currTime) || team.numMembers() == 0) {
      System.out.println("\nteam " + team + " is too old; replacing.");
      System.out.println("start time: " + team.getStartTimeInMillis()
        + ", end time: " + team.getEndTimeInMillis()
        + ", current time:" + currTime);
      removeTeam(index);
      
      return (addLiveTeam());
    } else {
      return team;
    }
  }

  
  private static synchronized TeamInfo addLiveTeam() {
    String teamName = randomElement(COLORS) + randomElement(ANIMALS);
    String robot = null;
    
    if (random.nextInt(ROBOT_PROBABILITY) == 0) {
      robot = "Robot-" + random.nextInt(NUM_ROBOTS);
    }
    
    TeamInfo newTeam = new TeamInfo(teamName, System.currentTimeMillis(), robot);
    liveTeams.add(newTeam);
    System.out.println("[+" + newTeam + "]");
    return newTeam;
  }

  
  private static synchronized void removeTeam(int teamIndex) {
    TeamInfo removedTeam = liveTeams.remove(teamIndex);
    System.out.println("[-" + removedTeam + "]");
  }

  /** Generate a user gaming event. */
  private static String generateEvent(Long currTime, int delayInMillis) {
    TeamInfo team = randomTeam(liveTeams);
    String teamName = team.getTeamName();
    String user;
    final int parseErrorRate = 900000;

    String robot = team.getRobot();
    
    if (robot != null) {
      
      
      
      if (random.nextInt(team.numMembers() / 2) == 0) {
        user = robot;
      } else {
        user = team.getRandomUser();
      }
    } else { 
      user = team.getRandomUser();
    }
    String event = user + "," + teamName + "," + random.nextInt(MAX_SCORE);
    
    if (random.nextInt(parseErrorRate) == 0) {
      System.out.println("Introducing a parse error.");
      event = "THIS LINE REPRESENTS CORRUPT DATA AND WILL CAUSE A PARSE ERROR";
    }
    return addTimeInfoToEvent(event, currTime, delayInMillis);
  }

  
  private static String addTimeInfoToEvent(String message, Long currTime, int delayInMillis) {
    String eventTimeString =
        Long.toString((currTime - delayInMillis) / 1000 * 1000);
    
    String dateString = fmt.print(currTime);
    message = message + "," + eventTimeString + "," + dateString;
    return message;
  }

  
   */
  public static void publishData(int numMessages, int delayInMillis)
      throws IOException {
    List<PubsubMessage> pubsubMessages = new ArrayList<>();

    for (int i = 0; i < Math.max(1, numMessages); i++) {
      Long currTime = System.currentTimeMillis();
      String message = generateEvent(currTime, delayInMillis);
      PubsubMessage pubsubMessage = new PubsubMessage()
              .encodeData(message.getBytes("UTF-8"));
      pubsubMessage.setAttributes(
          ImmutableMap.of(TIMESTAMP_ATTRIBUTE,
              Long.toString((currTime - delayInMillis) / 1000 * 1000)));
      if (delayInMillis != 0) {
        System.out.println(pubsubMessage.getAttributes());
        System.out.println("late data for: " + message);
      }
      pubsubMessages.add(pubsubMessage);
    }

    PublishRequest publishRequest = new PublishRequest();
    publishRequest.setMessages(pubsubMessages);
    pubsub.projects().topics().publish(topic, publishRequest).execute();
  }

  
  public static void publishDataToFile(String fileName, int numMessages, int delayInMillis)
      throws IOException {
    PrintWriter out = new PrintWriter(new OutputStreamWriter(
        new BufferedOutputStream(new FileOutputStream(fileName, true)), "UTF-8"));

    try {
      for (int i = 0; i < Math.max(1, numMessages); i++) {
        Long currTime = System.currentTimeMillis();
        String message = generateEvent(currTime, delayInMillis);
        out.println(message);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (out != null) {
        out.flush();
        out.close();
      }
    }
  }


  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 3) {
      System.out.println("Usage: Injector project-name (topic-name|none) (filename|none)");
      System.exit(1);
    }
    boolean writeToFile = false;
    boolean writeToPubsub = true;
    project = args[0];
    String topicName = args[1];
    String fileName = args[2];
    
    
    if (topicName.equalsIgnoreCase("none")) {
      writeToFile = true;
      writeToPubsub = false;
    }
    if (writeToPubsub) {
      
      pubsub = InjectorUtils.getClient();
      
      topic = InjectorUtils.getFullyQualifiedTopicName(project, topicName);
      InjectorUtils.createTopic(pubsub, topic);
      System.out.println("Injecting to topic: " + topic);
    } else {
      if (fileName.equalsIgnoreCase("none")) {
        System.out.println("Filename not specified.");
        System.exit(1);
      }
      System.out.println("Writing to file: " + fileName);
    }
    System.out.println("Starting Injector");

    
    while (liveTeams.size() < NUM_LIVE_TEAMS) {
      addLiveTeam();
    }

    
    for (int i = 0; true; i++) {
      if (Thread.activeCount() > 10) {
        System.err.println("I'm falling behind!");
      }

      
      final int numMessages;
      final int delayInMillis;
      if (i % LATE_DATA_RATE == 0) {
        
        delayInMillis = BASE_DELAY_IN_MILLIS + random.nextInt(FUZZY_DELAY_IN_MILLIS);
        numMessages = 1;
        System.out.println("DELAY(" + delayInMillis + ", " + numMessages + ")");
      } else {
        System.out.print(".");
        delayInMillis = 0;
        numMessages = MIN_QPS + random.nextInt(QPS_RANGE);
      }

      if (writeToFile) { 
        publishDataToFile(fileName, numMessages, delayInMillis);
      } else { 
        
        new Thread(){
          @Override
          public void run() {
            try {
              publishData(numMessages, delayInMillis);
            } catch (IOException e) {
              System.err.println(e);
            }
          }
        }.start();
      }

      
      Thread.sleep(THREAD_SLEEP_MS);
    }
  }
}
