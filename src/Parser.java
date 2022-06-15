import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
//C:\Users\Martin\IntelliJ\VI_Project\dataset\enwiki-latest-pages-meta-current11.xml-p5399367p6899366

//start 19:53
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_extract;

public class Parser {

//    parsovanie title
    public static UDF1<String, String> titleParser = title -> {
        String new_title = title;
        if(title.contains("User:") || title.contains("Wikipedia:") || title.contains("Talk:") || title.contains("Template:")){
            new_title = title.substring(title.indexOf(":") + 1);
        }
        new_title = new_title.replaceAll("\"", "'");
        new_title = new_title.replaceAll("[\\/\\:\\*\\?\\<\\>\\|\\\\]", "-");
        return new_title;
    };

    private static final String[] months = {"January", "February", "March", "April",
            "May", "June", "July", "August", "September",
            "October", "November", "December"};

    private static final String era = "BC|B.C.|BCE|B.C.E.";
    private static final String century = "st|nd|rd|th";

    private static final String[] months3 = {"Jan", "Feb", "Mar", "Apr",
            "May", "Jun", "Jul", "Aug", "Sep",
            "Oct", "Nov", "Dec"};

    //    podporovane formaty datumu
    private static final DateTimeParser[] dateParsers = {
            DateTimeFormat.forPattern("MM dd, yyyy").getParser(),
            DateTimeFormat.forPattern("dd MM yyyy").getParser(),
            DateTimeFormat.forPattern("yyyy MM dd").getParser(),
            DateTimeFormat.forPattern("MM, yyyy").getParser(),
            DateTimeFormat.forPattern("MM yyyy").getParser(),
            DateTimeFormat.forPattern("yyyy").getParser()
    };

    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, dateParsers).toFormatter();

    //    overenie ci sa string nachadza v liste
    public static boolean stringContainsItemFromList(String inputStr, String[] items) {
        return Arrays.stream(items).anyMatch(inputStr::contains);
    }

    //    preformatovanie datumu z textoveho (cele nazvy, 3-pismenkove nazvy) na ciselny format,
    public static String monthToDigit(String line) {
        for(int i = 0; i < months.length; i++){
            if(line.contains(months[i])){
                line = line.replaceAll(months[i], String.valueOf(i+1));
            }
            else if(line.contains(months3[i])){
                line = line.replaceAll(months3[i], String.valueOf(i+1));
            }
        }
        return line;
    }

    //    parsovanie datumu z textu
    public static UDF1<String, String> dateParser = line -> {
        // prevod mesiacov do ciselneho formatu
        if(stringContainsItemFromList(line, months) || stringContainsItemFromList(line, months3)){
            line = monthToDigit(line);
        }
        line = line.replaceAll("[\\[\\]]*","");

        //        kontrola ci je pred nasim letopoctom alebo po
        Pattern p = Pattern.compile("(\\d+,*)[^\\p{L}0-9]?(\\d+,*)*[^0-9]*(\\d+,*)*\\P{L}?("+era+")?", Pattern.CASE_INSENSITIVE);

        //        kontrola ci je datum ako storocie
        Pattern p2 = Pattern.compile("(\\d+,*)(?:"+century+"){1}\\P{L}+Century\\P{L}?("+era+")?", Pattern.CASE_INSENSITIVE);

        List<String> numbers = new ArrayList<String>();
        Matcher m = p.matcher(line);
        Matcher m2 = p2.matcher(line);

        boolean exist = false;

        //      datum v default formate
        if (m.find()) {
            for(int i = 0; i < m.groupCount(); i++){

                if(m.group(i + 1) != null){
                    numbers.add(m.group(i + 1));
                }

                if(m.group(4) != null){
                    exist = true;
                }
            }
        }

        //      storocie
        if(m2.find()){

            numbers = new ArrayList<String>();

            if(m2.group(1) != null){
                String temp = m2.group(1);
                temp = temp.replaceAll("[^\\d]","");
                int cent;
                if(temp.length()>0){
                    cent = Integer.parseInt(temp);
                    cent = (cent - 1) * 100;
                    numbers.add(String.valueOf(cent));
                }
            }

            if(m2.group(2) != null){
                exist = true;
            }
        }

        String joinedString = String.join(" ", numbers);

        // formatovanie datumu do jednotneho tvaru
        if(joinedString.length() > 0){
            LocalDate finaldate = null;
            // BC - pred nasim letopoctom
            if(exist){
                joinedString = joinedString.replaceAll("[^\\d-]","");
                try {
                    LocalDate temp_date = new LocalDate(formatter.parseDateTime(joinedString));
                    finaldate = new LocalDate("-"+temp_date);
                }
                catch (IllegalArgumentException ignored) {
                }
            }
            // AD - nas letopocet
            else{
                try {
                    finaldate = new LocalDate(formatter.parseDateTime(joinedString));
                }
                catch (IllegalArgumentException ignored) {
                }
            }

            try {
                if(finaldate != null){
                    return finaldate.toString("yyyy-MM-dd");
                }
                else{
                    return "null";
                }
            }
            catch (IllegalArgumentException ignored) {
            }
        }
        return "null";
    };

// parsovanie miesta z textu
    public static UDF1<String, String> placeParser = line -> {

        line = line.substring(line.indexOf("=") + 1);

        Pattern p = Pattern.compile("^([\\p{L}\\[\\]|.,'\\s]*)", Pattern.CASE_INSENSITIVE);
        List<String> places = new ArrayList<String>();
        Matcher m = p.matcher(line);

        while(m.find()){
            String temp = m.group();
            temp = temp.replaceAll("\\|", " ");
            temp = temp.replaceAll("[^\\s.,\\p{L}]", "");
            places.add(temp);
        }

        String str_places = String.join(" ", places);
        str_places = str_places.trim();

        if(str_places.length() != 0){
            return str_places;
        }

        return "null";
    };

//    parsovanie mien z textu
    public static UDF2<String, String, List> namesParser = (text, title) -> {
        List<String> names = new ArrayList<>();
        names.add(title);

        String[] wholetext = text.split("\n");

//          prechadza sa riadok po riadku a zapisuju sa potrebne udaje, meno, alternativne mena, datumy, miesta
        for (String line : wholetext){
            if (line.contains("name")){

                final Pattern pattern = Pattern.compile("^[\\s\\|\\p{L}\\_]*name\\s*\\=\\s*([\\p{L} \\'\\-\\.\\’]*)", Pattern.CASE_INSENSITIVE);
                final Matcher matcher = pattern.matcher(line);

                while(matcher.find()){
                    String name = matcher.group(1);
                    name = name.replaceAll("\\s+$", "");

//                      ziskanie mena ktore zacina a konci pismenom
                    Pattern p = Pattern.compile("\\p{L}");
                    Matcher m = p.matcher(name);
                    String reverse = new StringBuffer(name).reverse().toString();

                    int start_i = 0;
                    int end_i = name.length();

                    if (m.find()) {
                        start_i = m.start();
                    }

                    m = p.matcher(reverse);

                    if (m.find()) {
                        end_i = name.length() - m.start();
                    }

                    if(start_i != 0 || end_i != name.length()){
                        name = name.substring(start_i, end_i);
                    }

                    if(name.length() > 0){
                        names.add(name);
                    }
                }
            }
        }
//            vyfiltrovanie iba unikátnych mien
        names = names.stream().distinct().collect(Collectors.toList());
        return names;
    };

    public static void main(String[] args) throws IOException {
//        kontrola argumentov
        if (args.length != 3) {
            System.out.println("Wrong arguments, use: input_file output_file filtered_infobox");
            System.exit(1);
        }

//        nacitanie argumentov
        String input = args[0];
        String output = args[1];
        String types = args[2];

        System.out.println("Input: "+input);
        System.out.println("Output: "+output);
        System.out.println("Filtered types: "+types);

//        nacitanie vyfiltrovanych typov infoboxov
        List<String> result = Files.readAllLines(Paths.get(types));
        String strresult = String.join("|", result);

        SparkConf sparkConf = new SparkConf()
//                .set("spark.driver.cores", "6")
//                .set("spark.driver.memory", "4g")
//                .set("spark.executor.cores", "6")
//                .set("spark.executor.memory", "4g")
//                .set("spark.executor.instances", "6")
                .setMaster("local[*]")
                .setAppName("Parser");

//        spark session
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

//        nacitanie XML datasetu podla tagu <page>
        Dataset<Row> df_people = spark.read().format("xml").option("rowTag", "page").load(input);

        df_people  = df_people.withColumn("text",col("revision.text._VALUE"));
        df_people  = df_people.select("title","id","text");
//        df_people.printSchema();
//        df_people.show();

//        vyfiltrovanie iba vhodnych infoboxov
//        final Pattern mpattern = Pattern.compile("(?i)(\\{\\{\\s*infobox\\s*("+strresult+"$))", Pattern.CASE_INSENSITIVE);
        final Pattern mpattern = Pattern.compile("(?i)(\\{\\{\\s*infobox\\s*("+strresult+"))", Pattern.CASE_INSENSITIVE);

        // parse vhodnych typov infoboxov
        df_people = df_people.filter(col("text").rlike(mpattern.toString()));

//        ulozenie riadku s datumom narodenia
        df_people = df_people.withColumn("birth_date",regexp_extract(col("text"),"birth_date(.*)",1));
//        ulozenie riadku s datumom umrtia
        df_people = df_people.withColumn("death_date",regexp_extract(col("text"),"death_date(.*)",1));
//        ulozenie riadku s miestom narodenia
        df_people = df_people.withColumn("birth_place",regexp_extract(col("text"),"birth_place(.*)",1));
//        ulozenie riadku s miestom umrtia
        df_people = df_people.withColumn("death_place",regexp_extract(col("text"),"death_place(.*)",1));
//        df_people.show();

//        register funkcii
        spark.udf().register("titleParser", titleParser, DataTypes.StringType);
        spark.udf().register("dateParser", dateParser, DataTypes.StringType);
        spark.udf().register("placeParser", placeParser, DataTypes.StringType);
        spark.udf().register("namesParser", namesParser, DataTypes.createArrayType(DataTypes.StringType));

//        parse title
        Dataset<Row> dataset = df_people.withColumn("title", functions.callUDF("titleParser", col("title")));
//        parse mien - alternativne meno, meno narodenia...
        dataset = dataset.withColumn("names", functions.callUDF("namesParser", col("text"), col("title")));
//        parse datumu narodenia
        dataset = dataset.withColumn("birth_date", functions.callUDF("dateParser", col("birth_date")));
//        parse datumu umrtia
        dataset = dataset.withColumn("death_date", functions.callUDF("dateParser", col("death_date")));
//        parse miesta narodenia
        dataset = dataset.withColumn("birth_place", functions.callUDF("placeParser", col("birth_place")));
//        parse miesta umrtia
        dataset = dataset.withColumn("death_place", functions.callUDF("placeParser", col("death_place")));

//        ulozenie v json formate
        dataset.select(col("id"),
                        col("title"),
                        col("names"),
                        col("birth_date"),
                        col("death_date"),
                        col("birth_place"),
                        col("death_place"))
                .write()
                .format("json")
                .save(output);
//        System.out.println(dataset.count());
    }
}