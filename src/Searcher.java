import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.joda.time.LocalDate;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Searcher {

    public static Person readIndex(String query, String indexPath) throws Exception
    {
        // index prieciniok
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        // citanie indexov
        IndexReader reader = DirectoryReader.open(dir);
        // index searcher
        IndexSearcher searcher = new IndexSearcher(reader);

        // search query
        QueryParser qp = new QueryParser("<default field>", new StandardAnalyzer());

        // vyhladavanie v indexovanych suboroch - max top 10 vyskytov podla query
        TopDocs foundDocs = searcher.search(qp.parse(query),10);

        // pocet vyskytov
        System.out.println("Total results: " + foundDocs.totalHits);

        ArrayList<String> files = new ArrayList<>();
        ArrayList<Float> filesscore = new ArrayList<>();

        boolean bFirst = true;
        Person person = new Person();

        // 10 najvhodnejsich vysledkov
        for (ScoreDoc sd : foundDocs.scoreDocs) {
            Document d = searcher.doc(sd.doc);
            // najvhodnejsi vyskyt
            if(bFirst){
                System.out.println("The most relevant results:");
                bFirst = false;

                person.setName(d.get("title"));
                person.setOther_names(Arrays.toString(d.getValues("names")).replaceAll("[\\[\\]]",""));
                if(!Objects.equals(d.get("birth_date"), "null")){
                    person.setBirth_date(new LocalDate(d.get("birth_date")));
                }
                if(!Objects.equals(d.get("death_date"), "null")){
                    person.setDeath_date(new LocalDate(d.get("death_date")));
                }
                person.setBirth_place(d.get("birth_place"));
                person.setDeath_place(d.get("death_place"));

//                System.out.println(d.get("title"));
//                System.out.println(d.get("names"));
//                System.out.println(d.get("birth_date"));
//                System.out.println(d.get("death_date"));
//                System.out.println(d.get("birth_place"));
//                System.out.println(d.get("death_place"));
            }

            files.add(d.get("title"));
            filesscore.add(sd.score);

            // vypisanie vhodnych vysledkov
            System.out.print(" - "+d.get("title"));
            System.out.println(", Score: " + sd.score);
        }

        if(files.isEmpty()){
            return null;
        }

//        vyberie sa osoba s najvacsim score (vacsie score = relevantnejsi vysledok)
        Float maxVal = Collections.max(filesscore);
        int maxIdx = filesscore.indexOf(maxVal);
        String name = files.get(maxIdx);
        System.out.println("Selected person: "+ name +"\n");
        return person;
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Wrong arguments, use: index_directory");
            System.exit(1);
        }

        final String indexPath = args[0];

        while(true){

            boolean bAexist = true;
            boolean bBexist = true;

            Scanner input = new Scanner(System.in);

            // zadanie query prvej osoby
            System.out.print("Query - first person: ");
            String queryA = input.nextLine();
            // vypnutie programu
            if(queryA.equals("quit")){
                return;
            }
            // vyhladanie na zaklade indexu
            Person A = readIndex(queryA, indexPath);
            // ak sa osoba nenasla
            if (A == null) {
                System.out.println("No results.");
                bAexist = false;
            }

            // zadanie query druhej osoby
            System.out.print("Query - second person: ");
            String queryB = input.nextLine();
            // vypnutie programu
            if(queryB.equals("quit")){
                return;
            }
            // vyhladanie na zaklade indexu
            Person B = readIndex(queryB, indexPath);
            // ak sa osoba nenasla
            if (B == null) {
                System.out.println("No results.");
                bBexist = false;
            }

            // ak osoby existuju
            if (bAexist && bBexist) {
                // rovnake osoby
                if (A.getName().equals(B.getName())) {
                    System.out.println("Same person");
                }
                else {
                    // vypis udajov o osobach
                    System.out.println("FIRST PERSON");
                    System.out.println("Name: " + A.getName());
                    System.out.println("Alternative names: " + A.getOther_names());
                    System.out.println("Date of birth: " + A.getBirth_date());
                    System.out.println("Date of death: " + A.getDeath_date());
                    System.out.println("Place of birth: " + A.getBirth_place());
                    System.out.println("Place of death: " + A.getDeath_place());

                    System.out.print(System.getProperty("line.separator"));

                    System.out.println("SECOND PERSON");
                    System.out.println("Name: " + B.getName());
                    System.out.println("Alternative names: " + B.getOther_names());
                    System.out.println("Date of birth: " + B.getBirth_date());
                    System.out.println("Date of death: " + B.getDeath_date());
                    System.out.println("Place of birth: " + B.getBirth_place());
                    System.out.println("Place of death: " + B.getDeath_place());

                    System.out.print(System.getProperty("line.separator"));

                    boolean bCouldmeet = false;

                    // porovnanie datumov a vyhodnotenie
                    if ((A.getBirth_date() == null) || (B.getBirth_date() == null)) {
                        System.out.println(A.getName() + " / " + B.getName() + " - unknown (missing dates)\n");
                    } else if ((A.getBirth_date().compareTo(B.getBirth_date()) < 0)) {
                        if (A.getDeath_date() == null) {
                            bCouldmeet = true;
                            System.out.println(A.getName() + " / " + B.getName() + " could meet (TRUE), if they are / were alive at the same time, (date of death doesn't exist)");
                        } else
                            if (A.getDeath_date().compareTo(B.getBirth_date()) < 0) {
                            System.out.println(A.getName() + " / " + B.getName() + " could not meet (FALSE)\n");
                        } else if (A.getDeath_date().compareTo(B.getBirth_date()) >= 0) {
                            bCouldmeet = true;
                            System.out.println(A.getName() + " / " + B.getName() + " could meet (TRUE)");
                        }
                    } else if ((B.getBirth_date().compareTo(A.getBirth_date()) < 0)) {
                        if (B.getDeath_date() == null) {
                            bCouldmeet = true;
                            System.out.println(A.getName() + " / " + B.getName() + " could meet (TRUE), if they are / were alive at the same time, (date of death doesn't exist)");
                        } else if (B.getDeath_date().compareTo(A.getBirth_date()) < 0) {
                            System.out.println(B.getName() + " / " + A.getName() + " could not meet (FALSE)\n");
                        } else if (B.getDeath_date().compareTo(A.getBirth_date()) >= 0) {
                            bCouldmeet = true;
                            System.out.println(B.getName() + " / " + A.getName() + " could meet (TRUE)");
                        }
                    } else {
                        bCouldmeet = true;
                        System.out.println(B.getName() + " / " + A.getName() + " could meet (TRUE)");
                    }

                    // ak sa osoby mohli stretnut podla datumov, tak sa vyhodnoti pravdepodobnost stretnutia
                    // aj na zaklade datumov narodenia a datumov umrtia (prekryv lokacii - vyssia pravdepodobnost)
                    if (bCouldmeet) {
                        if (!((A.getBirth_place().equals("null") && A.getDeath_place().equals("null")) || (B.getBirth_place().equals("null") && B.getDeath_place().equals("null")))) {
                            List<String> A_place_tokensB = null;
                            List<String> A_place_tokensD = null;

                            boolean bpA = false;
                            boolean dpA = false;

                            List<String> A_places = null;

                            List<String> B_place_tokensB = null;
                            List<String> B_place_tokensD = null;

                            boolean bpB = false;
                            boolean dpB = false;

                            List<String> B_places = null;


                            if (!A.getBirth_place().equals("null")) {
                                A_place_tokensB = new ArrayList<String>(Arrays.asList(A.getBirth_place().split(",")));
                                bpA = true;
                            }

                            if (!A.getDeath_place().equals("null")) {
                                A_place_tokensD = new ArrayList<String>(Arrays.asList(A.getDeath_place().split(",")));
                                dpA = true;
                            }

                            if (bpA && dpA) {
                                A_places = Stream.concat(A_place_tokensB.stream(), A_place_tokensD.stream()).collect(Collectors.toList());
                            } else if (bpA) {
                                A_places = A_place_tokensB;
                            } else if (dpA) {
                                A_places = A_place_tokensD;
                            }

                            if (!B.getBirth_place().equals("null")) {
                                B_place_tokensB = new ArrayList<String>(Arrays.asList(B.getBirth_place().split(",")));
                                bpB = true;
                            }

                            if (!B.getDeath_place().equals("null")) {
                                B_place_tokensD = new ArrayList<String>(Arrays.asList(B.getDeath_place().split(",")));
                                dpB = true;
                            }

                            if (bpB && dpB) {
                                B_places = Stream.concat(B_place_tokensB.stream(), B_place_tokensD.stream()).collect(Collectors.toList());
                            } else if (bpB) {
                                B_places = B_place_tokensB;
                            } else if (dpB) {
                                B_places = B_place_tokensD;
                            }

                            if (A_places != null && B_places != null) {
                                if (!Collections.disjoint(A_places, B_places)) {
                                    System.out.println("with a HIGH probability, they have some same locations visited during their lives");
                                } else {
                                    System.out.println("with a LOW probability, locations visited during their lives are different");
                                }
                            }
                        }
                    }
                }
            }
            else{
                System.out.println("Unknown person");
            }
        }
    }
}