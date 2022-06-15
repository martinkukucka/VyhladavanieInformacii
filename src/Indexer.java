import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

public class Indexer {
    // indexovanie vyfiltrovanych dat
    public static void createIndex(String docsPath, String indexPath) throws IOException {

        BufferedReader br = null;
        JSONParser parser = new JSONParser();
        // pricinok s datami
        File folder = new File(docsPath);
        // subory s datami
        File[] listOfFiles = folder.listFiles();

        if(listOfFiles == null){
            System.out.println("Missing data directory");
            return;
        }

        int count = 0;

        // priecinok s index subormi
        Directory dir = FSDirectory.open(Paths.get(indexPath));

        // analyzer
        Analyzer analyzer = new StandardAnalyzer();

        // konfiguracia writer-a
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

        // writer
        IndexWriter writer = new IndexWriter(dir, iwc);

        // prechadzanie po suboroch
        for (File file : listOfFiles) {
            // len json subory
            if (file.isFile() && file.getName().endsWith(".json")) {
                try {
                    String currentLine;
                    br = new BufferedReader(new FileReader(file));
                    // citanie po riadkoch
                    while ((currentLine = br.readLine()) != null) {
                        Object obj;
                        try {
                            // novy dokument
                            Document doc = new Document();

                            obj = parser.parse(currentLine);
                            JSONObject jsonObject = (JSONObject) obj;
                            count++;

                            // vyber informacii z riadku
                            String id = jsonObject.get("id").toString();
                            String title = jsonObject.get("title").toString();
                            String names = jsonObject.get("names").toString();
                            String[] listnames = names.split("\",\"");
                            String birth_date = jsonObject.get("birth_date").toString();
                            String death_date = jsonObject.get("death_date").toString();
                            String birth_place = jsonObject.get("birth_place").toString();
                            String death_place = jsonObject.get("death_place").toString();

                            // pridanie informacii do dokumentu
                            doc.add(new StringField("id", id, Field.Store.YES));
                            doc.add(new TextField("title", title, Field.Store.YES));
                            for (int i=0; i<listnames.length; i++) {
                                listnames[i] = listnames[i].replaceAll("[\"\\[\\]]", "");

                                Field field = new TextField("names", listnames[i], Field.Store.YES);
                                doc.add(field);
                            }
                            doc.add(new TextField("birth_date", birth_date, Field.Store.YES));
                            doc.add(new TextField("death_date", death_date, Field.Store.YES));
                            doc.add(new TextField("birth_place", birth_place, Field.Store.YES));
                            doc.add(new TextField("death_place", death_place, Field.Store.YES));

                            // aktualizacia dokumentu na zaklade id
                            writer.updateDocument(new Term("id", id), doc);

                            // commit kazdych 10000 suborov
                            if(count % 10000 == 0 && count != 0){
                                writer.commit();
                            }
                        }
                        catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                finally {
                    try {
                        if (br != null)
                            br.close();
                    }
                    catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
        // zaverecny commit
        writer.commit();
        // zatvorenie writera
        writer.close();
    }

    public static void main(String[] args) throws Exception {
//        kontrola argumentov
        if (args.length != 2) {
            System.out.println("Wrong arguments, use: data_directory index_directory");
            System.exit(1);
        }

//        nacitanie argumentov
        String docsPath = args[0];
        String indexPath = args[1];

//        zavolanie funkcie na vytvorenie indexu
        createIndex(docsPath, indexPath);
    }
}