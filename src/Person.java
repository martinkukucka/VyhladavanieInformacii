import org.joda.time.LocalDate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//https://www.joda.org/joda-time/
public class Person {
    private String name;
    private List<String> other_names = new ArrayList<>();
    private LocalDate birth_date;
    private LocalDate death_date;
    private String birth_place;
    private String death_place;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setOther_names(String other_names) {
        this.other_names = new ArrayList<String>(Arrays.asList(other_names.split("\\|")));
    }

    public String getOther_names() {
        return String.join(", ", other_names);
    }

    public void addOther_names(String name) {
        if(name.length() > 0){
            this.other_names.add(name);
        }
    }

    public LocalDate getBirth_date() {
        return birth_date;
    }

    public void setBirth_date(LocalDate birth_date) {
        this.birth_date = birth_date;
    }

    public LocalDate getDeath_date() {
        return death_date;
    }

    public void setDeath_date(LocalDate death_date) {
        this.death_date = death_date;
    }

    public String getBirth_place() {
        return birth_place;
    }

    public void setBirth_place(String birth_place) {
        this.birth_place = birth_place;
    }

    public String getDeath_place() {
        return death_place;
    }

    public void setDeath_place(String death_place) {
        this.death_place = death_place;
    }
}