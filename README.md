# VyhladavanieInformacii

Program obsahuje 3 časti, ktoré sú spustiteľné nezávisle od seba ako 3 rôzne programy:
  - Parser
  - Indexer
  - Searcher

**Parser** beží paralelne na Apache Spark a vykonáva:
  * načítanie datasetu
  * spracovanie datasetu - filtrácia údajov
  * uloženie spracovaných údajov do priečinku vo formáte .json

Spracovanie datasetu prebieha prechádzaním tagu <page> a vyhľadávanie vhodných typov infoboxov, ktoré boli vyfiltrované zo zvoleného datasetu a taktiež doplnené listom infoboxov z Wikipédie: https://en.wikipedia.org/wiki/Wikipedia:List_of_infoboxes. Pri spracovaní jednej stránky s vhodným infoboxom sa extrahujú tieto údaje:
  * id 
  * title = meno
  * meno, alternatívne mená, meno narodenia...
  * dátum narodenia
  * dátum úmrtia
  * miesto narodenia
  * miesto úmrtia

Dátum narodenia aj úmrtia podporuje bežné dátumy, storočia a taktiež aj dátumy pred naším letopočtom. 
Oproti predchádzajúcemu odovzdaniu bolo nutné pridať aj parameter id, aby mohol indexer aktualizovať súbory na základe tohto parametra.

Vyfiltrované dáta vyzerajú takto: 
![image](https://user-images.githubusercontent.com/43440530/173825108-35ca6682-745f-4d6f-babf-2b3eb3ce068f.png)


**Indexer** beží na Apache Lucene a vykonáva indexovanie spracovaných údajov uložených v .json súboroch. Indexer v priečinku po súboroch prechádza jednotlivé riadky a vytvára dokumenty (1 riadok = 1 osoba) na základe parametru id.

**Searcher** beží na Apache Lucene a slúži na vyhľadávanie osôb v zaindexovaných súboroch a vykonávanie hlavnej funkcionality programu - vyhľadanie, či sa dve osoby mohli stretnúť. Vyhľadanie prebieha na základe dvoch queries, ktoré obsahujú informácie o vyhľadávanej osobe, nájdu sa všetky vhodné výskyty a vezme sa výskyt s najväčším skóre. Ak searcher nájde vhodné výskyty osôb v indexe, tak sa prechádza k porovnaniu dátumov a ak sa osoby podľa dátumov mohli stretnúť, tak sa prechádza k porovnaniu miest, ktoré určujú výšku pravdepodobnosti stretnutia na základe podobnosti (rovnaké miesto = vyššia pravdepodobnosť).  

#### Spustenie programu ####

Finálne riešenie sa skladá z 3 častí:
  * Parser
  * Indexer
  * Searcher
Každú časť je možné spustiť nezávisle od seba.

### Podmienky spustenia programu ###

Na spustenie programu sú nutné:
  * Java SDK 1.8.0_311
  * Apache Spark 3.1.2
  * Apache Lucene 7.5.0
  * Scala 2.12.10
  * Spark-XML 2.12-0.5.0
  * org.joda.time
  * org.json.simple

=== Parametre ===

#### Parser ####
  * //parametre spustenia:// dataset (vo formáte XML), priečinok na uloženie vyfiltrovaných údajov, txt súbor s vhodnými typmi infoboxov
  * //výstup:// výstupný priečinok na uloženie vyfiltrovaných údajov

Ukážka parametrov spustenia: 
![image](https://user-images.githubusercontent.com/43440530/173825045-446a1eca-8c62-45cc-9b1e-ee80ebf4fbb8.png)



#### Indexer ####
  * //parametre spustenia:// priečinok s filtrovanými údajmi, priečinok na uloženie indexu
  * //výstup:// výstupný priečinok na uloženie indexu

Ukážka parametrov spustenia: 
![image](https://user-images.githubusercontent.com/43440530/173825163-c1c53f35-dfdb-49c4-b35f-9eccd0ccd053.png)



#### Searcher ####
  * //parametre spustenia:// priečinok s indexom
  * //vstup:// query s informáciami o vyhľadávanej osobe
  * //výstup:// výpis vhodných výsledkov vyhľadania osôb, údaje o zvolených osobách, výsledok, či sa osoby mohli stretnúť

Ukážka parametrov spustenia: 
![image](https://user-images.githubusercontent.com/43440530/173825186-31a779d1-7540-4f5b-9a54-c61e79cf3b09.png)

Ukážka vstupu a výstupu: 
![image](https://user-images.githubusercontent.com/43440530/173825237-bbf5c49f-5ceb-4ed6-ac9f-66fd00c664d9.png)
