"""
Alle Abbildungen fuer Kapitel 7 - aus den CSV-Dateien, nicht von Hand.

    python modelle/m05_abbildungen.py

Eingang: results/regression/*.csv · results/klassifikation/*.csv
Ausgang: results/abbildungen/*.pdf

STAND: noch zu implementieren. Setzt m02 und m03 voraus.

Dieses Skript RECHNET NICHTS. Es liest nur. Dadurch laesst sich eine Darstellung
aendern, ohne die Modelle neu zu rechnen, und nach einem neuen Lauf ist ein
Befehl genug.

--------------------------------------------------------------------------
DREI ABBILDUNGEN
--------------------------------------------------------------------------
  a1_boxplot_menge.pdf        Boxplot je Verfahren ueber die 50 Laeufe,
  a1_boxplot_struktur.pdf     je Zielgroesse. Zeigt die Streuung ehrlich,
                              statt sie zu mitteln.

  a2_gegen_baseline.pdf       Balken: jedes Verfahren gegen seine
                              Stufe-2-Baseline, Fehlerbalken aus
                              std_wiederholungen. Das ist die Primaeraussage
                              nach Decision Log #34.

  a3_laufzeit_guete.pdf       Streudiagramm: Trainingszeit (log-Achse) gegen
                              Prognoseguete, ein Punkt je Verfahren.
                              Beantwortet Unterfrage 3 und 4 in einem Bild.

--------------------------------------------------------------------------
ANFORDERUNGEN AN DIE DARSTELLUNG
--------------------------------------------------------------------------
Sie landen im gedruckten Dokument, und Gestaltung war im Gutachten ein eigenes
Bewertungskriterium.

  Format        PDF, nicht PNG. Rasterbilder werden im Druck unscharf.
  Groesse       In der ENDGROESSE erzeugen, nicht gross erzeugen und in LaTeX
                schrumpfen - sonst steht dort 5-pt-Schrift. Mindestens 9 pt.
  Titel         KEINE Titel in der Abbildung. Die Bildunterschrift in LaTeX ist
                der Titel, beides doppelt sich sonst.
  Farbe         Graustufentauglich - Verfahren zusaetzlich ueber Marker oder
                Schraffur unterscheiden, nicht allein ueber Farbe.
  Achsen        Beschriftung mit Einheit, deutsches Dezimalkomma.
  Nulllinie     Bei R2-Darstellungen einzeichnen. Negative Werte sind hier
                normal und muessen als solche erkennbar sein, sonst sieht es
                nach Fehler aus.
  Fehlerbalken  IMMER beschriften: std_wiederholungen, nicht std_folds. Ein
                Fehlerbalken ohne Angabe, was er zeigt, ist bedeutungslos.

--------------------------------------------------------------------------
FALLSTRICK
--------------------------------------------------------------------------
Bei a2 muss die richtige Streuung verwendet werden. `std_folds` ist die
Standardabweichung ueber alle 50 Laeufe und zu optimistisch, weil die Laeufe
nicht unabhaengig sind - es sind dieselben 29 Stadtteile in zehn Gruppierungen.
Massgeblich ist `std_wiederholungen` (docs/06_RISIKEN.md, R-5).

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Sind die Abbildungen bei Textbreite (etwa 15 cm) noch lesbar? Einmal
    ausdrucken, nicht am Bildschirm beurteilen.
  - Stimmen die Werte in a2 mit *_mittel.csv ueberein? Stichprobe genuegt.
  - Ist bei a3 erkennbar, dass die Laufzeiten um Groessenordnungen
    auseinanderliegen? Wenn nicht, ist die Achse falsch skaliert.
"""
raise SystemExit(__doc__)
