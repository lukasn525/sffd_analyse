# Zwei Fehler, gefunden am 10./11.08.2026

*In einfachen Worten. Für mich selbst, für die Sprechstunde und als Grundlage
für die kritische Reflexion in Kapitel 8.*

---

## Vorweg: kein einziges Ergebnis ist falsch

Beides waren Fehler in der **Beschreibung**, nicht in der Rechnung. Keine Zahl
in `results/` ändert sich, kein Modell muss neu laufen. Nachgemessen, nicht
gehofft: Nach der Korrektur liefern die Skripte bitgleiche Dateien.

Trotzdem waren beide gefährlich, und der zweite mehr als der erste.

---

## Fehler 1 — Dasselbe Modell stand zweimal im Code

### Was los war

Die Klassifikation hat eine Vergleichsmesslatte: ein einfaches statistisches
Modell, das Random Forest und XGBoost schlagen müssen. Dieses Modell war an
**zwei Stellen** aufgeschrieben — einmal für die Kreuzvalidierung, einmal für
die Schlussbewertung am Ende.

Beide Fassungen waren identisch. Das ist genau das Tückische daran.

### Warum das ein Problem ist

Stell dir zwei Kopien desselben Rezepts vor, in zwei Küchen. Solange niemand
etwas ändert, kommt zweimal dasselbe heraus. Ändert jemand in einer Küche das
Salz, schmeckt es unterschiedlich — und niemand merkt es, weil beide Köche
überzeugt sind, nach demselben Rezept zu arbeiten.

Im Code hätte das bedeutet: Die Kreuzvalidierung misst gegen ein Modell, die
Schlussbewertung gegen ein anderes. Der Vergleich wäre still ungültig geworden.
Keine meiner Prüfungen hätte angeschlagen — der Zahlenwächter vergleicht die
Dokumentation mit den Ergebnisdateien, aber nicht Code mit Code.

### Was daran ärgerlich ist

Der andere Strang der Arbeit — die Vorhersage der Einsatz**menge** — war von
Anfang an richtig gebaut. Dort holt sich die Schlussbewertung das Modell aus
derselben Datei, in der es definiert ist. Es war also nicht Unwissen, sondern
Unachtsamkeit: derselbe Gedanke, einmal umgesetzt und einmal vergessen.

### Was jetzt anders ist

Das Modell steht an genau einer Stelle. Beide Verwendungen holen es von dort.
Geprüft: Vorhersagen, Wahrscheinlichkeiten und Klassenreihenfolge sind vorher
und nachher exakt gleich, Abweichung 0,0.

---

## Fehler 2 — Die Begründung widersprach der Umsetzung

Der ernstere von beiden.

### Die Sachfrage dahinter

Die Einsatzzahlen schwanken stark. Das einfachste Zählmodell — Poisson — geht
davon aus, dass die Schwankung ungefähr so groß ist wie der Durchschnitt. In
meinen Daten ist sie **54-mal so groß**. Die Annahme ist also klar verletzt.

Die Frage ist: Was folgt daraus?

### Was ich zuerst geschrieben hatte

„Die Annahme ist verletzt, also fällt Poisson raus." Klingt logisch, ist aber
zu kurz gedacht.

### Was tatsächlich folgt

Es kommt darauf an, **was man vom Modell will**.

Ein Bild dazu: eine Küchenwaage, die im Durchschnitt richtig wiegt, deren
Anzeige aber zittert. Willst du nur wissen, wie schwer das Mehl ist — kein
Problem, der Wert stimmt. Willst du behaupten „das sind 500 g, plus/minus 2 g" —
dann ist das Zittern ein Problem, weil du die Genauigkeit falsch angibst.

Genauso hier. Die verletzte Annahme beschädigt die **Genauigkeitsangaben** des
Poisson-Modells, nicht seine **Vorhersagen**. Meine Messlatte macht
ausschließlich Vorhersagen — sie behauptet nirgends, wie genau ein einzelner
Einfluss geschätzt ist. Sie ist von dem Problem also nicht betroffen. Das ist
kein Trick, sondern ein bekanntes Ergebnis der Ökonometrie (Gourieroux, Monfort
und Trognon, 1984).

Die starke Schwankung hat trotzdem eine echte Folge — nur an anderer Stelle:
Random Forest und XGBoost mussten ihre Fehlerbewertung umstellen, weil ein
gewöhnlicher quadratischer Fehler bei so schiefen Zahlen die großen Stadtteile
dominieren lässt. Der Befund war also nie falsch. Er trug nur die falsche
Schlussfolgerung.

### Warum das schlimmer ist als Fehler 1

Betroffen war ausgerechnet das Dokument, das die Wahl der Messlatte
**begründen** soll. Wer es gelesen hätte, hätte dort gefunden: „Poisson scheidet
aus" — und im Modellteil ein Poisson-Modell als Messlatte.

Von außen sieht das aus wie ein Widerspruch, den ich nicht bemerkt habe.
Tatsächlich war die Entscheidung gut begründet und von Prof. Schröter am
08.08. schriftlich freigegeben — nur hatte ich das Begründungsdokument nicht
nachgezogen. Das ist genau das Muster, das im Gutachten des Vorprojekts
kritisiert wurde: Probleme erkennen, aber nicht sauber zu Ende bringen.

### Der Nebenfund

Beim Korrigieren kam heraus, dass dieselbe Datei noch in einem zweiten Punkt
veraltet war. Sie zeigte für die Klassifikation:

- den **falschen Modellnamen** — eine ältere Variante mit Zusatzbremse, die seit
  Anfang August nicht mehr verwendet wird
- den **falschen Wert** 0,290 statt 0,297
- **5 Durchläufe** statt der inzwischen 50

Grund: Die Datei wird von einem Skript erzeugt, und dieses Skript war seit dem
05.08. nicht mehr gelaufen. Ein Bericht, den man nicht neu erzeugt, altert
still.

---

## Was beide gemeinsam haben

Kein Rechenfehler. In beiden Fällen lebte **eine Information an zwei Orten**,
und nur einer wurde gepflegt.

Genau dagegen war der Zahlenwächter (`tools/pruefe_zahlen.py`) einmal gebaut
worden. Er hat beide nicht gefunden, aus zwei nachvollziehbaren Gründen:

1. Er liest die Dokumentation im Ordner `docs/`. Der fehlerhafte Bericht liegt
   aber unter `results/` — er wird erzeugt, nicht geschrieben, und war deshalb
   nie in seinem Blickfeld.
2. Er sucht nach **Zahlen**. Hier war es ein **Name**.

### Was jetzt dagegen eingebaut ist

Eine fünfte Prüfung. Sie liest aus den Ergebnisdateien, welches Modell
tatsächlich gerechnet wurde, und verlangt, dass der Begründungsbericht genau
dieses Modell nennt. Ein verworfenes Modell darf vorkommen — aber nur, wenn
danebensteht, dass es verworfen wurde.

Der Sollwert kann dabei nicht veralten: Er entsteht bei jedem Lauf neu aus dem,
was die Skripte gerade getan haben.

---

## In zwei Sätzen, falls jemand fragt

> Zwei Dokumentationsfehler, kein Rechenfehler: Ein Vergleichsmodell war doppelt
> im Code definiert, und ein Begründungsdokument war nach einer freigegebenen
> Methodenänderung nicht nachgezogen worden. Beides ist behoben, kein Ergebnis
> ändert sich — und gegen die zugrunde liegende Ursache, dieselbe Information an
> zwei Orten, prüft jetzt eine automatische Kontrolle.
