# Was ich in meiner Bachelorarbeit gemacht habe

*Ohne Fachbegriffe. Für alle, die fragen — und als Einstieg für das Kolloquium.*

---

## Die Frage

In San Francisco rückt die Feuerwehr rund 2.700-mal im Monat aus. Nicht
gleichmäßig über die Stadt: In manchen Vierteln sind es 6 Einsätze im Monat, in
anderen 280.

Meine Frage war: **Kann man das vorhersagen, ohne die Feuerwehr zu fragen?**
Also allein daraus, wie ein Viertel gebaut ist und wer dort lebt — Einkommen,
Armut, Leerstand, Altbauten, Kriminalität. Und wenn ja: Welches Rechenverfahren
kann das am besten?

Der praktische Sinn dahinter: Eine Stadt, die weiß, wo viel passieren wird, kann
Wachen und Personal besser planen. Und für ein Viertel, für das noch keine Daten
vorliegen — ein Neubaugebiet zum Beispiel — hätte man trotzdem eine Schätzung.

## Die Daten

Elf Jahre, 2015 bis 2025. 35 Stadtteile, jeder Monat einzeln — macht 4.620
Zeilen. Vier Quellen: die Einsatzdaten der Feuerwehr, die US-Volkszählung, die
Kriminalstatistik der Polizei und ein Kataster der Bebauung.

Die wichtigste Regel dabei: **Jeder Monat darf nur Daten sehen, die es damals
schon gab.** Wenn ich den Januar 2020 vorhersage, darf ich keine Zahl aus 2021
verwenden — auch nicht versehentlich. Sonst sagt das Modell nichts vorher, es
schaut nur nach.

## Der Test, der die Arbeit trägt

Der übliche Weg wäre: Man nimmt die ersten Jahre zum Lernen und die letzten zum
Prüfen. Das habe ich bewusst nicht gemacht, und das ist die zentrale Entscheidung
der Arbeit.

Der Grund: Bei diesem Weg kennt das Modell jedes Viertel bereits. Es hat den
Tenderloin schon hundertmal gesehen und weiß, dass dort viel passiert. Dann sagt
es nichts wirklich vorher — es erinnert sich.

Stattdessen habe ich **Viertel weggelassen, nicht Zeiträume**. Das Modell lernt
an 23 Stadtteilen und wird an 6 anderen geprüft, von denen es keinen einzigen
Monat gesehen hat. Es muss also tatsächlich aus Einkommen, Bebauung und
Kriminalität schließen, wie viel dort los ist. Das ist die schwierigere Aufgabe —
und die einzige, die meine Frage wirklich beantwortet.

Sechs Stadtteile habe ich dabei vollständig beiseitegelegt und bis ganz zum
Schluss nicht angesehen — kein einziges Mal, bei keiner Zwischenentscheidung.
Erst als alles fertig war, habe ich einmal darauf geprüft. Das ist wie eine
Klausur, deren Aufgaben man vorher nicht kennt: Nur so ist das Ergebnis ehrlich.

## Was herauskam

**Erstens: Ja, es funktioniert — und zwar gut.** Für die sechs unbekannten
Stadtteile erklärt das Modell rund **80 Prozent** der Unterschiede in der
Einsatzzahl. Ohne diese Merkmale, also wenn man einfach den Durchschnitt aller
Viertel raten würde, wären es null. Struktur und Sozialdaten eines Viertels sagen
also tatsächlich, wie viel die Feuerwehr dort zu tun hat.

**Zweitens: Es ist egal, welches der drei Verfahren man nimmt.** Ich habe ein
einfaches statistisches Verfahren gegen zwei moderne Machine-Learning-Verfahren
antreten lassen. Die modernen haben nicht gewonnen. Sie waren auch nicht
schlechter — sie waren schlicht **nicht unterscheidbar**. Das einfache Verfahren
ist dabei rund **500-mal schneller**.

**Drittens — und das ist mein eigentlicher Beitrag:** Was tatsächlich über Erfolg
oder Misserfolg entscheidet, ist nicht die Wahl des Verfahrens, sondern **ob man
dem Modell sagt, dass große Viertel mehr Einsätze haben**.

Das klingt banal, ist aber der ganze Unterschied. Solange die Verfahren die
Einwohnerzahl selbst herausfinden mussten, lagen sie weit daneben. Sobald ich
ihnen vorgab, „rechne pro tausend Einwohner und multipliziere am Ende hoch",
waren sie so gut wie das einfache Modell. Der Effekt dieser einen
Entscheidung ist **fast sechzigmal größer** als der Unterschied zwischen allen
drei Verfahren zusammen.

## Warum „kein Gewinner" ein gutes Ergebnis ist

Das ist die Frage, die ich am häufigsten bekomme.

Die ehrliche Antwort: Ich habe vorher aufgeschrieben, wie ich mit diesem Fall
umgehe — bevor ich die erste Zahl gesehen hatte. Wer erst rechnet und sich dann
überlegt, welche Auswertung am besten aussieht, findet immer irgendwo einen
Gewinner. Der ist dann aber Zufall.

Und die Aussage selbst ist nützlich. Wer heute so eine Aufgabe hat, greift
reflexhaft zu den aufwendigen Verfahren. Meine Arbeit zeigt an einem konkreten
Fall: Das kostet hier das Fünfhundertfache an Rechenzeit und bringt nichts. Die
Zeit ist besser investiert, wenn man vorher nachdenkt, wie das Problem eigentlich
gebaut ist.

Kurz: **Ein sorgfältig aufgesetztes einfaches Modell schlägt ein achtlos
aufgesetztes kompliziertes.** Das ist kein Nullergebnis, das ist eine Empfehlung.

## Was ich nicht behaupte

- **Keine Ursachen.** Wenn in einem Viertel mit hoher Kriminalität mehr Einsätze
  sind, heißt das nicht, dass das eine das andere verursacht. Beides kann
  denselben Hintergrund haben. Ich sage vorher, ich erkläre nicht.
- **Nur 35 Stadtteile.** Das klingt nach viel — 4.620 Datenzeilen —, aber es sind
  eben nur 35 wirklich verschiedene Orte. Kleine Zahlen tragen weniger weit, und
  das steht auch so in der Arbeit.
- **Nur San Francisco.** Ob dasselbe in Köln gilt, weiß ich nicht.

## Und was mir dabei am meisten geholfen hat

Dreimal hatte ich eine überzeugende Erklärung für ein Ergebnis. Dreimal habe ich
sie nachgerechnet, statt sie aufzuschreiben. Dreimal war sie falsch.

Das steht jetzt so in der Arbeit — inklusive der Erklärungen, die nicht
gestimmt haben. Es hat mich mehr Zeit gekostet als alles andere und ist
vermutlich der Teil, auf den ich am meisten stolz bin.
