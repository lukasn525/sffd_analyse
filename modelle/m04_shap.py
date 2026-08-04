"""
Interpretation: Welche Merkmale tragen die Vorhersage?

    python modelle/m04_shap.py

Eingang: results/regression/menge_mittel.csv · results/klassifikation/struktur_mittel.csv
         data/processed/{regression,klassifikation}.parquet
Ausgang: results/shap/

STAND: noch zu implementieren. Setzt m02 und m03 voraus.

--------------------------------------------------------------------------
DIE EINE REGEL
--------------------------------------------------------------------------
SHAP wird NUR fuer Modelle gerechnet, die ihre Stufe-2-Baseline schlagen. Fuer
alle anderen erklaert man Rauschen - und eine Abbildung, die Beitraege zeigt, wo
kein Signal ist, ist schlimmer als keine Abbildung. Das Skript prueft das selbst
und ueberspringt Modelle, die die Latte reissen.

--------------------------------------------------------------------------
WAS ZU RECHNEN IST
--------------------------------------------------------------------------
  TreeExplainer   fuer Random Forest und XGBoost
  Koeffizienten   fuer Ridge - dort braucht es kein SHAP, die standardisierten
                  Koeffizienten sind direkt interpretierbar
  Fold            EIN Fold, nicht alle. Die Auswahl ist zu begruenden und im
                  Text zu nennen (Vorschlag: der Fold mit dem geringsten
                  Extrapolationsanteil, also der "normalste").

--------------------------------------------------------------------------
FALLSTRICK: BLOCKWEISE INTERPRETIEREN
--------------------------------------------------------------------------
Die Strukturmerkmale sind untereinander korreliert. SHAP verteilt den Beitrag
dann auf mehrere Merkmale, und einzelne Werte sind nicht sinnvoll deutbar -
"median_haushaltseinkommen traegt 8 %" waere eine Scheinpraezision.

Deshalb zusammenfassen zu den drei Faktorgruppen des Exposes:

    soziooekonomisch   median_haushaltseinkommen · armutsquote_pct
                       akademikerquote_pct · median_miete · leerstandsquote_pct
    kriminalitaets-    log_kriminalitaetsindex
      bezogen
    baulich            anteil_altbau_vor_1940_pct · anteil_wohngebaeude_pct
                       anteil_risikogewerbe_pct

    (log_bevoelkerung ist Groessenkontrolle, monat_sin/cos Saison - beide
     gehoeren in keine der drei Gruppen und werden getrennt ausgewiesen.)

Das beantwortet Unterfrage 1 direkt: Welche Faktorgruppe traegt wie viel?

--------------------------------------------------------------------------
HIERHER VERSCHOBEN: DER VIF
--------------------------------------------------------------------------
Die Multikollinearitaetspruefung lag frueher in der Eignungspruefung, entschied
dort aber nichts - Ridge ist durch den L2-Strafterm robust dagegen, Baumverfahren
interessiert sie nicht. Ihre einzige echte Konsequenz betrifft genau diese
Interpretation. Deshalb steht sie hier.

Zu rechnen auf den EINDEUTIGEN Stadtteil-Merkmalskombinationen, nicht auf allen
Zeilen: Die Strukturmerkmale sind innerhalb eines Jahres konstant, ueber alle
Zeilen zaehlte jede Kombination bis zu zwoelfmal und der VIF waere kuenstlich
stabilisiert. Der gemessene Wert (max 11,5 bei `median_haushaltseinkommen`,
7,1 bei `median_miete`) ist die Begruendung fuer die blockweise Auswertung und
gehoert als solche in den Text.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  - Stimmt die Rangfolge der Faktorgruppen zwischen Random Forest und XGBoost
    ueberein? Wenn nicht, ist das ein Befund fuer Kapitel 8, kein Fehler.
  - Passt sie zu den Korrelationen aus der Eignungspruefung? Dort lagen
    log_kriminalitaetsindex und anteil_risikogewerbe_pct vorn.
  - Wird eine Faktorgruppe als praktisch bedeutungslos ausgewiesen? Das waere
    eine der wenigen wirklich inhaltlichen Aussagen der Arbeit.
"""
raise SystemExit(__doc__)
