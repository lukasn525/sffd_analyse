"""
Quellenwaechter - prueft main.tex gegen das Register in literatur.bib.

    python tools/quellen.py            pruefen und beide Uebersichten erzeugen
    python tools/quellen.py --nur-pruefen   nur pruefen, nichts schreiben

Ausgang: docs/10_QUELLEN.txt    Lesefassung fuers Repo
         docs/quellen.html      visuelle Uebersicht fuer den Browser
Exit:    1, wenn eine harte Regel verletzt ist

NICHT TEIL DER ABGABE - wie tools/pruefe_zahlen.py und tools/codebook.py.

--------------------------------------------------------------------------
WOZU
--------------------------------------------------------------------------
`docs/03_STAND.md` ist der einzige Ort fuer Ergebniszahlen, und
`tools/pruefe_zahlen.py` erzwingt das. Fuer QUELLEN gab es bis zum
18.08.2026 keine Entsprechung - mit zwei Folgen:

  - Marban et al. (2009) trug sieben Fussnoten mit den
    CRISP-DM-Phasendefinitionen. Die stehen woertlich bei Wirth & Hipp
    (2000). Eine Fehlzuschreibung, die monatelang unbemerkt blieb.
  - Sechs Eintraege lagen in der Datenbank, die nie freigegeben waren.

Beides waere aufgefallen, haette es diese Pruefung gegeben.

--------------------------------------------------------------------------
DIE DREI REGELN, DIE HIER ERZWUNGEN WERDEN
--------------------------------------------------------------------------
  R1  Zitiert werden darf nur, was auf status = frei steht.
  R2  Zitiert werden darf nur eine SEITE, die unter seiten = eingetragen
      ist. Das ist die eigentliche Absicherung: Eine Seitenzahl kann nicht
      mehr geraten werden, weil eine ungenehmigte Seite den Lauf bricht.
  R3  Eine Quelle ohne Zugang kann nicht ins Abgabe-Zip und ist deshalb
      nicht zitierfaehig (Auflage Schroeter). Wird als Warnung gemeldet,
      nicht als Fehler - der Zugang kann sich noch klaeren.

`\\footcite[??]{...}` ist ausdruecklich ERLAUBT und wird gezaehlt. Das ??
ist der ehrliche Platzhalter fuer eine noch nicht nachgeschlagene Seite;
es steht sichtbar in der PDF und faellt beim Korrekturlesen auf.

--------------------------------------------------------------------------
WARUM DIE REGISTERDATEN IN literatur.bib STEHEN
--------------------------------------------------------------------------
Weil sonst zwei Dateien uebereinstimmen muessten. biber ignoriert alles
ausserhalb der @-Eintraege, also stoeren die %%-Zeilen den Satz nicht und
stehen trotzdem direkt neben dem Eintrag, zu dem sie gehoeren.

--------------------------------------------------------------------------
PRUEFAUFTRAEGE
--------------------------------------------------------------------------
  1  Laeuft der Waechter mit Exit-Code 0? Sonst steht in der Ausgabe, welche
     Stelle in main.tex gegen welche Regel verstoesst.
  2  Ist die Zahl der ??-Stellen gesunken? Sie ist das Mass fuer die noch
     offene Seitenarbeit.
  3  Sind Eintraege mit vollstaendig = nein noch vorhanden? Die duerfen
     nicht auf frei gesetzt werden, bevor die Angaben geprueft sind.
"""
from __future__ import annotations

import html
import re
import sys
import textwrap
from pathlib import Path

WURZEL = Path(__file__).resolve().parents[1]
BIB = WURZEL / "literatur.bib"
TEX = WURZEL / "main.tex"
TXT = WURZEL / "docs" / "10_QUELLEN.txt"
HTM = WURZEL / "docs" / "quellen.html"

FELDER = ["status", "zugang", "seiten", "abschnitt", "link", "vollstaendig",
          "hinweis"]

# LaTeX-Akzente fuer die Anzeige aufloesen. Die .bib bleibt unveraendert -
# dort muessen die Escapes stehen, sonst setzt biber sie falsch.
ESCAPES = {
    r"{\'a}": "a", r"{\'e}": "e", r"{\'i}": "i", r"{\'o}": "o", r"{\'u}": "u",
    r"{\'A}": "A", r"{\'O}": "O", r"{\'U}": "U",
    r'{\"a}': "ae", r'{\"o}': "oe", r'{\"u}': "ue",
    r'{\"A}': "Ae", r'{\"O}': "Oe", r'{\"U}': "Ue",
    r"\&": "&", "{": "", "}": "",
}


def klartext(s: str) -> str:
    """LaTeX-Schreibweise in lesbaren Text; ' and ' wird zu ' · '."""
    for a, b in ESCAPES.items():
        s = s.replace(a, b)
    s = s.replace(" and others", " et al.").replace(" and ", " \u00b7 ")
    return s.strip()


def kurz(url: str, n: int = 52) -> str:
    """Link fuer die Anzeige kuerzen, ohne ihn unkenntlich zu machen."""
    z = url.replace("https://", "").replace("http://", "")
    return z if len(z) <= n else z[:n - 1] + "\u2026"
MINDESTQUELLEN = 30          # Vorgabe Schroeter: min. 30, eher bis 100


# ==========================================================================
# 1  LESEN
# ==========================================================================
def lies_register() -> dict:
    """Liest literatur.bib und gibt je Kennung die Register- und Bibdaten.

    Ein:  nichts, liest literatur.bib
    Aus:  {kennung: {status, zugang, seiten, abschnitt, link, vollstaendig,
                     hinweis, autor, titel, jahr, typ}}

    - die %%-Zeilen UEBER einem Eintrag gehoeren zu diesem Eintrag; ein
      leerer %%-Block wird beim naechsten @ zurueckgesetzt
    - fehlt ein Feld, steht dort der leere String statt eines KeyError
    """
    text = BIB.read_text(encoding="utf-8")
    reg, offen = {}, {}
    kopf = re.compile(r"^%%\s*(" + "|".join(FELDER) + r")\s*=\s*(.*?)\s*$")
    for zeile in text.split("\n"):
        m = kopf.match(zeile)
        if m:
            offen[m.group(1)] = m.group(2)
            continue
        m = re.match(r"^@(\w+)\{([^,]+),", zeile)
        if m:
            reg[m.group(2)] = dict(
                {f: "" for f in FELDER}, **offen, typ=m.group(1),
                autor="", titel="", jahr="")
            offen = {}

    # Autor, Titel, Jahr aus den Eintraegen nachziehen
    for m in re.finditer(r"@\w+\{([^,]+),(.*?)\n\}", text, re.S):
        key, koerper = m.group(1), m.group(2)
        if key not in reg:
            continue
        for feld in ("author", "title", "year"):
            f = re.search(r"\n\s*" + feld + r"\s*=\s*\{(.*?)\}?,?\s*\n", koerper, re.S)
            if f:
                wert = re.sub(r"\s+", " ", f.group(1)).strip(" {}")
                reg[key]["autor" if feld == "author" else
                          "titel" if feld == "title" else "jahr"] = wert
    return reg


def lies_zitate() -> list:
    """Alle Zitierstellen aus dem FLIESSTEXT von main.tex.

    Ein:  nichts, liest main.tex
    Aus:  Liste von (zeilennummer, kennung, seitenangabe oder None)

    - Kommentarzeilen werden uebersprungen: dort stehen Ankuendigungen, die
      noch keine Zitate sind
    """
    zitate = []
    for nr, zeile in enumerate(TEX.read_text(encoding="utf-8").split("\n"), 1):
        if zeile.lstrip().startswith("%"):
            continue
        for post, keys in re.findall(r"\\(?:foot)?cite\w*\[([^\]]*)\]\{([^}]+)\}", zeile):
            for k in keys.split(","):
                zitate.append((nr, k.strip(), post.strip()))
        for keys in re.findall(r"\\(?:foot)?cite\w*\{([^}]+)\}", zeile):
            for k in keys.split(","):
                if k.strip() != "*":
                    zitate.append((nr, k.strip(), None))
    return zitate


def seiten_menge(feld: str) -> set:
    """Zerlegt das Feld seiten= in einzelne freigegebene Angaben."""
    return {s.strip() for s in feld.split(";") if s.strip()}


# ==========================================================================
# 2  PRUEFEN
# ==========================================================================
def pruefe(reg: dict, zitate: list) -> tuple:
    """Prueft die drei Regeln.

    Ein:  Register, Zitierstellen
    Aus:  (fehler, warnungen, offene_seiten) - je Liste von Textzeilen
    """
    fehler, warnung, offen = [], [], []
    for nr, key, post in zitate:
        if key not in reg:
            fehler.append(f"Z.{nr:<5} {key}: steht nicht in literatur.bib")
            continue
        e = reg[key]
        if e["status"] != "frei":
            fehler.append(f"Z.{nr:<5} {key}: status = {e['status'] or 'leer'}, "
                          f"nicht zitierfaehig (R1)")
            continue
        if post is None:
            warnung.append(f"Z.{nr:<5} {key}: ohne Seitenangabe zitiert")
            continue
        if post == "??":
            offen.append(f"Z.{nr:<5} {key}: Seite noch nachzuschlagen")
            continue
        frei = seiten_menge(e["seiten"])
        if not frei:
            fehler.append(f"Z.{nr:<5} {key}: Seite \"{post}\" zitiert, aber "
                          f"KEINE Seite freigegeben (R2)")
        else:
            for teil in [p.strip() for p in post.split(",") if p.strip()]:
                if teil not in frei:
                    fehler.append(
                        f"Z.{nr:<5} {key}: Seite \"{teil}\" nicht freigegeben "
                        f"(R2). Frei: {', '.join(sorted(frei))}")

    zitiert = {k for _, k, _ in zitate}
    for key, e in sorted(reg.items()):
        if e["status"] == "frei" and key in zitiert and e["zugang"] in ("zu",):
            warnung.append(f"{key}: zitiert, aber Zugang = zu - kein PDF "
                           f"fuers Abgabe-Zip (R3)")
        if e["status"] == "frei" and e["vollstaendig"] == "nein":
            warnung.append(f"{key}: freigegeben, aber bibliografische Angaben "
                           f"sind ungeprueft")
    return fehler, warnung, offen


# ==========================================================================
# 3  TEXTFASSUNG
# ==========================================================================
def schreibe_txt(reg: dict, zitate: list, fehler, warnung, offen) -> None:
    zaehl = {}
    for _, k, _ in zitate:
        zaehl[k] = zaehl.get(k, 0) + 1
    frei = [k for k, e in reg.items() if e["status"] == "frei"]

    z = ["=" * 78,
         "  QUELLENREGISTER",
         "  erzeugt von tools/quellen.py aus literatur.bib - NICHT von Hand aendern",
         "=" * 78, "",
         f"  freigegeben {len(frei):>3}      Vorgabe Schroeter: mindestens {MINDESTQUELLEN}",
         f"  in Pruefung {len([k for k,e in reg.items() if e['status']=='pruef']):>3}",
         f"  abgelehnt   {len([k for k,e in reg.items() if e['status']=='weg']):>3}",
         "",
         f"  Zitierstellen in main.tex   {len(zitate)}",
         f"  davon Seite offen (??)      {len(offen)}",
         f"  Fehler                      {len(fehler)}",
         "", ""]

    for titel, status in (("FREIGEGEBEN", "frei"),
                          ("IN PRUEFUNG - nicht zitierfaehig", "pruef"),
                          ("ABGELEHNT", "weg")):
        z += ["-" * 78, f"  {titel}", "-" * 78, ""]
        for key in sorted(k for k, e in reg.items() if e["status"] == status):
            e = reg[key]
            z.append(f"{key:<58}[{e['status']}]")
            z.append(f"  Autor      {klartext(e['autor'])[:62]}")
            z.append(f"  Titel      {klartext(e['titel'])[:62]}")
            if e["link"]:
                z.append(f"  Link       {e['link']}")
            z.append(f"  Zugang     {e['zugang'] or '?'}")
            z.append(f"  Seiten frei  {e['seiten'] or '-'}")
            z.append(f"  Abschnitt  {e['abschnitt'] or '-'}")
            z.append(f"  zitiert    {zaehl.get(key, 0)}x in main.tex")
            if e["vollstaendig"] == "nein":
                z.append("  ACHTUNG    bibliografische Angaben ungeprueft")
            if e["hinweis"]:
                for i, teil in enumerate(textwrap.wrap(e["hinweis"], 62)):
                    z.append(("  Hinweis    " if i == 0 else "             ") + teil)
            z.append("")

    for titel, liste in (("FEHLER", fehler), ("WARNUNGEN", warnung),
                         ("OFFENE SEITEN", offen)):
        if liste:
            z += ["-" * 78, f"  {titel}", "-" * 78, ""]
            z += ["  " + x for x in liste] + [""]

    z.append("=" * 78)
    TXT.write_text("\n".join(z), encoding="utf-8")


# ==========================================================================
# 4  HTML-FASSUNG
# ==========================================================================
# Farben nach dem Statuspalettenteil des Designsystems - good/warning/critical.
# Statusfarben tragen NIE allein: jede Karte fuehrt das Wort mit.
FARBE = {"frei": "#0ca30c", "pruef": "#fab219", "weg": "#d03b3b"}
WORT = {"frei": "freigegeben", "pruef": "in Pruefung", "weg": "abgelehnt"}


def schreibe_html(reg: dict, zitate: list, fehler, warnung, offen) -> None:
    zaehl = {}
    for _, k, _ in zitate:
        zaehl[k] = zaehl.get(k, 0) + 1
    n = {s: len([k for k, e in reg.items() if e["status"] == s])
         for s in ("frei", "pruef", "weg")}
    mit_seiten = len([k for k, e in reg.items()
                      if e["status"] == "frei" and e["seiten"].strip()])
    anteil = min(100, round(100 * n["frei"] / MINDESTQUELLEN))

    def karte(key, e):
        f = FARBE.get(e["status"], "#898781")
        link = (f'<a href="{html.escape(e["link"])}">{html.escape(kurz(e["link"]))}</a>'
                if e["link"] else '<span class="mut">kein Link hinterlegt</span>')
        seiten = (f'<b>{html.escape(e["seiten"])}</b>' if e["seiten"].strip()
                  else '<span class="mut">keine Seite freigegeben</span>')
        warn = ('<div class="warn">bibliografische Angaben ungeprueft</div>'
                if e["vollstaendig"] == "nein" else "")
        hinweis = (f'<div class="hin">{html.escape(e["hinweis"])}</div>'
                   if e["hinweis"] else "")
        return f"""
      <article class="q" data-status="{e['status']}">
        <div class="bar" style="background:{f}"></div>
        <div class="body">
          <div class="top">
            <code>{html.escape(key)}</code>
            <span class="tag" style="color:{f};border-color:{f}">
              {WORT.get(e['status'], e['status'])}</span>
          </div>
          <div class="au">{html.escape(klartext(e['autor']))} ({html.escape(e['jahr'])})</div>
          <div class="ti">{html.escape(klartext(e['titel']))}</div>
          <dl>
            <dt>Link</dt><dd>{link}</dd>
            <dt>Zugang</dt><dd>{html.escape(e['zugang'] or '?')}</dd>
            <dt>Seiten frei</dt><dd>{seiten}</dd>
            <dt>Abschnitt</dt><dd>{html.escape(e['abschnitt'] or '-')}</dd>
            <dt>zitiert</dt><dd>{zaehl.get(key, 0)}&times; in main.tex</dd>
          </dl>
          {warn}{hinweis}
        </div>
      </article>"""

    abschnitte = ""
    for status in ("frei", "pruef", "weg"):
        keys = sorted(k for k, e in reg.items() if e["status"] == status)
        if not keys:
            continue
        abschnitte += (f'<h2><span class="dot" style="background:{FARBE[status]}">'
                       f'</span>{WORT[status]} <span class="n">{len(keys)}</span></h2>'
                       f'<div class="grid">'
                       + "".join(karte(k, reg[k]) for k in keys) + "</div>")

    def liste(titel, eintraege, farbe):
        if not eintraege:
            return ""
        zeilen = "".join(f"<li>{html.escape(x)}</li>" for x in eintraege)
        return (f'<h2><span class="dot" style="background:{farbe}"></span>{titel} '
                f'<span class="n">{len(eintraege)}</span></h2>'
                f'<ul class="log">{zeilen}</ul>')

    HTM.write_text(f"""<!DOCTYPE html>
<html lang="de"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quellenregister</title>
<style>
:root{{color-scheme:light dark;
 --surface:#fcfcfb; --plane:#f9f9f7; --ink:#0b0b0b; --ink2:#52514e;
 --mut:#898781; --line:#e1e0d9; --ring:rgba(11,11,11,.10);}}
@media (prefers-color-scheme:dark){{:root{{
 --surface:#1a1a19; --plane:#0d0d0d; --ink:#fff; --ink2:#c3c2b7;
 --mut:#898781; --line:#2c2c2a; --ring:rgba(255,255,255,.10);}}}}
*{{box-sizing:border-box}}
body{{margin:0;padding:32px 24px 64px;background:var(--plane);color:var(--ink);
 font:15px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif;}}
.wrap{{max-width:1180px;margin:0 auto}}
h1{{font-size:22px;margin:0 0 4px}}
.sub{{color:var(--ink2);margin:0 0 28px;font-size:14px}}
.tiles{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
 gap:12px;margin-bottom:14px}}
.tile{{background:var(--surface);border:1px solid var(--ring);border-radius:10px;
 padding:14px 16px}}
.tile .v{{font-size:28px;font-weight:600;letter-spacing:-.02em}}
.tile .l{{color:var(--ink2);font-size:13px;margin-top:2px}}
.prog{{background:var(--surface);border:1px solid var(--ring);border-radius:10px;
 padding:14px 16px;margin-bottom:32px}}
.track{{height:8px;border-radius:4px;background:var(--line);overflow:hidden;
 margin-top:10px}}
.fill{{height:100%;border-radius:4px;background:#0ca30c}}
h2{{font-size:15px;margin:34px 0 12px;display:flex;align-items:center;gap:9px;
 font-weight:600}}
.dot{{width:9px;height:9px;border-radius:50%;display:inline-block;flex:none}}
h2 .n{{color:var(--mut);font-weight:400}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));
 gap:12px}}
.q{{display:flex;background:var(--surface);border:1px solid var(--ring);
 border-radius:10px;overflow:hidden}}
.bar{{width:3px;flex:none}}
.body{{padding:13px 15px;min-width:0;flex:1}}
.top{{display:flex;justify-content:space-between;align-items:center;gap:8px;
 margin-bottom:7px}}
code{{font:600 13px/1 ui-monospace,SFMono-Regular,Menlo,monospace}}
.tag{{font-size:11px;border:1px solid;border-radius:99px;padding:2px 8px;
 white-space:nowrap}}
.au{{font-size:13px;color:var(--ink2)}}
.ti{{font-size:13px;margin:2px 0 10px}}
dl{{display:grid;grid-template-columns:88px 1fr;gap:3px 10px;margin:0;
 font-size:12px}}
dt{{color:var(--mut)}}
dd{{margin:0;overflow-wrap:break-word;word-break:normal}}
a{{color:inherit}}
.mut{{color:var(--mut)}}
.warn{{margin-top:9px;font-size:12px;color:#d03b3b}}
.hin{{margin-top:7px;font-size:12px;color:var(--ink2);border-top:1px solid
 var(--line);padding-top:7px}}
.log{{background:var(--surface);border:1px solid var(--ring);border-radius:10px;
 margin:0;padding:12px 16px 12px 32px;font:12px/1.7 ui-monospace,Menlo,monospace}}
</style></head><body><div class="wrap">
<h1>Quellenregister</h1>
<p class="sub">Erzeugt aus <code>literatur.bib</code> von
<code>tools/quellen.py</code>. Nicht von Hand &auml;ndern &ndash; die Datenquelle
ist die <code>.bib</code>.</p>

<div class="tiles">
  <div class="tile"><div class="v" style="color:{FARBE['frei']}">{n['frei']}</div>
    <div class="l">freigegeben</div></div>
  <div class="tile"><div class="v" style="color:{FARBE['pruef']}">{n['pruef']}</div>
    <div class="l">in Pr&uuml;fung</div></div>
  <div class="tile"><div class="v">{mit_seiten}</div>
    <div class="l">davon mit Seitenfreigabe</div></div>
  <div class="tile"><div class="v">{len(offen)}</div>
    <div class="l">Zitate mit offener Seite</div></div>
  <div class="tile"><div class="v"
    style="color:{'#d03b3b' if fehler else 'inherit'}">{len(fehler)}</div>
    <div class="l">Regelverst&ouml;&szlig;e</div></div>
</div>

<div class="prog">
  <b>{n['frei']} von mindestens {MINDESTQUELLEN} Quellen</b>
  <span class="mut">&nbsp;&ndash; Vorgabe Schr&ouml;ter, laut Sprechstunde eher bis 100</span>
  <div class="track"><div class="fill" style="width:{anteil}%"></div></div>
</div>

{liste("Regelverst&ouml;&szlig;e", fehler, "#d03b3b")}
{liste("Warnungen", warnung, "#fab219")}
{liste("Offene Seitenzahlen", offen, "#ec835a")}
{abschnitte}
</div></body></html>""", encoding="utf-8")


# ==========================================================================
def main(argv: list) -> int:
    reg = lies_register()
    zitate = lies_zitate()
    fehler, warnung, offen = pruefe(reg, zitate)

    if "--nur-pruefen" not in argv:
        schreibe_txt(reg, zitate, fehler, warnung, offen)
        schreibe_html(reg, zitate, fehler, warnung, offen)

    frei = len([k for k, e in reg.items() if e["status"] == "frei"])
    print(f"{len(reg)} Eintraege · {frei} freigegeben · "
          f"{len(zitate)} Zitierstellen")
    for titel, liste_ in (("FEHLER", fehler), ("Warnungen", warnung),
                          ("offene Seiten", offen)):
        if liste_:
            print(f"\n{titel} ({len(liste_)}):")
            for x in liste_:
                print("  " + x)
    if not fehler:
        print("\nKeine Regelverstoesse.")
    if "--nur-pruefen" not in argv:
        print(f"\ngeschrieben: {TXT.relative_to(WURZEL)} · "
              f"{HTM.relative_to(WURZEL)}")
    return 1 if fehler else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
