"""Rendert Foliensaetze aus Inhaltsdaten nach HTML und PDF.

Kein pandoc, kein Beamer - eigenes Layout mit voller Kontrolle ueber Satz und
Raster. 16:9 bei 1280x720 CSS-Pixeln.
"""
from __future__ import annotations

import html
import pathlib
import subprocess
import sys

BREITE, HOEHE = 1280, 720

THEMEN = {
    "prep":        {"akzent": "#0f766e", "hell": "#ccfbf1", "matt": "#f0fdfa", "nr": "01"},
    "vorpruefung": {"akzent": "#9a3412", "hell": "#ffedd5", "matt": "#fff7ed", "nr": "02"},
    "modelle":     {"akzent": "#3730a3", "hell": "#e0e7ff", "matt": "#eef2ff", "nr": "03"},
}

CSS = """
@page { size: 1280px 720px; margin: 0; }
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { background: #fff; }
body {
  font-family: 'Inter', sans-serif;
  font-feature-settings: 'ss01','cv05','tnum';
  color: #1e293b;
  -webkit-font-smoothing: antialiased;
}
.folie {
  position: relative; width: 1280px; height: 720px; overflow: hidden;
  page-break-after: always; background: #fff;
  padding: 52px 68px 54px 68px; display: flex; flex-direction: column;
}
.folie::before {
  content: ''; position: absolute; left: 0; top: 0; width: 100%; height: 5px;
  background: var(--akzent);
}
.kopf {
  display: flex; justify-content: space-between; align-items: baseline;
  font-size: 12.5px; letter-spacing: .10em; text-transform: uppercase;
  color: #94a3b8; font-weight: 600; margin-bottom: 20px; flex: 0 0 auto;
}
.kopf .pfad { color: var(--akzent); }
.kopf .pfad b { color: #475569; font-weight: 600; }
h1 { font-family:'Inter Display','Inter',sans-serif; font-size: 40px; line-height: 1.12;
     font-weight: 700; letter-spacing: -.022em; color: #0f172a; }
h2 { font-family:'Inter Display','Inter',sans-serif; font-size: 30px; line-height: 1.15;
     font-weight: 700; letter-spacing: -.018em; color: #0f172a; }
h2 code, h1 code { font-family:'JetBrains Mono',monospace; font-size: .86em; font-weight: 600;
     letter-spacing: -.01em; }
.unter { font-size: 17px; color: #64748b; margin-top: 10px; line-height: 1.45;
     font-weight: 400; max-width: 1000px; }
.koerper { flex: 1 1 auto; min-height: 0; margin-top: 22px;
           display: flex; flex-direction: column; justify-content: center; }
.koerper > * { width: 100%; }
.fuss {
  flex: 0 0 auto; display: flex; justify-content: space-between; align-items: center;
  font-size: 11.5px; color: #cbd5e1; border-top: 1px solid #f1f5f9;
  padding-top: 11px; margin-top: 16px; letter-spacing: .04em;
}
.fuss .nr { font-variant-numeric: tabular-nums; font-weight: 600; color: #94a3b8; }

/* ---------- Bausteine ---------- */
.reihe { display: flex; gap: 22px; align-items: stretch; }
.spalte { flex: 1 1 0; min-width: 0; display: flex; flex-direction: column; }
.spalte > .block, .spalte > .kasten { flex: 1 1 auto; }

.block { border: 1px solid #e2e8f0; border-radius: 10px; padding: 16px 18px 15px; background: #fff; }
.block h3, .kasten h3 {
  font-size: 11.5px; text-transform: uppercase; letter-spacing: .10em;
  color: var(--akzent); font-weight: 700; margin-bottom: 9px;
}
.block p, .kasten p { font-size: 15px; line-height: 1.5; color: #334155; }

.kasten { border-radius: 10px; padding: 16px 18px 15px; background: var(--matt);
          border: 1px solid var(--hell); }
.kasten.frage { background: #fffbeb; border-color: #fde68a; }
.kasten.frage h3 { color: #b45309; }
.kasten.warn { background: #fef2f2; border-color: #fecaca; }
.kasten.warn h3 { color: #b91c1c; }

ul.punkte { list-style: none; }
ul.punkte li {
  position: relative; padding-left: 17px; font-size: 15px; line-height: 1.48;
  color: #334155; margin-bottom: 7px;
}
ul.punkte li:last-child { margin-bottom: 0; }
ul.punkte li::before {
  content: ''; position: absolute; left: 2px; top: 8px; width: 5px; height: 5px;
  border-radius: 50%; background: var(--akzent); opacity: .55;
}
ul.punkte.eng li { font-size: 14px; margin-bottom: 5px; line-height: 1.42; }

code, .mono { font-family: 'JetBrains Mono', monospace; font-size: .88em;
              background: #f1f5f9; padding: 1px 5px; border-radius: 4px; color: #0f172a; }
.kasten code, .block code { background: rgba(255,255,255,.75); }
b, strong { font-weight: 650; color: #0f172a; }

/* Ein/Aus */
.ea { display: flex; gap: 10px; margin-bottom: 8px; align-items: flex-start; }
.ea .tag {
  flex: 0 0 46px; font-size: 10.5px; font-weight: 700; letter-spacing: .08em;
  text-transform: uppercase; color: #fff; background: var(--akzent);
  border-radius: 4px; padding: 3px 0; text-align: center; margin-top: 2px;
}
.ea .tag.aus { background: #64748b; }
.ea .txt { font-size: 14.5px; line-height: 1.45; color: #334155; flex: 1 1 auto; }
.ea .txt code { font-size: .9em; }

/* Tabelle */
table { width: 100%; border-collapse: collapse; }
th { font-size: 10.5px; text-transform: uppercase; letter-spacing: .09em;
     color: #94a3b8; font-weight: 700; text-align: left;
     padding: 0 10px 7px 0; border-bottom: 1.5px solid #e2e8f0; }
td { font-size: 14px; line-height: 1.4; color: #334155; padding: 8px 10px 8px 0;
     border-bottom: 1px solid #f1f5f9; vertical-align: top; }
tr:last-child td { border-bottom: none; }
td.fn { font-family:'JetBrains Mono',monospace; font-size: 12.6px; color: #0f172a;
        font-weight: 500; white-space: nowrap; }
td.z { font-variant-numeric: tabular-nums; color: #94a3b8; font-size: 12.5px;
       white-space: nowrap; text-align: right; }
table.eng td { font-size: 13.2px; padding: 6px 10px 6px 0; }
table.eng td.fn { font-size: 12px; }

/* Fluss */
.fluss { display: flex; align-items: stretch; gap: 0; }
.stufe { flex: 1 1 0; border: 1px solid #e2e8f0; border-radius: 10px;
         padding: 13px 14px; background: #fff; min-width: 0; }
.stufe.akt { border-color: var(--akzent); background: var(--matt); }
.stufe .st-nr { font-size: 10px; font-weight: 700; letter-spacing: .1em;
                color: var(--akzent); text-transform: uppercase; }
.stufe .st-tl { font-family:'JetBrains Mono',monospace; font-size: 13px;
                font-weight: 600; color: #0f172a; margin: 5px 0 6px; }
.stufe .st-tx { font-size: 12.6px; line-height: 1.4; color: #64748b; }
.pfeil { flex: 0 0 34px; display: flex; align-items: center; justify-content: center;
         color: #cbd5e1; font-size: 20px; }

/* Kennzahlen */
.kpi { display: flex; gap: 14px; }
.kpi .k { flex: 1 1 0; border: 1px solid #e2e8f0; border-radius: 10px;
          padding: 13px 15px; background: #fff; }
.kpi .k .w { font-family:'Inter Display','Inter',sans-serif; font-size: 27px;
             font-weight: 700; color: var(--akzent); letter-spacing: -.02em;
             font-variant-numeric: tabular-nums; }
.kpi .k .l { font-size: 12.2px; color: #64748b; margin-top: 3px; line-height: 1.35; }

/* Titelfolie */
.folie.titel { justify-content: center; padding: 0 96px; }
.folie.titel::after {
  content: ''; position: absolute; right: -170px; top: -170px;
  width: 560px; height: 560px; border-radius: 50%; background: var(--matt); z-index: 0;
}
.folie.titel > * { position: relative; z-index: 1; }
.titel .marke { font-size: 13px; letter-spacing: .16em; text-transform: uppercase;
                color: var(--akzent); font-weight: 700; margin-bottom: 20px; }
.titel h1 { font-size: 62px; line-height: 1.04; letter-spacing: -.03em; }
.titel h1 code { font-size: .8em; }
.titel .unter { font-size: 20px; margin-top: 20px; max-width: 760px; color: #475569; }
.titel .meta { margin-top: 40px; padding-top: 22px; border-top: 1px solid #e2e8f0;
               display: flex; gap: 44px; }
.titel .meta div .l { font-size: 10.5px; letter-spacing: .1em; text-transform: uppercase;
                      color: #94a3b8; font-weight: 700; }
.titel .meta div .v { font-size: 15.5px; color: #334155; margin-top: 4px; font-weight: 500; }

/* Kapiteltrenner */
.folie.kapitel { justify-content: center; padding: 0 96px; background: #0f172a; }
.folie.kapitel::before { background: var(--akzent); height: 7px; }
.kapitel .marke { font-size: 12.5px; letter-spacing: .16em; text-transform: uppercase;
                  color: var(--hell); font-weight: 700; margin-bottom: 16px; opacity: .8; }
.kapitel h1 { color: #fff; font-size: 50px; }
.kapitel h1 code { background: rgba(255,255,255,.10); color: #fff; padding: 2px 10px; }
.kapitel .unter { color: #94a3b8; font-size: 18px; max-width: 820px;
                  margin-top: 20px; }
.kapitel .kpi .k { background: rgba(255,255,255,.05); border-color: rgba(255,255,255,.14); }
.kapitel .kpi .k .w { color: #fff; }
.kapitel .kpi .k .l { color: #94a3b8; }
.kapitel .kpi { margin-top: 34px; }
"""


def _e(t: str) -> str:
    """Escaped Text, laesst aber <code>, <b> und <br> durch."""
    t = html.escape(t)
    for auf, zu in (("&lt;code&gt;", "<code>"), ("&lt;/code&gt;", "</code>"),
                    ("&lt;b&gt;", "<b>"), ("&lt;/b&gt;", "</b>"),
                    ("&lt;i&gt;", "<i>"), ("&lt;/i&gt;", "</i>"),
                    ("&lt;br&gt;", "<br>")):
        t = t.replace(auf, zu)
    return t


# ---------------------------------------------------------------- Bausteine
def punkte(items, eng=False):
    k = " eng" if eng else ""
    return (f'<ul class="punkte{k}">'
            + "".join(f"<li>{_e(i)}</li>" for i in items) + "</ul>")


def block(titel, inhalt):
    return f'<div class="block"><h3>{_e(titel)}</h3>{inhalt}</div>'


def kasten(titel, inhalt, art=""):
    a = f" {art}" if art else ""
    return f'<div class="kasten{a}"><h3>{_e(titel)}</h3>{inhalt}</div>'


def absatz(t):
    return f"<p>{_e(t)}</p>"


def einaus(ein, aus):
    return (f'<div class="ea"><div class="tag">Ein</div>'
            f'<div class="txt">{_e(ein)}</div></div>'
            f'<div class="ea"><div class="tag aus">Aus</div>'
            f'<div class="txt">{_e(aus)}</div></div>')


def tabelle(kopf, zeilen, eng=False):
    k = ' class="eng"' if eng else ""
    th = "".join(f"<th>{_e(h)}</th>" for h in kopf)
    tr = ""
    for z in zeilen:
        tds = ""
        for i, zelle in enumerate(z):
            kl = ""
            if isinstance(zelle, tuple):
                zelle, kl = zelle
                kl = f' class="{kl}"'
            tds += f"<td{kl}>{_e(str(zelle))}</td>"
        tr += f"<tr>{tds}</tr>"
    return f"<table{k}><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table>"


def fluss(stufen):
    teile = []
    for i, s in enumerate(stufen):
        if i:
            teile.append('<div class="pfeil">&#8594;</div>')
        akt = " akt" if s.get("akt") else ""
        teile.append(
            f'<div class="stufe{akt}"><div class="st-nr">{_e(s["nr"])}</div>'
            f'<div class="st-tl">{_e(s["titel"])}</div>'
            f'<div class="st-tx">{_e(s["text"])}</div></div>')
    return f'<div class="fluss">{"".join(teile)}</div>'


def kpi(werte):
    k = "".join(f'<div class="k"><div class="w">{_e(w)}</div>'
                f'<div class="l">{_e(l)}</div></div>' for w, l in werte)
    return f'<div class="kpi">{k}</div>'


def reihe(*spalten, gewichte=None):
    st = []
    for i, s in enumerate(spalten):
        g = f' style="flex:{gewichte[i]} 1 0"' if gewichte else ""
        st.append(f'<div class="spalte"{g}>{s}</div>')
    return f'<div class="reihe">{"".join(st)}</div>'


def stapel(*teile, abstand=18):
    return f'<div style="display:flex;flex-direction:column;gap:{abstand}px">' \
           + "".join(teile) + "</div>"


# ---------------------------------------------------------------- Folien
class Deck:
    def __init__(self, schluessel, titel, unter, meta):
        self.s = schluessel
        self.t = THEMEN[schluessel]
        self.folien = []
        self.titel_folie(titel, unter, meta)

    def _rahmen(self, inhalt, klasse=""):
        self.folien.append((klasse, inhalt))

    def titel_folie(self, titel, unter, meta):
        m = "".join(f'<div><div class="l">{_e(l)}</div>'
                    f'<div class="v">{_e(v)}</div></div>' for l, v in meta)
        self._rahmen(
            f'<div class="marke">Funktionsdokumentation &middot; Teil {self.t["nr"]}</div>'
            f"<h1>{_e(titel)}</h1><div class=\"unter\">{_e(unter)}</div>"
            f'<div class="meta">{m}</div>', "titel")

    def kapitel(self, titel, unter, kennzahlen=None):
        k = kpi(kennzahlen) if kennzahlen else ""
        self._rahmen(f'<div class="marke">Datei</div>'
                     f"<h1>{_e('<code>' + titel + '</code>')}</h1>"
                     f'<div class="unter">{_e(unter)}</div>{k}', "kapitel")

    def folie(self, pfad, titel, unter=None, koerper="", label="Funktion"):
        u = f'<div class="unter">{_e(unter)}</div>' if unter else ""
        self._rahmen(
            f'<div class="kopf"><div class="pfad">{_e(pfad)}</div>'
            f"<div>{_e(label)}</div></div>"
            f'<h2>{_e(titel)}</h2>{u}<div class="koerper">{koerper}</div>')

    def html(self, deckname):
        out = []
        n = 0
        gesamt = len(self.folien)
        for klasse, inhalt in self.folien:
            n += 1
            if klasse in ("titel", "kapitel"):
                fuss = ""
            else:
                fuss = (f'<div class="fuss"><div>{_e(deckname)}</div>'
                        f'<div class="nr">{n} / {gesamt}</div></div>')
            k = f" {klasse}" if klasse else ""
            out.append(f'<section class="folie{k}">{inhalt}{fuss}</section>')
        stil = (f':root{{--akzent:{self.t["akzent"]};--hell:{self.t["hell"]};'
                f'--matt:{self.t["matt"]};}}')
        return ("<!doctype html><html lang='de'><head><meta charset='utf-8'>"
                f"<style>{CSS}{stil}</style></head><body>"
                + "".join(out) + "</body></html>")


def schreibe(deck: Deck, deckname: str, ziel: pathlib.Path):
    ziel.parent.mkdir(parents=True, exist_ok=True)
    htm = ziel.with_suffix(".html")
    htm.write_text(deck.html(deckname), encoding="utf-8")
    skript = f"""
import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        b = await p.chromium.launch()
        pg = await b.new_page(viewport={{"width": {BREITE}, "height": {HOEHE}}})
        await pg.goto("file://{htm}")
        await pg.wait_for_timeout(700)
        ueber = await pg.evaluate('''() => Array.from(
            document.querySelectorAll('.folie')).map((f,i) =>
            [i+1, f.scrollHeight]).filter(x => x[1] > {HOEHE})''')
        if ueber:
            print("UEBERLAUF:", ueber)
        await pg.pdf(path="{ziel}", width="{BREITE}px", height="{HOEHE}px",
                     print_background=True, margin={{"top":"0","bottom":"0",
                     "left":"0","right":"0"}})
        await b.close()

asyncio.run(main())
"""
    tmp = pathlib.Path("/tmp/_render.py")
    tmp.write_text(skript, encoding="utf-8")
    r = subprocess.run([sys.executable, str(tmp)], capture_output=True, text=True)
    if r.returncode:
        print(r.stderr[-2000:])
        raise SystemExit(1)
    if r.stdout.strip():
        print("  " + r.stdout.strip())
    print(f"  {ziel.name}  {len(deck.folien)} Folien  "
          f"{ziel.stat().st_size / 1024:.0f} kB")
