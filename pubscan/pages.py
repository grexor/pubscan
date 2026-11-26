"""The page (web/index.html) as the server sends it for the address of an author or a topic: with the content of
that address already in it.

The page is an application: its scripts fetch a network, a publication list or a topic list and draw them. A
search engine, a link preview or an assistant that does not run scripts would see the same empty page under every
address. So for the address of an author (/<Name>_<ORCID>) or a topic (/topic/<Name>) the server fills in what the
scripts are going to show anyway:

  head     title, description, canonical address, Open Graph fields
  heading  the <h1> of the page names the author or the topic
  topic    the list of its authors, shown at once (the scripts take it over without asking the server again)
  author   the panels of the page: publications, topics and co-authors, each with links to the pages of the
           co-authors and topics, so that the pages are connected by links that can be followed

web/index.html marks the places with comments: <!-- ssr:NAME -->default<!-- /ssr:NAME -->. Whatever is put there is
what the scripts of the page produce for the same data (the functions here mirror them, see the names given); the
values are data (names from OpenAlex and ORCID) and are escaped.
"""
import html
import json
import re
import urllib.parse

SITE_URL = "https://pubscan.org"

# English short names of the countries (ISO 3166-1 alpha-2), as the page shows them (Intl.DisplayNames there)
COUNTRIES = {
    "AD": "Andorra", "AE": "United Arab Emirates", "AF": "Afghanistan", "AG": "Antigua & Barbuda", "AL": "Albania", "AM": "Armenia", "AO": "Angola",
    "AR": "Argentina", "AT": "Austria", "AU": "Australia", "AZ": "Azerbaijan", "BA": "Bosnia & Herzegovina", "BB": "Barbados", "BD": "Bangladesh",
    "BE": "Belgium", "BF": "Burkina Faso", "BG": "Bulgaria", "BH": "Bahrain", "BI": "Burundi", "BJ": "Benin", "BN": "Brunei", "BO": "Bolivia",
    "BR": "Brazil", "BS": "Bahamas", "BT": "Bhutan", "BW": "Botswana", "BY": "Belarus", "BZ": "Belize", "CA": "Canada", "CD": "Congo - Kinshasa",
    "CF": "Central African Republic", "CG": "Congo - Brazzaville", "CH": "Switzerland", "CI": "Côte d’Ivoire", "CL": "Chile", "CM": "Cameroon",
    "CN": "China", "CO": "Colombia", "CR": "Costa Rica", "CU": "Cuba", "CV": "Cape Verde", "CY": "Cyprus", "CZ": "Czechia", "DE": "Germany",
    "DJ": "Djibouti", "DK": "Denmark", "DO": "Dominican Republic", "DZ": "Algeria", "EC": "Ecuador", "EE": "Estonia", "EG": "Egypt", "ER": "Eritrea",
    "ES": "Spain", "ET": "Ethiopia", "FI": "Finland", "FJ": "Fiji", "FO": "Faroe Islands", "FR": "France", "GA": "Gabon", "GB": "United Kingdom",
    "GD": "Grenada", "GE": "Georgia", "GF": "French Guiana", "GH": "Ghana", "GL": "Greenland", "GM": "Gambia", "GN": "Guinea", "GP": "Guadeloupe",
    "GQ": "Equatorial Guinea", "GR": "Greece", "GT": "Guatemala", "GW": "Guinea-Bissau", "GY": "Guyana", "HK": "Hong Kong SAR China", "HN": "Honduras",
    "HR": "Croatia", "HT": "Haiti", "HU": "Hungary", "ID": "Indonesia", "IE": "Ireland", "IL": "Israel", "IN": "India", "IQ": "Iraq", "IR": "Iran",
    "IS": "Iceland", "IT": "Italy", "JM": "Jamaica", "JO": "Jordan", "JP": "Japan", "KE": "Kenya", "KG": "Kyrgyzstan", "KH": "Cambodia",
    "KP": "North Korea", "KR": "South Korea", "KW": "Kuwait", "KZ": "Kazakhstan", "LA": "Laos", "LB": "Lebanon", "LI": "Liechtenstein",
    "LK": "Sri Lanka", "LR": "Liberia", "LS": "Lesotho", "LT": "Lithuania", "LU": "Luxembourg", "LV": "Latvia", "LY": "Libya", "MA": "Morocco",
    "MC": "Monaco", "MD": "Moldova", "ME": "Montenegro", "MG": "Madagascar", "MK": "North Macedonia", "ML": "Mali", "MM": "Myanmar (Burma)",
    "MN": "Mongolia", "MO": "Macao SAR China", "MQ": "Martinique", "MR": "Mauritania", "MT": "Malta", "MU": "Mauritius", "MV": "Maldives",
    "MW": "Malawi", "MX": "Mexico", "MY": "Malaysia", "MZ": "Mozambique", "NA": "Namibia", "NC": "New Caledonia", "NE": "Niger", "NG": "Nigeria",
    "NI": "Nicaragua", "NL": "Netherlands", "NO": "Norway", "NP": "Nepal", "NZ": "New Zealand", "OM": "Oman", "PA": "Panama", "PE": "Peru",
    "PF": "French Polynesia", "PG": "Papua New Guinea", "PH": "Philippines", "PK": "Pakistan", "PL": "Poland", "PR": "Puerto Rico",
    "PS": "Palestinian Territories", "PT": "Portugal", "PY": "Paraguay", "QA": "Qatar", "RE": "Réunion", "RO": "Romania", "RS": "Serbia",
    "RU": "Russia", "RW": "Rwanda", "SA": "Saudi Arabia", "SC": "Seychelles", "SD": "Sudan", "SE": "Sweden", "SG": "Singapore", "SI": "Slovenia",
    "SK": "Slovakia", "SL": "Sierra Leone", "SN": "Senegal", "SO": "Somalia", "SR": "Suriname", "SS": "South Sudan", "SV": "El Salvador",
    "SY": "Syria", "SZ": "Eswatini", "TD": "Chad", "TG": "Togo", "TH": "Thailand", "TJ": "Tajikistan", "TL": "Timor-Leste", "TM": "Turkmenistan",
    "TN": "Tunisia", "TR": "Türkiye", "TT": "Trinidad & Tobago", "TW": "Taiwan", "TZ": "Tanzania", "UA": "Ukraine", "UG": "Uganda",
    "US": "United States", "UY": "Uruguay", "UZ": "Uzbekistan", "VA": "Vatican City", "VE": "Venezuela", "VN": "Vietnam", "XK": "Kosovo",
    "YE": "Yemen", "ZA": "South Africa", "ZM": "Zambia", "ZW": "Zimbabwe",
}

SLOT = re.compile(r"<!-- ssr:(\w+) -->(.*?)<!-- /ssr:\1 -->", re.S)


def esc(value):
    """Text as HTML text or attribute value (the page's esc())."""
    return html.escape("" if value is None else str(value), quote=True)


def count(n):
    """1234567 -> "1,234,567" (the page's format_count())."""
    return f"{int(n):,}"


def url_part(text):
    """A name as part of an address: blanks become underscores, the rest as encodeURIComponent writes it."""
    return urllib.parse.quote(text.replace(" ", "_"), safe="!*'()")


def author_path(name, orcid):
    return "/" + url_part(f"{name} {orcid}")


def topic_path(name):
    return "/topic/" + url_part(name)


class Template:
    """web/index.html cut at its slots. render() puts a value into every slot it is given one for and the text of
    the file into the others; the comments that mark the slots are left out."""

    def __init__(self, text):
        self.parts = []
        self.slots = set()
        position = 0
        for match in SLOT.finditer(text):
            self.parts.append(text[position:match.start()])
            self.parts.append((match.group(1), match.group(2)))
            self.slots.add(match.group(1))
            position = match.end()
        self.parts.append(text[position:])

    def render(self, values=None):
        values = values or {}
        out = []
        for part in self.parts:
            if isinstance(part, str):
                out.append(part)
            else:
                value = values.get(part[0])
                out.append(part[1] if value is None else value)
        return "".join(out)


def with_head(text, head, index=True):
    """The page with title, description, canonical address and Open Graph fields from head ({"title",
    "description", "url"}; without a title only the canonical address is set). index False marks the page noindex
    (an address that names nobody). Only whole tags that the file is known to contain are replaced."""
    if not index:
        text = text.replace('<meta name="robots" content="index, follow">', '<meta name="robots" content="noindex, follow">', 1)
    if not head:
        return text
    e = {key: esc(value) for key, value in head.items()}
    text = text.replace("<!-- canonical -->", f'<link rel="canonical" href="{e["url"]}">', 1)
    if "title" not in e:
        return text
    for pattern, tag in ((r"<title>[^<]*</title>", f"<title>{e['title']}</title>"),
                         (r'<meta name="description" content="[^"]*">', f'<meta name="description" content="{e["description"]}">'),
                         (r'<meta property="og:title" content="[^"]*">', f'<meta property="og:title" content="{e["title"]}">'),
                         (r'<meta property="og:description" content="[^"]*">', f'<meta property="og:description" content="{e["description"]}">'),
                         (r'<meta property="og:url" content="[^"]*">', f'<meta property="og:url" content="{e["url"]}">')):
        text = re.sub(pattern, lambda m: tag, text, count=1)
    return text


def json_script(data, element_id):
    """Data for the scripts of the page, inside the page ("<" is written as an escape, so the text cannot end the element)."""
    return f'<script type="application/json" id="{element_id}">' + json.dumps(data, ensure_ascii=False).replace("<", "\\u003c") + "</script>"


def linked_data(data):
    return '<script type="application/ld+json">' + json.dumps(data, ensure_ascii=False).replace("<", "\\u003c") + "</script>"


def orcid_link(orcid):
    """orcid_link() of the page."""
    return f"<a href='https://orcid.org/{esc(orcid)}' target=_new rel='noopener'>{esc(orcid)}</a>"


def country_name(code):
    return COUNTRIES.get(code or "", code or "")


def affiliation_text(affiliation):
    """"Institution, Country" of an affiliation (name, country code, year), as affiliation_html() of the page writes it."""
    name, code, _ = affiliation
    country = country_name(code)
    return name + (", " + country if country and f"({country})" not in name else "")


def affiliation_html(affiliation):
    """affiliation_html() of the page: the line under the header of an author's publication list."""
    if not affiliation:
        return ""
    year = affiliation[2]
    return ('<div class="pub_affiliation" title="Main affiliation: the institution named on the most recent publications of the author in OpenAlex'
            + (f" ({year})" if year else "") + '"><i class="fas fa-university"></i>' + esc(affiliation_text(affiliation)) + "</div>")


def publication_link(record):
    """publication_link() of the page: the title as a link to PubMed, else to the DOI, else to the OpenAlex page."""
    title = esc(record.get("title") or "(no title)")
    work = str(record.get("id") or "")
    work = work if re.fullmatch(r"W\d+", work) else ""
    if record.get("pmid"):
        url = f"https://pubmed.ncbi.nlm.nih.gov/{int(record['pmid'])}"
    elif record.get("doi"):
        url = "https://doi.org/" + urllib.parse.quote(str(record["doi"]), safe="/!*'()")
    elif work:
        url = "https://openalex.org/" + work
    else:
        return title
    return f"<a href='{esc(url)}' target='_{work or 'blank'}' rel='noopener'>{title}</a>"


def topic_kind_label(term):
    """topic_kind_label() of the page."""
    if term["kind"] == "topic":
        return "topic in " + (term.get("subfield") or term.get("field") or "OpenAlex")
    if term["kind"] == "subfield":
        return "subfield of " + (term.get("field") or "OpenAlex")
    return "keyword"


def heading(tag, name, what):
    """The title in the header of the page (#div_graph_title), an <h1> on the page of an author or a topic: the
    author or the topic, and what the page shows of it (graph_title_html() of the page)."""
    long = ' class="gt_long"' if len(name) > 20 else ""      # on a phone a long name takes both lines of the title
    return (f'<{tag} id="div_graph_title" class="graph_title"><b{long}>{esc(name)}</b><span class="gt_colon">: </span>'
            f'<span class="gt_what">{esc(what)}</span></{tag}>')


def topic_slots(term, data, year=None):
    """The page of a topic: term is its record, data the answer of the action topic_authors ({"n_total", "authors"}).
    Mirrors render_topic_authors() of the page; year is the current year (an older newest year is shown paler)."""
    authors = data["authors"]
    summary = (count(term["n_pubs"]) + " publications, " + count(term["n_authors"]) + " authors with an ORCID iD"
               + ("; showing the top " + str(len(authors)) if len(authors) < data["n_total"] else ""))
    if authors:
        most = max(1, max(a["n"] for a in authors))
        rows = ['<table><tr><th class="num">#</th><th>Author</th><th class="topics_orcid">ORCID iD</th><th class="num">Publications</th><th class="num">Latest</th></tr>']
        for i, a in enumerate(authors, 1):
            name = a["name"] or a["orcid"]
            old = " old" if a.get("year") and year and a["year"] < year - 5 else ""
            rows.append(f'<tr class="topics_row" data-query="{esc(name + " " + a["orcid"])}"><td class="topics_rank">{i}</td>'
                        f'<td class="topics_author_cell"><a href="{esc(author_path(name, a["orcid"]))}" class="topics_author">{esc(name)}</a></td>'
                        f'<td class="topics_orcid"><a href="https://orcid.org/{esc(a["orcid"])}" target="_blank" rel="noopener" title="ORCID record">{esc(a["orcid"])}</a></td>'
                        f'<td class="num topics_pubs"><span class="topics_bar"><span style="width: {max(2, round(100 * a["n"] / most))}%"></span></span>'
                        f'<span class="topics_n">{count(a["n"])}</span></td>'
                        f'<td class="num topics_year{old}">{esc(a["year"]) if a.get("year") else ""}</td></tr>')
        results = "".join(rows) + "</table>"
    else:
        results = '<div class="topics_hint">No authors found.</div>'
    return {
        "title": heading("h1", term["name"], "top authors"),
        "h1_start": '<div class="ab_h1">Scientific co-authorship networks, explored interactively</div>',
        "splash_open": '<div id="div_splash" class="splash_div hidden">',
        "topics_open": '<div id="div_topics" class="topics_view">',
        "topics_title": esc(term["name"]) + '<span class="topics_kind">' + esc(topic_kind_label(term)) + "</span>",
        "topics_summary": summary,
        "topics_results": results,
        "data": json_script({"topic": term, "topic_authors": data}, "page_data"),
    }


def author_slots(author):
    """The page of an author. author: {"orcid", "name", "affiliation": (name, country code, year) or None,
    "n_shown": publications listed, "publications": [records of Graph.records()], "coauthors": [{"orcid", "name",
    "n": shared publications, "weight", "affiliation"}] by weight, "n_coauthors": all co-authors, "topics": [the records of the action
    author_topics], "n_topics"}. Mirrors the headers of the panels, the publication list (fetch_publications()),
    load_author_topics() and render_coauthors() of the page."""
    name, orcid = author["name"], author["orcid"]
    who = f"<b>{esc(name)}</b> " + orcid_link(orcid)

    affiliation = affiliation_html(author["affiliation"])
    publications = (f"Publications: {who} ({author['n_shown']})" + (affiliation + "<br>" if affiliation else "<br><br>")
                    + "<table border=10>"
                    + "".join(f"<tr><td>{esc(r.get('pub_year') or '')}&nbsp;&nbsp;</td><td>{publication_link(r)}</td></tr>" for r in author["publications"])
                    + "</table>")

    topics = author["topics"]
    topics_html = (f"Topics: {who} ({count(author['n_topics'])}" + (f", top {len(topics)}" if len(topics) < author["n_topics"] else "") + ")<br><br>")
    if topics:
        rows = ['<table><tr><td class="num">#</td><td style="color: #aaaaaa">term (click: who publishes on it)</td><td class="num">publications</td><td class="num">latest</td></tr>']
        for i, t in enumerate(topics, 1):
            ranked = (f' author_topic_top" title="{esc(name)} is #{t["rank"]} among the authors of this term' if t.get("rank") else "")
            rows.append(f'<tr><td class="num">{i}</td><td><a href="{esc(topic_path(t["name"]))}" class="author_topic{ranked}" data-term="{int(t["term"])}">{esc(t["name"])}</a>'
                        + (f'<span class="author_topic_rank">#{t["rank"]}</span>' if t.get("rank") else "")
                        + f'<span class="topics_kind">{esc((t.get("subfield") or "topic") if t["kind"] == "topic" else t["kind"])}</span></td>'
                        + f'<td class="num">{count(t["n"])}</td><td class="num">{esc(t["year"]) if t.get("year") else ""}</td></tr>')
        topics_html += "".join(rows) + "</table>"
    else:
        topics_html += "No topics found."

    coauthors = sorted(author["coauthors"], key=lambda c: -c["n"])      # most joint publications first; equals stay in the order of weight
    coauthors_html = (f"Co-authors: {who} (" + (f"{len(coauthors)} of {count(author['n_coauthors'])}" if len(coauthors) < author["n_coauthors"] else str(len(coauthors))) + ")"
                      + '<div class="coauthors_hint">' + COAUTHORS_HINT + "</div>")
    if coauthors:
        rows = ['<table><tr><td class="num">#</td><td style="color: #aaaaaa">co-author (click: open the network)</td><td class="num">joint publications</td><td class="num">weight</td></tr>']
        for i, c in enumerate(coauthors, 1):
            rows.append(f'<tr><td class="num">{i}</td><td><a href="{esc(author_path(c["name"], c["orcid"]))}" class="coauthor_link" data-query="{esc(c["name"] + " " + c["orcid"])}">{esc(c["name"])}</a>'
                        + (f'<div class="coauthor_aff">{esc(affiliation_text(c["affiliation"]))}</div>' if c.get("affiliation") else "")
                        + f'</td><td class="num">{count(c["n"])}</td><td class="num coauthor_weight">{c["weight"]:.2f}</td></tr>')
        coauthors_html += "".join(rows) + "</table>"
    else:
        coauthors_html += "No co-authors found."

    person = {"@context": "https://schema.org", "@type": "Person", "name": name, "url": SITE_URL + author_path(name, orcid),
              "identifier": "https://orcid.org/" + orcid, "sameAs": ["https://orcid.org/" + orcid]}
    if author["affiliation"]:
        person["affiliation"] = {"@type": "Organization", "name": author["affiliation"][0]}
    return {
        "title": heading("h1", name, "co-authorship network"),
        "h1_start": '<div class="ab_h1">Scientific co-authorship networks, explored interactively</div>',
        "splash_open": '<div id="div_splash" class="splash_div hidden">',
        "pub": publications,
        "author_topics": topics_html,
        "coauthors": coauthors_html,
        "ld": linked_data(person),
    }


# the line under the header of the co-authors panel (the same text is in web/index.html, render_coauthors())
COAUTHORS_HINT = ("The co-authors shown in the network: those with the strongest ties by co-authorship weight, where a shared publication with <i>n</i> authors counts 1/(<i>n</i>&minus;1). Click a column to sort by it, a name to open that author's network.")


def anchor(text):
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def topic_index_slots(terms):
    """The page /topics (web/topics.html): every topic and subfield of the topic table as a link to its page, by
    field and subfield. terms: the records of the topic table (name, kind, field, subfield, n_authors)."""
    fields = {}
    for term in terms:
        if term["n_authors"] <= 0:
            continue
        if term["kind"] == "subfield":
            fields.setdefault(term["field"] or "Other", {}).setdefault(term["name"], [True, []])[0] = True
        elif term["kind"] == "topic":
            fields.setdefault(term["field"] or "Other", {}).setdefault(term["subfield"] or "Other", [False, []])[1].append(term["name"])
    n_topics = sum(len(topics) for subfields in fields.values() for _, topics in subfields.values())
    n_subfields = sum(len(subfields) for subfields in fields.values())
    out, index = [], []
    for field in sorted(fields):
        subfields = fields[field]
        index.append(f'<a href="#{anchor(field)}">{esc(field)}</a>')
        out.append(f'<section class="tix_field" id="{anchor(field)}"><h2>{esc(field)}<span>{sum(len(t) for _, t in subfields.values()):,} topics</span></h2>')
        for subfield in sorted(subfields):
            has_page, topics = subfields[subfield]
            title = f'<a href="{esc(topic_path(subfield))}">{esc(subfield)}</a>' if has_page else esc(subfield)
            out.append(f'<div class="tix_sub"><h3>{title}<span>{len(topics)}</span></h3><p class="tix_links">'
                       + "".join(f'<a href="{esc(topic_path(name))}">{esc(name)}</a>' for name in sorted(topics, key=str.lower)) + "</p></div>")
        out.append("</section>")
    return {"topic_index": "\n".join(out), "topic_fields": "".join(index),
            "topic_count": f"{n_topics:,} topics in {n_subfields:,} subfields and {len(fields)} fields"}
