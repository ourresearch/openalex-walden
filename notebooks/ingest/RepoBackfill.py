# Databricks notebook source
import re
import unicodedata
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# first run
# df = (
#     spark.read
#     .parquet("s3a://openalex-ingest/parquet_output/")
# )

# save to table so we don't need to query s3 next time
# df.write.format("delta").mode("overwrite").saveAsTable("openalex.repo.repo_items_backfill")

# second run
df = spark.table("openalex.repo.repo_items_backfill")

# COMMAND ----------

clean_df = df.withColumn("cleaned_xml",
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(col("api_raw"), 
                        r'^"{3}', ''
                    ),
                    r'"{3}$', ''
                ),
                r'\\\"', '"'
            ),
            r'""', '"'
        )
    )
).drop("api_raw")

# COMMAND ----------

# udfs
def clean_html(raw_html):
    cleanr = re.compile(r'<\w+.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def remove_everything_but_alphas(input_string):
    if input_string:
        return "".join(e for e in input_string if e.isalpha())
    return ""

def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

def normalize_title(title):
    if not title:
        return ""

    if isinstance(title, bytes):
        title = str(title, 'ascii')

    text = title[0:500]

    text = text.lower()

    # handle unicode characters
    text = remove_accents(text)

    # remove HTML tags
    text = clean_html(text)

    # remove articles and common prepositions
    text = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", text)

    # remove everything except alphabetic characters
    text = remove_everything_but_alphas(text)

    return text.strip()

# Types to filter out (DELETE from CSV mapping) - lowercase normalized
TYPES_TO_DELETE = [
    "person", "image", "newspaper", "info:eu-repo/semantics/lecture", "photograph",
    "bildband", "dvd-video", "video", "fotografia", "cd", "sound recording",
    "text and image", "moving image", "photographs", "cd-rom",
    "blu-ray-disc", "stillimage", "image; text", "image;stillimage", "still image",
    "image;", "ilustraciones y fotos", "fotografie", "fotografía"
]

# Repository dc:type -> OpenAlex work type.
# Generated for oxjob #537 from the validated top-2000 repository dc:type values
# (LLM-classified vs the #535 25-type defs, adversarially validated), MERGED over the
# legacy hand-curated keys. Conference -> conference-paper/-abstract; multilingual
# article/thesis/chapter/report variants added; 'award/grant' dropped (non-canonical);
# 'posted-content' left to default 'other' (#540 sets preprint via is_preprint_repository).
# KEEP IN SYNC with the other ingest notebook (Repo.py <-> RepoBackfill.py).
# Full review + per-value CSVs: oxjobs working/improve-repository-type-crosswalk/MAPPING.md
REPO_TYPE_MAPPING = {
    # --- article  (225 keys) ---
    "article": "article",
    "text": "article",
    "article-journal": "article",
    "publishedversion": "article",
    "journal article": "article",
    "journal articles": "article",
    "departmental bulletin paper": "article",
    "text (article)": "article",
    "peerreviewed": "article",
    "journal-article": "article",
    "artículo científico": "article",
    "journal contribution": "article",
    "doc-type:article": "article",
    "期刊论文": "article",
    "artículo": "article",
    "acceptedversion": "article",
    "contributiontoperiodical": "article",
    "journalarticle": "article",
    "peer reviewed": "article",
    "research article": "article",
    "vor": "article",
    "info:ulb-repo/semantics/openurl/article": "article",
    "artigo de jornal": "article",
    "manuscript": "article",
    "artículo de revista": "article",
    "artykuł": "article",
    "artykuł w czasopiśmie": "article",
    "letter": "article",
    "article de revue": "article",
    "zeitschriftenartikel": "article",
    "<dc:type>info:eu-repo/semantics/article": "article",
    "journal/magazine article": "article",
    "original research": "article",
    "article in journal": "article",
    "submittedversion": "article",
    "folyóiratcikk": "article",
    "contribution to journal": "article",
    "journal:earticle": "article",
    "http://purl.org/eprint/type/journalarticle": "article",
    "peer-reviewed article": "article",
    "artikel": "article",
    "refereed article": "article",
    "статья": "article",
    "artículo - article": "article",
    "bulletin (article)": "article",
    "artigo avaliado pelos pares": "article",
    "論文(article)": "article",
    "articulo": "article",
    "publication - article": "article",
    "<dc:type/><dc:type>info:eu-repo/semantics/article": "article",
    "journal_article": "article",
    "paper": "article",
    "стаття": "article",
    "research paper": "article",
    "text.article.journal.peerreviewed": "article",
    "artigo": "article",
    "articles": "article",
    "book article": "article",
    "artykuł naukowy": "article",
    "artã­culo cientã­fico": "article",
    "peer-reviewed paper": "article",
    "一般雑誌記事": "article",
    "紀要論文": "article",
    "1.1 articolo in rivista": "article",
    "<dc:type/><dc:type/><dc:type>info:eu-repo/semantics/article": "article",
    "journal paper": "article",
    "researchpaper": "article",
    "cikk": "article",
    "artikel in einer zeitschrift": "article",
    "zeitschriftenbeitrag": "article",
    "http://purl.org/escidoc/metadata/ves/publication-types/article": "article",
    "article post-print": "article",
    "contributiontojournal/article": "article",
    "postprint article": "article",
    "niitype:journal article": "article",
    "article journal": "article",
    "рецензована стаття": "article",
    "статья (article)": "article",
    "article (author version)": "article",
    "article (commonwealth reporting category c)": "article",
    "articles in journals": "article",
    "artikel/aufsatz": "article",
    "article accepté pour publication ou publié": "article",
    "fi=d1 artikkeli ammattilehdessä|sv=d1 artikel i en facktidskrift|en=d1 article in a trade journal|": "article",
    "artigo de periódico": "article",
    "\\ninfo:eu-repo/semantics/article\\n": "article",
    "journal article (ja)": "article",
    "journal article (paginated)": "article",
    "fi=a1 alkuperäisartikkeli tieteellisessä aikakauslehdessä|sv=a1 originalartikel i en vetenskaplig tidskrift|en=a1 journal article (refereed), original research|": "article",
    "article de pã©riodique (journal article)": "article",
    "case report": "article",
    "research": "article",
    "článek": "article",
    "article publié dans une revue, révisé par les pairs": "article",
    "articles in periodicals and books": "article",
    "niitype:research paper": "article",
    "论文": "article",
    "article de périodique": "article",
    "trabajo de divulgación": "article",
    "journal publications": "article",
    "idegen nyelvű folyóiratközlemény külföldi lapban": "article",
    "artã­culo": "article",
    "<dc:type/><dc:type/><dc:type/><dc:type>info:eu-repo/semantics/article": "article",
    "articolo in rivista": "article",
    "wissenschaftlicher artikel": "article",
    "article, peer reviewed scientific": "article",
    "journal articles (subsidised)": "article",
    "niitype:departmental bulletin paper": "article",
    "contribucion a revista": "article",
    "postprint": "article",
    "სტატია": "article",
    "bulletin": "article",
    "series paper (non-ids)": "article",
    "学术期刊": "article",
    "article - refereed": "article",
    "artículo original": "article",
    "articulo revista indexada": "article",
    "doc-type:contributiontoperiodical": "article",
    "case study": "article",
    "d1 artikkeli ammattilehdessä": "article",
    "journal article (print/paginated)": "article",
    "text.article": "article",
    "articolo su rivista": "article",
    "magyar nyelvű folyóiratközlemény hazai lapban": "article",
    "zeitschrift": "article",
    "artículo evaluado por pares": "article",
    "artikel dosen": "article",
    "journal article (unpaginated)": "article",
    "1 contributo su rivista::1.1 articolo in rivista": "article",
    "article publié dans une revue avec comité d'évaluation": "article",
    "professional paper": "article",
    "research study": "article",
    "journal article/review": "article",
    "info:ar-repo/semantics/artículo": "article",
    "text/article": "article",
    "<dc:type>journal article.": "article",
    "article de recherche": "article",
    "article, review/survey": "article",
    "aufsatz": "article",
    "journal articles (non-subsidised)": "article",
    "article (peer-reviewed)": "article",
    "artigo de revista": "article",
    "bulletin (other)": "article",
    "http://purl.org/redcol/resource_type/art": "article",
    "tieteelliset aikakauslehtiartikkelit": "article",
    "artykuł recenzowany": "article",
    "collection article": "article",
    "1.01 articolo in rivista": "article",
    "artigo original": "article",
    "article de revue scientifique": "article",
    "journal article (on-line/unpaginated)": "article",
    "artykuł z czasopisma": "article",
    "/dk/atira/pure/researchoutput/researchoutputtypes/contributiontojournal/article": "article",
    "recenzowany artykuł": "article",
    "artigos": "article",
    "งานวิจัย": "article",
    "a1 alkuperäisartikkeli tieteellisessä aikakauslehdessä": "article",
    "論文": "article",
    "magazine-article": "article",
    "journalarticles": "article",
    "artículo de poligrafía": "article",
    "professional, non refereed article": "article",
    "niitype:article": "article",
    "media article": "article",
    "artigo avaliado por pares": "article",
    "artigo de divulgação": "article",
    "idegen nyelvű folyóiratközlemény hazai lapban": "article",
    "archaeological note": "article",
    "a1 alkuperäisartikkeli tieteellisessä aikakauslehdessä (journal article-refereed, original research)": "article",
    "czasopismo - artykuł": "article",
    "article review": "article",
    "artikel, aufsatz": "article",
    "non refereed article": "article",
    "artigos, avaliado pelos pares": "article",
    "03.1 articolo su rivista": "article",
    "article, other scientific": "article",
    "artículo / artikulua": "article",
    "text/journal article": "article",
    "ð ðµñðµð½ð·ð¾ð²ð°ð½ð° ññð°ñññ": "article",
    "essay": "article",
    "articles de revues": "article",
    "연구논문": "article",
    "non-refereed article": "article",
    "学術雑誌論文": "article",
    "artículos": "article",
    "artykuły naukowe": "article",
    "article, refereed": "article",
    "case series": "article",
    "artigo em revista científica internacional": "article",
    "artigo em outras revistas": "article",
    "artykuły": "article",
    "article, other": "article",
    "contribution to refereed journal": "article",
    "članek": "article",
    "01 pubblicazione su rivista::01a articolo in rivista": "article",
    "non-indexed article": "article",
    "рад у часопису": "article",
    "article or abstract": "article",
    "article (publisher version)": "article",
    "artigo solicitado": "article",
    "peer-revied article": "article",
    "<dc:type/><dc:type/><dc:type/><dc:type/><dc:type>info:eu-repo/semantics/article": "article",
    "fi=m1 tutkimuksesta kertova kirjoitus|sv=m1 text som beskriver forskningen|en=m1text about research activities|": "article",
    "<dc:type>journal article": "article",
    "pesquisa empírica de campo": "article",
    "b1 kirjoitus tieteellisessä aikakauslehdessä (unrefereed journal articles)s": "article",
    "artigo de pesquisa": "article",
    "text.article.journal": "article",
    "e-article": "article",
    "law paper": "article",
    "articolo": "article",
    "pesquisa teórica": "article",
    "research_results": "article",
    "pesquisa científica": "article",
    "http://purl.org/escidoc/metadata/ves/publication-types/paper": "article",
    "článek v časopise": "article",
    "实证研究": "article",
    "fi=b1 kirjoitus tieteellisessä aikakauslehdessä|sv=b1 inlägg i en vetenskaplig tidskrift|en=b1 non-refereed journal articles|": "article",
    "makale - bilimsel dergi makalesi - çok yazarlı": "article",
    "artigo de convidado": "article",
    "text.article.journal.popular": "article",
    "tieteelliset artikkelit": "article",
    "isi article": "article",
    "pesquisa histórica": "article",
    "konferenzschrift": "article",
    # --- review  (12 keys) ---
    "review": "review",
    "review article": "review",
    "doc-type:review": "review",
    "artículo de revisión": "review",
    "revisão de literatura": "review",
    "reviewarticle": "review",
    "article/review": "review",
    "pesquisa bibliográfica": "review",
    "mini review": "review",
    "article de revisió": "review",
    "type:review": "review",
    "contributiontojournal/systematicreview": "review",
    # --- conference-paper  (126 keys) ---
    "conferenceobject": "conference-paper",
    "conference paper": "conference-paper",
    "conference papers": "conference-paper",
    "conferencepaper": "conference-paper",
    "conference or workshop item": "conference-paper",
    "conference": "conference-paper",
    "conference proceedings": "conference-paper",
    "conference output": "conference-paper",
    "conference object": "conference-paper",
    "proceedings paper": "conference-paper",
    "conference_paper": "conference-paper",
    "objeto de conferencia": "conference-paper",
    "conference item": "conference-paper",
    "konferenzbeitrag": "conference-paper",
    "conference proceeding": "conference-paper",
    "doc-type:conferenceobject": "conference-paper",
    "conference contribution": "conference-paper",
    "comunicación de congreso": "conference-paper",
    "proceedings": "conference-paper",
    "inproceedings": "conference-paper",
    "conference_item": "conference-paper",
    "conference-paper": "conference-paper",
    "会议论文": "conference-paper",
    "k - conference": "conference-paper",
    "http://purl.org/eprint/type/conferencepaper": "conference-paper",
    "communication de conférence": "conference-paper",
    "communication dans un congrès avec actes": "conference-paper",
    "info:ulb-repo/semantics/openurl/proceeding": "conference-paper",
    "conference lecture": "conference-paper",
    "konferenzveröffentlichung": "conference-paper",
    "text.article.conference.peerreviewed": "conference-paper",
    "conferenceitemnotinproceedings": "conference-paper",
    "contribution to conference": "conference-paper",
    "conference_object": "conference-paper",
    "presentation / conference contribution": "conference-paper",
    "<dc:type>conference contribution": "conference-paper",
    "conference paper, poster, etc.": "conference-paper",
    "conference publication": "conference-paper",
    "conference report": "conference-paper",
    "4.1 contributo in atti di convegno": "conference-paper",
    "conference presentation": "conference-paper",
    "conference article": "conference-paper",
    "konferenční příspěvek": "conference-paper",
    "conference document": "conference-paper",
    "comunicacion": "conference-paper",
    "conference proceeding article": "conference-paper",
    "documento de conferencia": "conference-paper",
    "compte rendu de conférence": "conference-paper",
    "refereed conference paper": "conference-paper",
    "статьи в сборниках": "conference-paper",
    "proceeding": "conference-paper",
    "conference or workshop item (commonwealth reporting category e)": "conference-paper",
    "communication / conférence": "conference-paper",
    "publikacja pokonferencyjna": "conference-paper",
    "communication dans un congrès": "conference-paper",
    "contributions to conferences": "conference-paper",
    "trabalho completo publicado em evento": "conference-paper",
    "conference materials": "conference-paper",
    "conference paper/proceeding/abstract": "conference-paper",
    "conferenceproceedings": "conference-paper",
    "conference or workshop contribution": "conference-paper",
    "conference paper not in proceedings": "conference-paper",
    "text.article.conference": "conference-paper",
    "proceeding_paper": "conference-paper",
    "доклад на конференции или семинаре": "conference-paper",
    "\\ninfo:eu-repo/semantics/conferenceobject\\n": "conference-paper",
    "actes de colloque": "conference-paper",
    "presentation / conference": "conference-paper",
    "meetings and proceedings": "conference-paper",
    "proceedings international": "conference-paper",
    "conferenceitem": "conference-paper",
    "conference-item": "conference-paper",
    "text:in_proceedings": "conference-paper",
    "conference contributions - published": "conference-paper",
    "non refereed conference paper": "conference-paper",
    "actas de congreso": "conference-paper",
    "fi=b3 vertaisarvioimaton artikkeli konferenssijulkaisussa|sv=b3 icke-referentgranskad artikel i konferenspublikation|en=b3 non-refereed conference proceedings|": "conference-paper",
    "intervento a convegno": "conference-paper",
    "publication - conference item": "conference-paper",
    "conference or conference paper": "conference-paper",
    "conferencepresentation": "conference-paper",
    "conference contribution - published": "conference-paper",
    "conference or workshop items": "conference-paper",
    "conference meeting part": "conference-paper",
    "documento relativo ad un convegno o altro evento": "conference-paper",
    "veranstaltungsbeitrag": "conference-paper",
    "konferenz- oder workshop-beitrag": "conference-paper",
    "comunicació de congrés": "conference-paper",
    "conferencecontribution": "conference-paper",
    "conferenceproceeding": "conference-paper",
    "proceedings national": "conference-paper",
    "<dc:type>conference paper.": "conference-paper",
    "рад у зборнику": "conference-paper",
    "document issu d'une conférence ou d'un atelier": "conference-paper",
    "conference material": "conference-paper",
    "article_in_conference_proceedings": "conference-paper",
    "veranstaltungsbeitrag (unveröffentlicht)": "conference-paper",
    "article; proceedings paper": "conference-paper",
    "contributo in atti di convegno": "conference-paper",
    "contribució a congrés": "conference-paper",
    "academic conference": "conference-paper",
    "conference contribution - unpublished": "conference-paper",
    "conference meeting-part": "conference-paper",
    "symposium": "conference-paper",
    "matériel de conférence": "conference-paper",
    "доповідь на конференції чи семінарі": "conference-paper",
    "contributiontoconference/paper": "conference-paper",
    "squeeze paper": "conference-paper",
    "conference papers and proceedings": "conference-paper",
    "conference/workshop item": "conference-paper",
    "conferencia": "conference-paper",
    "conference papers, meetings and proceedings": "conference-paper",
    "tam metin bildiri": "conference-paper",
    "ponencia": "conference-paper",
    "trabalhos em eventos": "conference-paper",
    "trabalho apresentado em evento": "conference-paper",
    "trabalho apresentado em evento": "conference-paper",
    "kokousjulkaisut": "conference-paper",
    "conf": "conference-paper",
    "comunicación": "conference-paper",
    "conference, symposium or workshop item": "conference-paper",
    "conference article (ca)": "conference-paper",
    "paper proceeding": "conference-paper",
    "workshop": "conference-paper",
    "conference-meeting part": "conference-paper",
    "artículo de conferencia": "conference-paper",
    # --- conference-abstract  (37 keys) ---
    "resumo publicado em evento": "conference-abstract",
    "poster": "conference-abstract",
    "conferenceposter": "conference-abstract",
    "conference abstract": "conference-abstract",
    "abstract": "conference-abstract",
    "conference poster": "conference-abstract",
    "conference talk": "conference-abstract",
    "szöveges plakát": "conference-abstract",
    "póster de congreso": "conference-abstract",
    "conference extract": "conference-abstract",
    "resumen": "conference-abstract",
    "carteles": "conference-abstract",
    "meeting abstract": "conference-abstract",
    "fi=m2 esitelmä tai posteri|sv=m2 presentation|en=m2 presentation or poster|": "conference-abstract",
    "póster": "conference-abstract",
    "plakat": "conference-abstract",
    "apresentação oral": "conference-abstract",
    "conference_abstract": "conference-abstract",
    "posters": "conference-abstract",
    "4.2 abstract in atti di convegno": "conference-abstract",
    "conference poster not in proceedings": "conference-abstract",
    "plakaty": "conference-abstract",
    "előadáskivonat": "conference-abstract",
    "autre communication scientifique (congrès sans actes - poster - séminaire...)": "conference-abstract",
    "abst": "conference-abstract",
    "präsentation auf konferenz": "conference-abstract",
    "poster presentation": "conference-abstract",
    "still image; poster": "conference-abstract",
    "affisch": "conference-abstract",
    "演示报告": "conference-abstract",
    "afisz/plakat": "conference-abstract",
    "1.5 abstract in rivista": "conference-abstract",
    "özet bildiri": "conference-abstract",
    "text.article.conference.poster": "conference-abstract",
    "тези доповідей": "conference-abstract",
    "conference meeting abstract": "conference-abstract",
    "conference-presentation": "conference-abstract",
    # --- preprint  (12 keys) ---
    "preprint": "preprint",
    "preprints, working papers, ...": "preprint",
    "doc-type:preprint": "preprint",
    "\\ninfo:eu-repo/semantics/preprint\\n": "preprint",
    "post-print": "preprint",
    "ao": "preprint",
    "pre-print": "preprint",
    "text.preprint": "preprint",
    "manuscript (preprint)": "preprint",
    "/dk/atira/pure/researchoutput/researchoutputtypes/workingpaper/preprint": "preprint",
    "mims preprint": "preprint",
    # --- book  (72 keys) ---
    "book": "book",
    "libros": "book",
    "monograph": "book",
    "books": "book",
    "książka": "book",
    "starodruk": "book",
    "volume": "book",
    "doc-type:book": "book",
    "libro": "book",
    "\\n        monograph\\n": "book",
    "info:ulb-repo/semantics/openurl/book": "book",
    "stary druk": "book",
    "printed monograph": "book",
    "buch": "book",
    "book:ebook": "book",
    "book (monograph)": "book",
    "e-książka": "book",
    "book item": "book",
    "livre": "book",
    "ebook": "book",
    "könyv": "book",
    "edited book": "book",
    "edited scientific work": "book",
    "livro": "book",
    "book editorial": "book",
    "programmed text book": "book",
    "ouvrage": "book",
    "multivolume_work": "book",
    "专著": "book",
    "llibre": "book",
    "monographie": "book",
    "3.1 monografia o trattato scientifico": "book",
    "collectededition": "book",
    "книга, монография (book)": "book",
    "sammelwerk": "book",
    "livros": "book",
    "libro - book": "book",
    "monografie": "book",
    "direction d'ouvrage": "book",
    "kirja": "book",
    "anthology": "book",
    "info:ar-repo/semantics/libro": "book",
    "ancientbook": "book",
    "წიგნი": "book",
    "bookanthology/report": "book",
    "книга": "book",
    "textbook": "book",
    "مؤلَّف محكَّم": "book",
    "authored book": "book",
    "authored books": "book",
    "monograph or book": "book",
    "\\n            \\n                book\\n            \\n": "book",
    "schoolbook": "book",
    "buku": "book",
    "buch / sammelwerk": "book",
    "монография": "book",
    "inkunabuł": "book",
    "fi=d6 toimitettu ammatillinen teos|sv=d6 redigerat yrkesinriktat verk|en= d6 edited professional book|": "book",
    "buch / monografie": "book",
    "edited book or journal volume": "book",
    "βιβλίο": "book",
    "publication - book": "book",
    "\\n        multivolumework\\n": "book",
    "book or monograph": "book",
    "図書": "book",
    "book, whole": "book",
    "编著": "book",
    "fi=d5 ammatillinen kirja|sv=d5 yrkesinriktad bok|en=d5 textbook, professional manual or guide|": "book",
    "text/book": "book",
    "підручник/посібник": "book",
    "book/monograph/conference proceedings": "book",
    "monografische reihe": "book",
    # --- book-chapter  (78 keys) ---
    "bookpart": "book-chapter",
    "book sections": "book-chapter",
    "chapter": "book-chapter",
    "book chapter": "book-chapter",
    "book section": "book-chapter",
    "bookchapter": "book-chapter",
    "book part": "book-chapter",
    "artykuł (rozdział w książce)": "book-chapter",
    "part of book or chapter of book": "book-chapter",
    "doc-type:bookpart": "book-chapter",
    "info:ulb-repo/semantics/openurl/bookitem": "book-chapter",
    "capítulo de libro": "book-chapter",
    "inbook": "book-chapter",
    "chapter in book, report or conference volume": "book-chapter",
    "bookitem": "book-chapter",
    "könyvrészlet": "book-chapter",
    "book_part": "book-chapter",
    "incollection": "book-chapter",
    "chapitre d'ouvrage": "book-chapter",
    "materiale didattico / articolo o estratto da libro": "book-chapter",
    "book_chapter": "book-chapter",
    "chapter, part of book": "book-chapter",
    "2.1 contributo in volume (capitolo o saggio)": "book-chapter",
    "contribution in book/report/proceedings": "book-chapter",
    "book_section": "book-chapter",
    "aufsatz in einem buch": "book-chapter",
    "book-chapter": "book-chapter",
    "buchkapitel": "book-chapter",
    "sammelwerksbeitrag": "book-chapter",
    "book_contribution": "book-chapter",
    "book or report section": "book-chapter",
    "chapter in book": "book-chapter",
    "part of book": "book-chapter",
    "könyv része": "book-chapter",
    "capítol de llibre": "book-chapter",
    "info:ar-repo/semantics/parte de libro": "book-chapter",
    "capitulo de libro": "book-chapter",
    "buchaufsatz/kapitel": "book-chapter",
    "book chapters": "book-chapter",
    "research book chapter": "book-chapter",
    "text:in_monograph": "book-chapter",
    "secciã³n de libro": "book-chapter",
    "publication - book section": "book-chapter",
    "buchkapitel / sammelwerksbeitrag": "book-chapter",
    "fi=d2 artikkeli ammatillisessa kokoomateoksessa (ml. toimittajan kirjoittama johdantoartikkeli)|sv=d2 artikel i ett yrkesinriktat samlingsverk (inkl. inledningsartikel som skrivits av redaktören)|en=d2 article in a professional book (incl. an introduction by the editor)|": "book-chapter",
    "book series article/chapter": "book-chapter",
    "book part or chapter": "book-chapter",
    "rozdział w książce": "book-chapter",
    "http://purl.org/eprint/type/bookitem": "book-chapter",
    "chapters in books": "book-chapter",
    "chapitre de livre": "book-chapter",
    "sección de libro": "book-chapter",
    "2 contributo in volume::2.1 contributo in volume (capitolo o saggio)": "book-chapter",
    "contributiontobookanthology/chapter": "book-chapter",
    "book chapter or section": "book-chapter",
    "book, section": "book-chapter",
    "专著章节/文集论文": "book-chapter",
    "booksection": "book-chapter",
    "beitrag im sammelband": "book-chapter",
    "beitrag in einem lehr- oder fachbuch": "book-chapter",
    "contributiontobookanthology/conference": "book-chapter",
    "contribution to book": "book-chapter",
    "rozdział z książki": "book-chapter",
    "sección o parte de un documento": "book-chapter",
    "book chapter (commonwealth reporting category b)": "book-chapter",
    "text.monograph.chapter": "book-chapter",
    "<dc:type>chapter": "book-chapter",
    "anthologyarticle": "book-chapter",
    "capítulo de livro": "book-chapter",
    "book section / proceedings": "book-chapter",
    "other book chapter": "book-chapter",
    "könyvfejezet": "book-chapter",
    "contribution ã  ouvrage collectif (book chapter)": "book-chapter",
    "\\ninfo:eu-repo/semantics/bookpart\\n": "book-chapter",
    "book_chap": "book-chapter",
    "articles in books": "book-chapter",
    "capítulo": "book-chapter",
    "book:ebooksection": "book-chapter",
    # --- book-review  (13 keys) ---
    "bookreview": "book-review",
    "book review": "book-review",
    "ressenya": "book-review",
    "rezension": "book-review",
    "ouvragerecu": "book-review",
    "notecritique": "book-review",
    "reseã±a": "book-review",
    "1.2 recensione in rivista": "book-review",
    "reseña libro": "book-review",
    "reseña de libro": "book-review",
    "book reviews": "book-review",
    "review single work": "book-review",
    "resenha": "book-review",
    # --- reference-entry  (6 keys) ---
    "linguistic type: lexicon": "reference-entry",
    "reference": "reference-entry",
    "reference material": "reference-entry",
    "2.4 voce (in dizionario o enciclopedia)": "reference-entry",
    "encyclopedia_article": "reference-entry",
    "referenceentry": "reference-entry",
    # --- dissertation  (349 keys) ---
    "thesis": "dissertation",
    "masterthesis": "dissertation",
    "bachelorthesis": "dissertation",
    "doctoralthesis": "dissertation",
    "master thesis": "dissertation",
    "u - thesis": "dissertation",
    "doctoral thesis": "dissertation",
    "dissertation": "dissertation",
    "theses": "dissertation",
    "student thesis": "dissertation",
    "electronic thesis or dissertation": "dissertation",
    "fi=amk-opinnäytetyö|sv=yh-examensarbete|en=bachelor's thesis|": "dissertation",
    "thesis or dissertation": "dissertation",
    "bakalářská práce": "dissertation",
    "bachelor thesis": "dissertation",
    "diplomová práce": "dissertation",
    "tesis": "dissertation",
    "skripsi": "dissertation",
    "doc-type:doctoralthesis": "dissertation",
    "thesis/dissertation": "dissertation",
    "master": "dissertation",
    "phd thesis": "dissertation",
    "etd": "dissertation",
    "学位论文": "dissertation",
    "g2 pro gradu, diplomityö": "dissertation",
    "final year project (fyp)": "dissertation",
    "master's thesis": "dissertation",
    "trabalho de conclusão de graduação": "dissertation",
    "licenciate": "dissertation",
    "fi=ylempi amk-opinnäytetyö|sv=högre yh-examensarbete|en=master's thesis|": "dissertation",
    "dissertação": "dissertation",
    "phd doctor of philosophy": "dissertation",
    "pg_thesis": "dissertation",
    "skripsi tesis atau disertasi": "dissertation",
    "info:ulb-repo/semantics/openurl/vlink-dissertation": "dissertation",
    "studentthesis": "dissertation",
    "fi=pro gradu -tutkielma | en=master's thesis|": "dissertation",
    "yüksek lisans": "dissertation",
    "trabajo de grado - maestría": "dissertation",
    "undergraduate thesis": "dissertation",
    "phd": "dissertation",
    "tugas akhir,skripsi,tesis,disertasi": "dissertation",
    "dissertationen": "dissertation",
    "masters thesis": "dissertation",
    "dissertação (mestrado)": "dissertation",
    "thèse ou mémoire / thesis or dissertation": "dissertation",
    "doctoral thesis, comprehensive summary": "dissertation",
    "fi=pro gradu -tutkielma|en=master's thesis|": "dissertation",
    "tesis/trabajos de grado - thesis": "dissertation",
    "doctor of philosophy": "dissertation",
    "doc-type:masterthesis": "dissertation",
    "doctor of philosophy (phd)": "dissertation",
    "dissertation or thesis": "dissertation",
    "tccgrad": "dissertation",
    "g1 kandidaatintyö": "dissertation",
    "niitype:thesis or dissertation": "dissertation",
    "doc-type:phdthesis": "dissertation",
    "硕士": "dissertation",
    "rozprawa doktorska": "dissertation",
    "thèse": "dissertation",
    "trabajo de grado - pregrado": "dissertation",
    "dissertation (older thesis)": "dissertation",
    "theses / dissertations": "dissertation",
    "thesis (phd)": "dissertation",
    "diplomado de profundización para grado": "dissertation",
    "diplomas": "dissertation",
    "diploma thesis": "dissertation",
    "thesis (ph.d.)": "dissertation",
    "tese (doutorado)": "dissertation",
    "thesis-reproduction (electronic)": "dissertation",
    "tesi di dottorato": "dissertation",
    "ph.d. thesis": "dissertation",
    "dissertation-reproduction (electronic)": "dissertation",
    "monografia especialização digital": "dissertation",
    "mémoire": "dissertation",
    "phdthesis": "dissertation",
    "dissertação de mestrado": "dissertation",
    "tese": "dissertation",
    "diplomityö": "dissertation",
    "magisterská práce": "dissertation",
    "tesis de maestría": "dissertation",
    "text.thesis.masters.h": "dissertation",
    "mémoire de maîtrise": "dissertation",
    "dissertation (open access)": "dissertation",
    "dissertation/thesis": "dissertation",
    "tesi doctoral": "dissertation",
    "kandidaatintyö": "dissertation",
    "electronic dissertation": "dissertation",
    "thèses de doctorat": "dissertation",
    "master thesis (pre-bologna period)": "dissertation",
    "bachelous paper": "dissertation",
    "doctoral_thesis": "dissertation",
    "disszertáció": "dissertation",
    "final year project report": "dissertation",
    "bachelor_thesis": "dissertation",
    "trabalho de conclusão de curso": "dissertation",
    "dissertation (university of nottingham only)": "dissertation",
    "doc-type:bachelorthesis": "dissertation",
    "trabalho de conclusão de especialização": "dissertation",
    "mémoire accepté": "dissertation",
    "doctoral dissertation": "dissertation",
    "proyecto fin de carrera": "dissertation",
    "academic thesis": "dissertation",
    "tesis i dissertacions electròniques": "dissertation",
    "hochschulschrift": "dissertation",
    "accreditation to supervise research": "dissertation",
    "fi=pro gradu - tutkielma |en=master's thesis|sv=pro gradu -avhandling|": "dissertation",
    "treball final de grau": "dissertation",
    "fi=diplomityö|en=master's thesis|": "dissertation",
    "mémoire ou thèse": "dissertation",
    "monografia graduação digital": "dissertation",
    "thèse de doctorat": "dissertation",
    "phd-thesis": "dissertation",
    "tese de doutorado": "dissertation",
    "tesis de grado": "dissertation",
    "disertační práce": "dissertation",
    "tesis de licenciatura": "dissertation",
    "doktora": "dissertation",
    "undergraduates project papers": "dissertation",
    "electronic thesis": "dissertation",
    "fi= diplomityö | en=master’s thesis (technology)|": "dissertation",
    "delo ali doktorska disertacija": "dissertation",
    "thèse ou mémoire numérique / electronic thesis or dissertation": "dissertation",
    "博士": "dissertation",
    "thesis (university of nottingham only)": "dissertation",
    "theses (doctoral - abstract and summary of review)": "dissertation",
    "fi=kandidaatintutkielma|en=bachelor's thesis|": "dissertation",
    "griffith thesis": "dissertation",
    "theses (doctoral)": "dissertation",
    "doctoral thesis, monograph": "dissertation",
    "trabajo fin de grado/gradu amaierako lana": "dissertation",
    "dysertacja": "dissertation",
    "md doctor of medicine": "dissertation",
    "doctoral": "dissertation",
    "mini dissertation": "dissertation",
    "specializationthesis": "dissertation",
    "thesis (open access)": "dissertation",
    "wissenschaftliche abschlussarbeiten » dissertation": "dissertation",
    "tesis/trabajo de grado - monografía - pregrado": "dissertation",
    "senior project": "dissertation",
    "diplomityö - master's thesis": "dissertation",
    "undergraduate senior honors thesis.": "dissertation",
    "tesi (dottorato)": "dissertation",
    "maisterin opinnäyte": "dissertation",
    "fi=kandidaatintyö|en=bachelor's thesis|": "dissertation",
    "avhandling pro gradu": "dissertation",
    "tesis de doctorado": "dissertation",
    "undergraduate honors thesis": "dissertation",
    "tesis de master": "dissertation",
    "博士論文 (doctoral dissertation)": "dissertation",
    "doktorarbeit": "dissertation",
    "väitöskirja": "dissertation",
    "memòria": "dissertation",
    "tesis doctorado": "dissertation",
    "trabajo de suficiencia profesional": "dissertation",
    "licentiate thesis": "dissertation",
    "thesis-doctor of philosophy": "dissertation",
    "masters research thesis": "dissertation",
    "bachelor's": "dissertation",
    "fi=pro gradu -tutkielma|en=master's thesis|sv=pro gradu -avhandling|": "dissertation",
    "skripsi sarjana": "dissertation",
    "text.thesis.doctoral": "dissertation",
    "thã¨se ou mã©moire numã©rique / electronic thesis or dissertation": "dissertation",
    "doktorat, praca dyplomowa": "dissertation",
    "tesis doctoral": "dissertation",
    "final year project": "dissertation",
    "text.thesis.licentiate": "dissertation",
    "thesis.doctoral": "dissertation",
    "master_thesis": "dissertation",
    "e-thesis": "dissertation",
    "pro gradu": "dissertation",
    "visokošolsko delo": "dissertation",
    "tcc graduação digital": "dissertation",
    "tcc especialização digital": "dissertation",
    "\\n        doctoral thesis\\n": "dissertation",
    "tesis magíster": "dissertation",
    "undergraduate final year project report": "dissertation",
    "phd/doctoral dissertation": "dissertation",
    "diplom": "dissertation",
    "graduating extended essay / research project": "dissertation",
    "วิทยานิพนธ์": "dissertation",
    "specialistthesis": "dissertation",
    "habilitation": "dissertation",
    "bachelor's thesis": "dissertation",
    "monografia graduação": "dissertation",
    "thesis-master by coursework": "dissertation",
    "dizertační práce": "dissertation",
    "thesis doctoral": "dissertation",
    "licentiate thesis, comprehensive summary": "dissertation",
    "thèse ou essai doctoral accepté": "dissertation",
    "thèse ou mémoire de l'uqac": "dissertation",
    "yamk": "dissertation",
    "capstone": "dissertation",
    "fi=artikkeliväitöskirja|en=doctoral dissertation (article-based)|": "dissertation",
    "thèse et mémoire": "dissertation",
    "master's": "dissertation",
    "trabajo de grado": "dissertation",
    "licentiatethesis": "dissertation",
    "research paper (m.a.), 4 hrs.": "dissertation",
    "masters": "dissertation",
    "fi= kandidaatintyö | en=bachelor's thesis|": "dissertation",
    "praca doktorska": "dissertation",
    "trabalho de conclusão de curso de graduação": "dissertation",
    "выпускная бакалаврская работа": "dissertation",
    "tcc": "dissertation",
    "trabajo de grado - especialización": "dissertation",
    "tesis/trabajo de grado - monografía - especialización": "dissertation",
    "doktorat": "dissertation",
    "g5 artikkeliväitöskirja": "dissertation",
    "treball de fi de postgrau": "dissertation",
    "laurea triennale": "dissertation",
    "skripsi,tesis,disertasi": "dissertation",
    "proyecto aplicado o tesis": "dissertation",
    "diplomamunka": "dissertation",
    "magistrali biennali": "dissertation",
    "väitöskirja (artikkeli)": "dissertation",
    "doctoral_dissertation": "dissertation",
    "trabajo de grado, licenciatura": "dissertation",
    "undergraduate senior honors thesis": "dissertation",
    "thã¨se ou mã©moire de l'uqac": "dissertation",
    "trabajo de grado - doctorado": "dissertation",
    "tıpta uzmanlık": "dissertation",
    "theses (doctoral - abstract of entire text)": "dissertation",
    "学士": "dissertation",
    "laurea vecchio ordinamento": "dissertation",
    "fi=syventävä työ | en=master's thesis (medicine)|": "dissertation",
    "pro gradu-tutkielma": "dissertation",
    "diss.(doctoral)": "dissertation",
    "phd doctorate": "dissertation",
    "habilitation à diriger des recherches": "dissertation",
    "graduating project": "dissertation",
    "fi= pro gradu -tutkielma | en=master's thesis|": "dissertation",
    "tcc (graduação)": "dissertation",
    "bachelor": "dissertation",
    "fi=syventävien opintojen kirjallinen työ|en=second cycle degree thesis|": "dissertation",
    "\\ninfo:eu-repo/semantics/doctoralthesis\\n": "dissertation",
    "text/thesis": "dissertation",
    "thesis (masters)": "dissertation",
    "msc master of science": "dissertation",
    "trabajo de integración curricular": "dissertation",
    "tesis (pre-grado)": "dissertation",
    "thèse d’exercice": "dissertation",
    "honors thesis": "dissertation",
    "tesis de especialidad": "dissertation",
    "thesis (dr.)": "dissertation",
    "tesis i dissertacions electròniques.": "dissertation",
    "second cycle, a2e": "dissertation",
    "masters' project": "dissertation",
    "\\n                                info:eu-repo/semantics/doctoralthesis\\n": "dissertation",
    "masters by research": "dissertation",
    "fi=väitöskirja | en=doctoral dissertation|": "dissertation",
    "master in computer science": "dissertation",
    "ph.d.": "dissertation",
    "info:eu-repo/bachelorthesis": "dissertation",
    "trabajo fin de máster/master amaierako lana": "dissertation",
    "text.thesis.bachelor.m": "dissertation",
    "research paper (m.a.), 3 hrs.": "dissertation",
    "undergraduate 5th year college of architecture and planning thesis.": "dissertation",
    "monografia especialização": "dissertation",
    "m.s.": "dissertation",
    "first cycle, g2e": "dissertation",
    "thèse ou mémoire": "dissertation",
    "honours thesis": "dissertation",
    "licentiate thesis, monograph": "dissertation",
    "text.thesis.bachelor.m2": "dissertation",
    "g4 monografiaväitöskirja": "dissertation",
    "masters coursework thesis": "dissertation",
    "baccalaureus work - undergraduate programme": "dissertation",
    "thesis (phd/research)": "dissertation",
    "ug_thesis": "dissertation",
    "tesis magister": "dissertation",
    "projecte/treball final de carrera": "dissertation",
    "fi=väitöskirja|en=doctoral dissertation|": "dissertation",
    "tesis doctoral (dr.)": "dissertation",
    "thesis abstract": "dissertation",
    "master of philosophy": "dissertation",
    "mémoire de master": "dissertation",
    "thesis / dissertation": "dissertation",
    "master thesis (research)": "dissertation",
    "thèse d'exercice": "dissertation",
    "trabajo de especializacion": "dissertation",
    "autoreferat": "dissertation",
    "tanulmány, értekezés": "dissertation",
    "master in management": "dissertation",
    "campusdissertation": "dissertation",
    "text (thesis)": "dissertation",
    "mémoire iufm": "dissertation",
    "trabajo final de grado": "dissertation",
    "master's thesis - graduate programme": "dissertation",
    "creative component": "dissertation",
    "doctor of sciences": "dissertation",
    "research master thesis": "dissertation",
    "bachelorarbeit": "dissertation",
    "phd dissertation": "dissertation",
    "student_research": "dissertation",
    "értekezés/disszertáció": "dissertation",
    "магистерская диссертация": "dissertation",
    "fi=artikkeliväitöskirja | en=doctoral dissertation (article-based) |": "dissertation",
    "thesis - masters": "dissertation",
    "doctor of philosophy (ph.d.)": "dissertation",
    "diploma di laurea vecchio ordinamento": "dissertation",
    "tesis de maestria": "dissertation",
    "maisterin opinnäytetyö": "dissertation",
    "tesina de grado": "dissertation",
    "academic exercise": "dissertation",
    "diploma_thesis": "dissertation",
    "physicsthesis": "dissertation",
    "学位論文(thesis)": "dissertation",
    "doctoralthesisexposure": "dissertation",
    "masterprogrammethesis": "dissertation",
    "theses and dissertations": "dissertation",
    "thèse, mémoire ou essai": "dissertation",
    "cumulativethesis": "dissertation",
    "diplom- oder magisterarbeit": "dissertation",
    "dissertations": "dissertation",
    "doctoral thesis (article-based)": "dissertation",
    "μεταπτυχιακή εργασία": "dissertation",
    "szakdolgozat/diplomamunka": "dissertation",
    "tesis pre-grado": "dissertation",
    "tcc graduação": "dissertation",
    "thesis-master by research": "dissertation",
    "学位論文": "dissertation",
    "diploma/tugas akhir": "dissertation",
    "master's report": "dissertation",
    "capstone project": "dissertation",
    "academic theses": "dissertation",
    "tcc especialização": "dissertation",
    "masters degree": "dissertation",
    "fi=lisensiaatintyö | en=licentiate thesis|": "dissertation",
    "diss.": "dissertation",
    "habilitační práce": "dissertation",
    "дипломний проект": "dissertation",
    "dissertation (campus access only)": "dissertation",
    "tez": "dissertation",
    "http://purl.org/eprint/type/thesis": "dissertation",
    "tesis maestría": "dissertation",
    "treball de recerca": "dissertation",
    "ma master of arts": "dissertation",
    "master thesis\\n": "dissertation",
    "book_phd": "dissertation",
    "master in biology": "dissertation",
    "докторска дисертација": "dissertation",
    "studythesis": "dissertation",
    "fi=diplomityö|en=master's thesis (m.sc. (tech.))|sv=diplomarbete|": "dissertation",
    "honours": "dissertation",
    "monografia, dissertação, tese": "dissertation",
    "master in economics": "dissertation",
    "masterarbeit": "dissertation",
    "masters paper": "dissertation",
    # --- report  (120 keys) ---
    "report": "report",
    "research report": "report",
    "working paper": "report",
    "technical report": "report",
    "r - report": "report",
    "doc-type:workingpaper": "report",
    "workingpaper": "report",
    "reports": "report",
    "научный доклад (working paper)": "report",
    "doc-type:report": "report",
    "mpra paper": "report",
    "external research report": "report",
    "y - progress report": "report",
    "departmental report": "report",
    "berichtsreihe": "report",
    "rapport": "report",
    "arbeitspapier": "report",
    "documento de trabajo": "report",
    "informe": "report",
    "informe final": "report",
    "publication - report": "report",
    "eur - scientific and technical research reports": "report",
    "technicaldocumentation": "report",
    "technical reports": "report",
    "policy report": "report",
    "report or paper": "report",
    "publication gouvernementale ou paragouvernementale": "report",
    "working/technical paper": "report",
    "internship report": "report",
    "info:ulb-repo/semantics/openurl/report": "report",
    "onderzoeksrapport": "report",
    "informe técnico": "report",
    "publications &amp; research :: policy research working paper": "report",
    "laporan kkn": "report",
    "opracowanie statystyczne": "report",
    "fi=d4 julkaistu kehittämis- tai tutkimusraportti taikka -selvitys|sv=d4 publicerad utvecklings- eller forskningsrapport eller -utredning|en=d4 published development or research report or study|": "report",
    "book/report": "report",
    "info:ulb-repo/semantics/openurl/vlink-workingpaper": "report",
    "working / discussion paper": "report",
    "internal report": "report",
    "discussion paper": "report",
    "internal document": "report",
    "기술용역보고서": "report",
    "comunicado técnico (infoteca-e)": "report",
    "government publication": "report",
    "text.techreport": "report",
    "rapport de recherche": "report",
    "annual report": "report",
    "student project report": "report",
    "eu european parliament document": "report",
    "research paper or report": "report",
    "研究報告書": "report",
    "forschungsbericht": "report",
    "sprawozdanie szkolne xix-xx w.": "report",
    "book/report/proceedings": "report",
    "document de travail - pré-publication": "report",
    "technical note": "report",
    "policy document": "report",
    "documento tecnico": "report",
    "project report": "report",
    "학술용역보고서": "report",
    "relatório de estágio": "report",
    "contract report": "report",
    "publications &amp; research :: working paper": "report",
    "techreport": "report",
    "working or discussion paper": "report",
    "eu commission - com document": "report",
    "niitype:technical report": "report",
    "documento de trabajo - monograph": "report",
    "memorandum": "report",
    "project deliverable": "report",
    "student report": "report",
    "eu commission - working document": "report",
    "sprawozdanie": "report",
    "report or working paper": "report",
    "laporan kp": "report",
    "departmental technical report": "report",
    "document de travail / working paper": "report",
    "factsheet": "report",
    "boletim de pesquisa e desenvolvimento (infoteca-e)": "report",
    "reporte": "report",
    "sprawozdania": "report",
    "fi=d4 julkaistu kehittämis- tai tutkimusraportti taikka selvitys|sv=d4 publicerad utvecklings eller forskningsrapport eller -utredning|en=d4 published development or research report or study|": "report",
    "sprawozdanie szkolne": "report",
    "rapporter": "report",
    "fact sheet": "report",
    "circular técnica (infoteca-e)": "report",
    "published research report": "report",
    "eu council of the eu document": "report",
    "outras publicações técnicas (infoteca-e)": "report",
    "technical-report": "report",
    "研究报告": "report",
    "technical_report": "report",
    "fi=d4 julkaistu kehittämis- tai tutkimusraportti taikka -selvitys|sv=d4 publicerad utvecklings- eller forskningsrapport samt utredningar|en=d4 published development or research report or study|": "report",
    "reports and working papers": "report",
    "technical documentation": "report",
    "d4_julkaistu kehittämis- tai tutkimusraportti taikka -selvitys": "report",
    "policy paper": "report",
    "http://purl.org/eprint/type/report": "report",
    "electronic report": "report",
    "department technical report": "report",
    "research reports": "report",
    "<dc:type>report": "report",
    "project-report": "report",
    "working_paper": "report",
    "relatório técnico": "report",
    "clinical trial report": "report",
    "working paper/technical report": "report",
    "raport": "report",
    "short report": "report",
    "research_report": "report",
    "working document": "report",
    "d4 julkaistu kehittämis- tai tutkimusraportti taikka -selvitys": "report",
    "paper or report": "report",
    "report / forschungsbericht / arbeitspapier": "report",
    "bericht, report": "report",
    "d4 julkaistu kehittämis- tai tutkimusraportti tai -selvitys": "report",
    "government or industry research": "report",
    "consultants report": "report",
    "reportpart": "report",
    # --- dataset  (21 keys) ---
    "dataset": "dataset",
    "dataset/dataset": "dataset",
    "dataset/protein-ligand binding data": "dataset",
    "n - numeric data": "dataset",
    "dataset publication series": "dataset",
    "data or dataset": "dataset",
    "data": "dataset",
    "dataset/mass spectrometry": "dataset",
    "survey data": "dataset",
    "other/protein-ligand binding data": "dataset",
    "scientific data": "dataset",
    "data set": "dataset",
    "[data collection]": "dataset",
    "data collection": "dataset",
    "dataset bundled publication": "dataset",
    "protein-ligand binding data": "dataset",
    "research data": "dataset",
    "doc-type:researchdata": "dataset",
    "research_data": "dataset",
    "experimental data": "dataset",
    "researchdata": "dataset",
    # --- data-paper  (1 keys) ---
    "data paper": "data-paper",
    # --- software  (4 keys) ---
    "software": "software",
    "archivos de ordenador": "software",
    "software educacional": "software",
    "computationalnotebook": "software",
    # --- standard  (5 keys) ---
    "branżowa norma": "standard",
    "industrystandard": "standard",
    "norma branżowa": "standard",
    "guideline": "standard",
    "standard": "standard",
    # --- editorial  (8 keys) ---
    "editorial": "editorial",
    "editorial reviewed": "editorial",
    "commentary": "editorial",
    "opinion": "editorial",
    "edito": "editorial",
    "editorials/short communications": "editorial",
    "stellungnahme": "editorial",
    "editorial material": "editorial",
    # --- letter  (11 keys) ---
    "article / letter to editor": "letter",
    "article / letter to the editor": "letter",
    "correspondence": "letter",
    "letter to the editor": "letter",
    "carta": "letter",
    "letter or note in journal": "letter",
    "general correspondence": "letter",
    "personal correspondence": "letter",
    "korespondencje": "letter",
    "correspondencia": "letter",
    "letters (correspondence)": "letter",
    # --- erratum  (2 keys) ---
    "corrigendum": "erratum",
    "erratum": "erratum",
    # --- peer-review  (3 keys) ---
    "isirev": "peer-review",
    "peerreview": "peer-review",
    "peer-review": "peer-review",
    # --- paratext  (3 keys) ---
    "\\n            \\n                issue\\n            \\n": "paratext",
    "issue": "paratext",
    "liminaire": "paratext",
    # --- supplementary-materials  (2 keys) ---
    "pangaea documentation": "supplementary-materials",
    "supplementary dataset": "supplementary-materials",
    # --- other  (7 keys) ---
    "other": "other",
    "image": "other",
    "chemical structures": "other",
    "lecture": "other",
    "patent": "other",
    "creative project": "other",
    "null": "other",
}

# COAR resource type codes (official COAR vocabulary). Mirror of Repo.py COAR_RESOURCE_TYPE_MAP.
COAR_RESOURCE_TYPE_MAP = {
    "c_6501": "article",             # journal article
    "c_bdcc": "article",             # research article
    "c_93fc": "article",             # contribution to journal
    "r60j-j5bd": "article",          # article
    "c_5794": "conference-paper",    # conference paper  (FIX: legacy mapped this to 'article')
    "c_2f33": "book",                # book              (FIX: legacy mapped this to 'article')
    "c_3248": "book-chapter",        # book part
    "c_efa0": "conference-abstract", # conference poster
    "c_db06": "dissertation",        # doctoral thesis
    "c_46ec": "dissertation",        # thesis
    "c_816b": "preprint",            # preprint
    "c_8042": "report",              # working paper
}

# --- oxjob #537: best dc:type element selection over the full array ---
def resolve_repo_type(input_type):
    """One dc:type value -> OpenAlex work type (default 'other'). Applied per array element.
    Our REPO_TYPE_MAPPING is PRIMARY (tried as-is, then eu-repo-stripped); the COAR / version
    handling is only a fallback for values the text map does not cover."""
    if not input_type or not isinstance(input_type, str):
        return "other"
    low = input_type.strip().lower()
    # 1) our mapping is primary
    if low in REPO_TYPE_MAPPING:
        return REPO_TYPE_MAPPING[low]
    if "info:eu-repo/semantics/" in low:
        stripped = low.split("info:eu-repo/semantics/")[-1].strip()
        if stripped in REPO_TYPE_MAPPING:
            return REPO_TYPE_MAPPING[stripped]
    # 2) COAR resource_type code (both hosts) -- values not in the text map
    if "coar/resource_type/" in low or "coar-repositories.org/resource_types/" in low:
        m = re.search(r"(c_[0-9a-z]+|r60j-j5bd)", low)
        return COAR_RESOURCE_TYPE_MAP.get(m.group(1), "other") if m else "other"
    # 3) COAR version marker -> article
    if "purl.org/coar/version/" in low:
        return "article"
    return "other"

# type priority: specific types > 'article' > 'other'; 'preprint' sits below 'article'
# (preprint-by-source is owned by the is_preprint_repository rule, oxjob #540).
_TYPE_PRIORITY = [
    "retraction", "dissertation", "conference-paper", "conference-abstract", "book-review",
    "data-paper", "software", "dataset", "book-chapter", "book", "report", "reference-entry",
    "standard", "editorial", "letter", "peer-review", "review", "paratext", "article",
    "preprint", "supplementary-materials", "other",
]
_TYPE_RANK = {t: i for i, t in enumerate(_TYPE_PRIORITY)}

def _source_quality(raw):
    low = raw.lower()
    if "coar/resource_type/" in low or "coar-repositories.org/resource_types/" in low:
        return 2
    if "info:eu-repo/semantics/" in low:
        return 1
    return 0

def best_raw_and_type(dc_types):
    """Pick the winning element of a dc:type array: highest-ranked canonical type; ties broken
    toward the more structured raw (COAR > eu-repo > text), then array order. Returns
    (raw_native_type, type) where raw_native_type is the ORIGINAL full string (not parsed)."""
    if dc_types is None:
        return (None, "other")
    best = None
    for idx, raw in enumerate(dc_types):
        if raw is None or not str(raw).strip():
            continue
        raw = str(raw)
        typ = resolve_repo_type(raw)
        key = (_TYPE_RANK.get(typ, 999), -_source_quality(raw), idx)
        if best is None or key < best[0]:
            best = (key, raw, typ)
    if best is None:
        return (None, "other")
    return (best[1], best[2])

_BEST_TYPE_SCHEMA = StructType([
    StructField("raw_native_type", StringType(), True),
    StructField("type", StringType(), True),
])

@pandas_udf(_BEST_TYPE_SCHEMA)
def best_type_udf(dc_type_arrays: pd.Series) -> pd.DataFrame:
    rows = [best_raw_and_type(list(a) if a is not None else None) for a in dc_type_arrays]
    return pd.DataFrame(rows, columns=["raw_native_type", "type"])

def normalize_language_code(lang_code):
    """
    Normalize language codes to ISO 639-1 two-letter lowercase format.
    Handles ISO 639-1 (two-letter), ISO 639-2/T (three-letter), and common variations.
    """
    if not lang_code or not isinstance(lang_code, str):
        return None

    lang_code = lang_code.strip().lower()
    
    # remove any [[iso]] prefix
    if "[[iso]]" in lang_code:
        lang_code = lang_code.replace("[[iso]]", "")
        
    # handle special cases
    if lang_code in ["null", "und", "other"]:
        return None
    
    # handle codes with regional variants
    if "_" in lang_code:
        lang_code = lang_code.split("_")[0]
        
    # handle multiple codes (e.g., "tr; en")
    if ";" in lang_code:
        lang_code = lang_code.split(";")[0].strip()

    # full names to codes mapping
    names_to_codes = {
        "english": "en",
        "spanish": "es",
        "french": "fr",
        "german": "de",
        "chinese": "zh",
        "russian": "ru",
        "japanese": "ja",
        "arabic": "ar",
        "portuguese": "pt",
        "italian": "it",
    }

    # ISO 639-2 to ISO 639-1 mapping
    three_to_two = {
        # most common languages first for quick matches
        "eng": "en", "fra": "fr", "spa": "es", "deu": "de", "rus": "ru",
        "zho": "zh", "jpn": "ja", "ara": "ar", "por": "pt", "ita": "it",
        # additional languages
        "abk": "ab", "aar": "aa", "afr": "af", "aka": "ak", "alb": "sq",
        "amh": "am", "arg": "an", "arm": "hy", "asm": "as", "ava": "av",
        "ave": "ae", "aym": "ay", "aze": "az", "bam": "bm", "bak": "ba",
        "baq": "eu", "bel": "be", "ben": "bn", "bih": "bh", "bis": "bi",
        "bos": "bs", "bre": "br", "bul": "bg", "bur": "my", "cat": "ca",
        "cha": "ch", "che": "ce", "nya": "ny", "chi": "zh", "chu": "cu",
        "chv": "cv", "cor": "kw", "cos": "co", "cre": "cr", "hrv": "hr",
        "cze": "cs", "dan": "da", "div": "dv", "dut": "nl", "dzo": "dz",
        "epo": "eo", "est": "et", "ewe": "ee", "fao": "fo", "fij": "fj",
        "fin": "fi", "fre": "fr", "fry": "fy", "ful": "ff", "geo": "ka",
        "ger": "de", "gla": "gd", "gle": "ga", "glg": "gl", "glv": "gv",
        "gre": "el", "grn": "gn", "guj": "gu", "hat": "ht", "hau": "ha",
        "heb": "he", "her": "hz", "hin": "hi", "hmo": "ho", "hun": "hu",
        "ice": "is", "ido": "io", "iii": "ii", "iku": "iu", "ile": "ie",
        "ina": "ia", "ind": "id", "ipk": "ik", "isl": "is", "jav": "jv",
        "kan": "kn", "kau": "kr", "kas": "ks", "kaz": "kk", "khm": "km",
        "kik": "ki", "kin": "rw", "kir": "ky", "kom": "kv", "kon": "kg",
        "kor": "ko", "kua": "kj", "kur": "ku", "lao": "lo", "lat": "la",
        "lav": "lv", "lim": "li", "lin": "ln", "lit": "lt", "ltz": "lb",
        "lub": "lu", "lug": "lg", "mac": "mk", "mah": "mh", "mal": "ml",
        "mao": "mi", "mar": "mr", "may": "ms", "mlg": "mg", "mlt": "mt",
        "mon": "mn", "nau": "na", "nav": "nv", "nbl": "nr", "nde": "nd",
        "ndo": "ng", "nep": "ne", "nno": "nn", "nob": "nb", "nor": "no",
        "oji": "oj", "ori": "or", "orm": "om", "oss": "os", "pan": "pa",
        "per": "fa", "pli": "pi", "pol": "pl", "pus": "ps", "que": "qu",
        "roh": "rm", "rum": "ro", "run": "rn", "sag": "sg", "san": "sa",
        "sin": "si", "slo": "sk", "slv": "sl", "sme": "se", "smo": "sm",
        "sna": "sn", "snd": "sd", "som": "so", "sot": "st", "srd": "sc",
        "srp": "sr", "ssw": "ss", "sun": "su", "swa": "sw", "swe": "sv",
        "tah": "ty", "tam": "ta", "tat": "tt", "tel": "te", "tgk": "tg",
        "tgl": "tl", "tha": "th", "tib": "bo", "tir": "ti", "ton": "to",
        "tsn": "tn", "tso": "ts", "tuk": "tk", "tur": "tr", "twi": "tw",
        "uig": "ug", "ukr": "uk", "urd": "ur", "uzb": "uz", "ven": "ve",
        "vie": "vi", "vol": "vo", "wel": "cy", "wln": "wa", "wol": "wo",
        "xho": "xh", "yid": "yi", "yor": "yo", "zha": "za", "zul": "zu"
    }
    
    # check if it's already a valid two-letter code
    if len(lang_code) == 2:
        return lang_code
        
    # check full names
    if lang_code in names_to_codes:
        return names_to_codes[lang_code]
        
    # check three-letter codes
    if len(lang_code) == 3:
        return three_to_two.get(lang_code)
        
    return None

url_pattern = r"(https?://\S+|www\.\S+)"

id_struct_type = StructType([
    StructField("id", StringType(), True),
    StructField("namespace", StringType(), True),
    StructField("relationship", StringType(), True)
])

def extract_ids(identifiers, native_id):
   try:
       if identifiers is None:
           return []
       if not isinstance(identifiers, list):
           identifiers = [identifiers]
       if native_id is None:
           native_id = ""
           
       patterns = {
           'arxiv': (r"https?://arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5}|[a-z\-]+/\d+)", 1),
           'arxiv_native': (r"oai:arXiv\.org:([^/\s]+/\d+|\d+\.\d+)", 1),
           'doi': (r"\b10\.\d{4,9}/\S+\b", 0),
           'issn': (r"\b\d{4}-\d{3}[0-9X]\b", 0),
           'hal': (r"\bhal-\d+\b", 0),
           'handle': (r"https?://hdl\.handle\.net/([^/\s]+/[^/\s]+)", 1)
       }
       
       results = []
       arxiv_id_from_native = None
       
       # extract arxiv ID from native_id and normalize it
       try:
           if isinstance(native_id, str):
               match = re.search(patterns['arxiv_native'][0], native_id)
               if match:
                   arxiv_id_from_native = match.group(1)
       except Exception:
           pass
       
       # process each identifier
       for identifier in identifiers:
           if not identifier or not isinstance(identifier, str):
               continue
               
           try:
               for namespace, (pattern, group) in patterns.items():
                   match = re.search(pattern, identifier)
                   if match:
                       try:
                           relationship = None
                           
                           if namespace.startswith('arxiv'):
                               id_value = "arXiv:" + match.group(group)  # prepend arXiv:
                               
                               # check if this is an arxiv ID and compare with native_id
                               if arxiv_id_from_native:
                                   if id_value == f"arXiv:{arxiv_id_from_native}" or f"oai:arXiv.org:{match.group(group)}" == native_id:
                                       relationship = 'self'
                           else:
                               id_value = match.group(group)
                           
                           results.append({
                               "id": id_value,
                               "namespace": namespace.split('_')[0],
                               "relationship": relationship
                           })
                           break
                       except Exception:
                           continue
           except Exception:
               continue
       
       # add native_id
       if native_id:
           results.append({
               "id": native_id,
               "namespace": "pmh",
               "relationship": "self"
           })
       
       # deduplicate
       seen = set()
       unique_results = []
       for r in results:
           try:
               key = (r['id'], r['namespace'], r['relationship'])
               if key not in seen:
                   seen.add(key)
                   unique_results.append(r)
           except Exception:
               continue
       
       return unique_results
       
   except Exception as e:
       print(f"Error in extract_ids: {str(e)}")
       return []
   
def normalize_license(text):
    if not text:
        return None

    normalized_text = text.replace(" ", "").replace("-", "").lower()

    license_lookups = [
        # open Access patterns
        ("infoeureposematicsaccess", "other-oa"),
        ("openaccess", "other-oa"),
        
        # publisher-specific
        ("elsevier.com/openaccess/userlicense", "other-oa"),
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "other-oa"),
        ("arxiv.orgperpetual", "other-oa"),
        ("arxiv.orgnonexclusive", "other-oa"),
        
        # creative Commons licenses
        ("ccbyncnd", "cc-by-nc-nd"),
        ("ccbyncsa", "cc-by-nc-sa"),
        ("ccbynd", "cc-by-nd"),
        ("ccbysa", "cc-by-sa"),
        ("ccbync", "cc-by-nc"),
        ("ccby", "cc-by"),
        ("creativecommons.org/licenses/by/", "cc-by"),
        
        # public domain
        ("publicdomain", "public-domain"),
        
        # software/Dataset licenses
        ("mit ", "mit"),
        ("gpl3", "gpl-3"),
        ("gpl2", "gpl-2"),
        ("gpl", "gpl"),
        ("apache2", "apache-2.0")
    ]

    for lookup, license in license_lookups:
        if lookup in normalized_text:
            if license == "public-domain" and "worksnotinthepublicdomain" in normalized_text:
                continue
            return license

    return None

def has_oa_domain(native_id):
    oa_domains = ["arxiv", "osti", "pubmedcentral", "biorxiv", "medrxiv", "zenodo", "figshare", "open-science.canada"]
    if native_id is None:
        return False
    
    parts = native_id.lower().split(":")
    if len(parts) >= 2:
        domain_part = parts[1]
        for domain in oa_domains:
            if domain in domain_part:
                return True
    return False

def detect_version_from_xml(cleaned_xml, native_id):
    """
    Detect version from XML content and native_id based on regex patterns
    Returns 'acceptedVersion', 'publishedVersion', or 'submittedVersion'
    """
    
    ACCEPTED_VERSION_REPOS = [
        "oai:catalog.lib.kyushu-u.ac.jp",
        "oai:cronfa.swan.ac.uk",
        "oai:dora",
        "oai:e-space.mmu.ac.uk",
        "oai:hrcak.srce.hr",
        "oai:infocom.co.jp",
        "oai:library.wur.nl",
        "oai:lirias2repo.kuleuven.be",
        "oai:mro.massey.ac.nz",
        "oai:raumplan.iaus.ac.rs",
        "oai:repository.arizona.edu",
        "oai:repository.cardiffmet.ac.uk",
        "oai:researchbank.swinburne.edu.au",
        "oai:researchonline.gcu.ac.uk",
        "oai:rke.abertay.ac.uk",
        "oai:shura.shu.ac.uk",
        "oai:taju.uniarts.fi"
    ]
    
    # Check if native_id starts with any of the accepted repo prefixes
    if native_id:
        native_id_str = str(native_id)
        for repo in ACCEPTED_VERSION_REPOS:
            if native_id_str.startswith(repo + ":"):
                return "acceptedVersion"
    
    if not cleaned_xml:
        return "submittedVersion"
    
    search_text = str(cleaned_xml).lower()
    
    accepted_patterns = [
        r"accepted.?version",
        r"version.?accepted", 
        r"accepted.?manuscript",
        r"peer.?reviewed",
        r"refereed/peer-reviewed"
    ]
    
    for pattern in accepted_patterns:
        if re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL):
            return "acceptedVersion"
    
    published_patterns = [
        r"publishedversion",
        r"published.*version",
        r"version.*published"
    ]
    
    for pattern in published_patterns:
        if re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL):
            return "publishedVersion"
    
    return "submittedVersion"

extract_ids_udf = udf(extract_ids, ArrayType(id_struct_type))

@pandas_udf(StringType())
def normalize_license_udf(license_series: pd.Series) -> pd.Series:
    return license_series.apply(normalize_license)

@pandas_udf(StringType())
def normalize_title_udf(title_series: pd.Series) -> pd.Series:
    return title_series.apply(normalize_title)


@pandas_udf(StringType())
def normalize_language_code_udf(language_code_series: pd.Series) -> pd.Series:
    return language_code_series.apply(normalize_language_code)

@pandas_udf(BooleanType())
def has_oa_domain_udf(url_series: pd.Series) -> pd.Series:
    return url_series.apply(has_oa_domain)

@pandas_udf(StringType())
def detect_version_udf(metadata_series: pd.Series, native_id_series: pd.Series) -> pd.Series:
    return pd.Series([
        detect_version_from_xml(metadata, native_id) 
        for metadata, native_id in zip(metadata_series, native_id_series)
    ])


# COMMAND ----------

MAX_TITLE_LENGTH = 5000
MIN_ABSTRACT_LENGTH = 100
MAX_ABSTRACT_LENGTH = 10000
MAX_AUTHOR_NAME_LENGTH = 500
MAX_AFFILIATION_STRING_LENGTH = 1000

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

parsed_df = clean_df \
    .withColumn("native_id", regexp_extract(col("cleaned_xml"), r"<identifier>(.*?)</identifier>", 1)) \
    .withColumn("native_id_namespace", lit("pmh")) \
    .withColumn("title", substring(regexp_extract(col("cleaned_xml"), r"<dc:title.*?>(.*?)</dc:title>", 1), 0, MAX_TITLE_LENGTH)) \
    .withColumn("normalized_title", normalize_title_udf(col("title"))) \
    .withColumn("authors", 
        expr(f"""
            transform(
                regexp_extract_all(cleaned_xml, '<dc:creator>(.*?)</dc:creator>'),
                x -> struct(
                    cast(null as string) as given,
                    cast(null as string) as family,
                    substring(x, 0, {MAX_AUTHOR_NAME_LENGTH}) as name,
                    cast(null as string) as orcid,
                    array(struct(
                        cast(null as string) as name,
                        cast(null as string) as department,
                        cast(null as string) as ror_id
                    )) as affiliations
                )
            )
        """)) \
    .withColumn("raw_native_types", expr("regexp_extract_all(cleaned_xml, '<dc:type.*?>(.*?)</dc:type>', 1)")) \
    .withColumn("_best", best_type_udf(col("raw_native_types"))) \
    .withColumn("raw_native_type", col("_best.raw_native_type")) \
    .withColumn("type", col("_best.type")) \
    .drop("_best") \
    .filter(col("raw_native_type").isNull() | ~lower(col("raw_native_type")).isin(TYPES_TO_DELETE)) \
    .withColumn("identifiers", 
        expr("""
            transform(
                regexp_extract_all(cleaned_xml, '<dc:identifier>(.*?)</dc:identifier>'),
                x -> trim(x)
            )
        """)) \
    .withColumn("ids",
        extract_ids_udf(
            col("identifiers"),
            col("native_id")
        )) \
    .withColumn("version", detect_version_udf(col("cleaned_xml"), col("native_id"))) \
    .withColumn("language", normalize_language_code_udf(regexp_extract(col("cleaned_xml"), r"<dc:language.*?>(.*?)</dc:language>", 1))) \
    .withColumn("published_date",
        expr("""
            array_min(
                filter(
                    transform(
                        regexp_extract_all(cleaned_xml, '<dc:date.*?>(.*?)</dc:date>'),
                        date_str -> coalesce(
                            -- ISO format with timezone
                            to_date(to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss'Z'")),
                            -- ISO format without timezone
                            to_date(to_timestamp(date_str, "yyyy-MM-dd'T'HH:mm:ss")),
                            -- Regular date
                            to_date(date_str, "yyyy-MM-dd"),
                            -- Month and year
                            to_date(date_str, "yyyy-MM"),
                            -- Period-separated format
                            to_date(regexp_replace(date_str, '\\.', '-'), "yyyy-MM-dd"),
                            -- Year only
                            to_date(
                                if(length(trim(date_str)) = 4, concat(date_str, "-01-01"), null),
                                "yyyy-MM-dd"
                            )
                        )
                    ),
                    d -> d is not null and year(d) >= 1900
                )
            )
        """)) \
    .withColumn("created_date", col("published_date")) \
    .withColumn("updated_date", to_date(
        regexp_extract(col("cleaned_xml"), r"<datestamp>(.*?)</datestamp>", 1))) \
    .withColumn("abstract_raw", 
        element_at(expr("regexp_extract_all(cleaned_xml, '<dc:description>(.*?)</dc:description>')"), 1)) \
    .withColumn("abstract",
        when(length(col("abstract_raw")) >= MIN_ABSTRACT_LENGTH, 
            substring(col("abstract_raw"), 0, MAX_ABSTRACT_LENGTH))
        .otherwise(lit(None))) \
    .withColumn("source_name", 
        regexp_extract(col("cleaned_xml"), r"<dc:source.*?>(.*?)</dc:source>", 1)) \
    .withColumn("publisher", 
        regexp_extract(col("cleaned_xml"), r"<dc:publisher.*?>(.*?)</dc:publisher>", 1)) \
    .withColumn("urls",
        expr("""
            transform(
                case
                    when size(regexp_extract_all(cleaned_xml, '<dc:identifier>(http.*?)</dc:identifier>')) > 0
                    then regexp_extract_all(cleaned_xml, '<dc:identifier>(http.*?)</dc:identifier>')
                    else regexp_extract_all(cleaned_xml, '<dc:relation>(http.*?)</dc:relation>')
                end,
                x -> struct(
                    x as url,
                    case when lower(x) like '%pdf%' then 'pdf' else 'html' end as `content-type`,
                    case when lower(x) like '%pdf%' then 'pdf' else 'html' end as `content_type`
                )
            )
        """)) \
    .withColumn("raw_license",
        expr("""
            case 
                when size(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?creativecommons.org.*?)</dc:rights>')) > 0 
                then element_at(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?creativecommons.org.*?)</dc:rights>'), 1)
                else element_at(regexp_extract_all(cleaned_xml, '<dc:rights>(.*?)</dc:rights>'), 1)
            end
        """)) \
    .withColumn("license", normalize_license_udf(col("raw_license"))) \
    .withColumn("issue", lit(None).cast("string")) \
    .withColumn("volume", lit(None).cast("string")) \
    .withColumn("first_page", lit(None).cast("string")) \
    .withColumn("last_page", lit(None).cast("string")) \
    .withColumn("is_retracted", lit(None).cast("boolean")) \
    .withColumn("funders", array(
        struct(
            lit(None).cast("string").alias("doi"),
            lit(None).cast("string").alias("ror"),
            lit(None).cast("string").alias("name"),
            array(lit(None).cast("string")).alias("awards")
        )
    )) \
    .withColumn("references", array(
        struct(
            lit(None).cast("string").alias("doi"),
            lit(None).cast("string").alias("pmid"),
            lit(None).cast("string").alias("arxiv"),
            lit(None).cast("string").alias("title"),
            lit(None).cast("string").alias("authors"),
            lit(None).cast("string").alias("year"),
            lit(None).cast("string").alias("raw")
        )
    )) \
    .withColumn("mesh", lit(None).cast("string")) \
    .withColumn(
    "is_oa",
        when(
            lower(col("license")).startswith("cc") |
            lower(col("license")).contains("other-oa") |
            lower(col("license")).contains("public-domain") |
            has_oa_domain_udf(col("native_id")),
            lit(True)
        ).otherwise(lit(False))
    ) \
    \
    .filter(size(col("urls")) > 0) \
    .filter(size(filter(col("urls"), lambda x: ~x.url.contains("doi.org"))) > 0)

# Select final columns in the same order as DLT
parsed_df = parsed_df.select(
    "native_id",
    "native_id_namespace",
    "title",
    "normalized_title",
    "authors",
    "ids",
    "raw_native_type",
    "type",
    "version",
    "license",
    "language",
    "published_date",
    "created_date",
    "updated_date",
    "issue",
    "volume",
    "first_page",
    "last_page",
    "is_retracted",
    "abstract",
    "source_name",
    "publisher",
    "funders",
    "references",
    "urls",
    "mesh",
    "is_oa",
    "endpoint_id"
)

# deduplicate based on native_id and most recent updated date
parsed_df = parsed_df.sort(col("updated_date").desc()).dropDuplicates(["native_id"])

# Use MERGE instead of overwrite to avoid reprocessing all records in CDF stream
target_table = "openalex.repo.repo_works_backfill"
parsed_df.createOrReplaceTempView("repo_works_backfill_updates")

# Check if table exists
table_exists = spark.catalog.tableExists(target_table)

if table_exists:
    # Table exists, use MERGE to only update changed/new records
    # This prevents CDF from seeing all records as new inserts
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forName(spark, target_table)
    delta_table.alias("target").merge(
        parsed_df.alias("source"),
        "target.native_id = source.native_id"
    ).whenMatchedUpdateAll(
        condition="source.updated_date >= target.updated_date"  # update ALL columns from source
    ).whenNotMatchedInsertAll(  # insert ALL columns from source
    ).execute()
else:
    # Table doesn't exist yet, create it (first run only)
    parsed_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table)
