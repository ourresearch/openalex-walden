# Changelog

We document notable changes to the data in this file; it's in reverse chronological order (recent changes up top).

## [Unreleased]
### Changed
* Works: the `publication_date` of a work dated by its Crossref record is now the earliest publication date the publisher states in that record (issued, published online, published in print, posted or approved). The day the DOI was registered with Crossref, and the day of its latest deposit, count only when the record states no online or posted date: then they stand in for when the work went online, as for publishers that deposit only a print date. Before, the registration day always counted, so a DOI registered ahead of its stated online date was dated by its registration: 10.1162/rest_a_01152, registered 22 December 2021 and published online and in print in March 2024, read 2021 and now reads 2024. About 9.2M Crossref records change date, every one to a later date; about 832,000 change year, and `publication_year` follows. A date more than a year ahead of today is still replaced by the day the work first appeared. (oxjob #1157)
* Works: `related_works` is recomputed for every work: up to 10 works closest in meaning to each one (the nearest neighbours of its title, abstract and venue in OpenAlex's Qwen3 embeddings), never paratext, errata, retractions, reviews or retracted works, and never a work with the same title. 437.5M works have a list (95.8% of titled core works, 92.8% of titled expansion-pack works; before: 64.9% of core works and no expansion-pack work, a one-time 2025 copy of an older topic-based method). Blind-judged on random works, the new lists' items average 2.34 on a 0-3 relatedness scale vs 0.72 for the old lists. New works now get their list the day after they are added (a nightly step; recall of the 10 nearest vs exact search 0.93). The backfill did not move `updated_date` (snapshot users need a full reload of works); a new work's list landing does. (oxjob #1648)
* Works: `topics` and `primary_topic` now come from a new topic classifier (a Qwen3-8B model trained on 2M works labelled by Claude Opus 5.5, reading the title, abstract and venue). The topics themselves do not change: same 4,516 topics, names, IDs and subfield / field / domain hierarchy. Each work gets up to 3 topics; `score` is the model's calibrated probability that the topic is the work's main one, so 2nd and 3rd scores are often small. Works the model cannot classify (mostly records with no abstract and little title text) get no topic. On a fresh test set of random works the primary topic is right 81.7% of the time vs 24.8% for the previous model; about 447M works change topics. `fwci`, `citation_normalized_percentile` and `cited_by_percentile_year` move with the new subfields (their cohort). `updated_date` does not move for this change: snapshot users need a full reload. Code, weights and evaluations: https://github.com/ourresearch/openalex-topic-classification/tree/main/v2 (oxjob #1531)
* Works: `sustainable_development_goals` now comes from a new classifier: a per-goal head on the work's Qwen3 embedding, trained on 200K Jev (TypeSafe) judgments of the 17 goals and calibrated per goal. Same element shape (`id`, `display_name`, `score`); a goal is listed when its `score` (the classifier's confidence, 0-1, higher = surer) is at least 0.4, highest first. Works without an embedding have an empty list. Display names follow the UN wording: "Quality education", "Life on land", "Peace, justice, and strong institutions". Against a judge committee on 2,000 random works the new tags score F1 0.68 vs 0.29 for the previous model, and better on all 17 goals. (oxjob #1300)
### Fixed
* Works: research and news about retractions are no longer typed `retraction`. A type rule reads a title that starts 'Retraction' as a retraction notice, so papers such as 'Retractions in business, management, and accounting journals' (W4412689778) and 'Retractions onto series-parallel posets' were typed retraction. Titles that start 'Retractions' followed by words such as 'in', 'of', 'and' or 'onto' now take their type from the record; real notices ('Retractions and Errata', 'Retractions for vol. 96, p. 11566', 'Retractions: <paper title>') are still typed retraction. (oxjob #1976)
* Works: posters deposited in HAL, the French open archive, are now typed `conference-abstract`, not `conference-paper`. HAL's record names its own document type ('Conference poster', or 'Poster communications' in older records), but it lists the generic 'conference object' first, and we read only that, so about 84,000 HAL poster records, on about 57,000 to 73,000 works, were typed conference-paper (W7205683094). Where a poster's work also has a copy in published proceedings, that copy leads and the work keeps its type. (oxjob #1976)
* Locations: a copy's `pdf_url` is never an image. On about 43,600 Elsevier articles the publisher copy's `pdf_url` (and so `open_access.oa_url` and `best_oa_location.pdf_url`) was the article's graphical abstract or a figure (`https://ars.els-cdn.com/content/image/...-ga1_lrg.jpg`), which our landing-page reader had taken for the PDF link; about 5,500 repository copies gave a page thumbnail or cover image (`.../name.pdf.jpg`, `cover.png`). Such a copy now gives its real PDF link where we have one and otherwise no `pdf_url`; whether the copy counts as open is unchanged.
* Sources: a location can now be moved to the venue that really holds it, through a curation (users-api `POST /curations`, entity `locations`, property `source_id`, site curators; `remove` leaves the location with no source). The first 3,446 curations move MAG records filed under a venue that shares an abbreviation with their real one: 1,238 IEICE technical reports of the Satellite Telecommunications committee ('SAT') leave Theory and Applications of Satisfiability Testing (S4306421062) for Technical report of IEICE. SAT (S2764659323); 2,070 Minor Planet Circulars records ('MPC') leave Mathematics of Program Construction (S4306420494) for Minor planet circulars/minor planets and comets (S4306519989); 123 articles of the Japanese journal 組織科学 (Organizational Science) leave INFORMS's Organization Science (S206124708) for 組織科学 (S4306556653); and 15 NASA conference items that ADS catalogues as 'sat' no longer name the satisfiability conference. Each venue's works list and counts now hold only its own works. (oxjob #2173)
* Works: `language` is always an ISO 639-1 code, or a three-letter ISO 639-2/3 code for a language without one (`ceb`), as the docs say. When no text classification is available we use the language a source record declares, and those values were served as written: `EN`, `en-US`, `zh-CN`, `eng`, `中文`, `chi`, `und`, or `enc` (a code that Zenodo's language picker offers for English deposits). Each declared value is now normalised: lowercased and trimmed, the region dropped (`zh-CN` -> `zh`), three-letter and named forms mapped to the two-letter code (`eng`, `English` -> `en`), and "no language" or private-use values (`und`, `mis`, `mul`, `zxx`, `qaa`-`qtz`, 'other', country codes that name no one language) left empty. About 228,000 works change (102,000 outside the expansion pack). Detected languages and the language PubMed's indexers record are unchanged.
* Works: a DOI that doi.org says does not exist is no longer shown. Some MAG, repository and PubMed records carry a DOI that was never registered (Taylor & Francis chapter DOIs such as `10.4324/9780203412770.ch22_1`, ACM's `10.5555` pseudo-DOIs, a `.an` suffix on a Nature DOI) or one on DataCite's `10.5072` test prefix. Such a DOI now appears on neither the work (`doi`, `ids.doi`) nor the record's location; the work and its other data are unchanged, and a Crossref or DataCite record's own DOI is never touched. This first pass covers 551 DOIs checked one by one at doi.org (551 works). (oxjob #1656)
* Authorships: a work whose source lists the whole author list as a single author ("Ratkovic, Marc T. (University Of Wisconsin, Madison); Eng, Kevin H. (...)", "Joao A. de Andrade, MD;Gary M. Owens, MD;...", "Hartmut BOESCH,Yi LIU,Paul I PALMER,...") now has one author per name. About 368,000 works changed, mostly Harvard Dataverse, Zenodo, OSTI and other repository and DataCite records, and the combined name no longer lands on one person's profile. A list is split only when every part reads as a person's name; a name printed with a spaced hyphen ("Lo, Li - Ta" in OSTI records) counts as one name. An organisation's name stays whole ("Ministère de l'Agriculture et de la Réforme Agraire, Direction de la Conservation Foncière ...", "Koninklijke Academie voor Nederlandse Taal- en Letterkunde"), as do two names joined by one comma, which can't be told apart from "Family, Given". (oxjob #583)
* Works: posters deposited through DataCite (Zenodo, the European Society of Radiology's EPOS, ESGAR, ESCRS and other repositories) are now typed `conference-abstract`, not `article`. Their DataCite record names them a poster in its own `resourceType` ('Poster', 'Conference poster'), but we read only the record's generic citation type ('article-journal'), so about 147,000 poster records, on up to about 147,000 works, were typed article (W7224247776). Poster uploads on ResearchGate are left as they are, and so are presentations: a deposited slide deck may be a conference talk, a lecture or a webinar.
* Authorships: a byline name written twice in one string is listed once. Some deposits put the same full name in both the given and the family field, so the byline read 'J. L. Lebrun J. L. Lebrun' (W2984076902); since 17 November 2026 `raw_author_name`, and `author.display_name` for a seat with no author profile, show the name once (about 159,700 seats on about 99,800 works). One-word repeats ('Yang Yang') and Arabic name chains made only of common given names ('Mohamed Hassan Mohamed Hassan', which can be a real four-part name) are left as printed. A byline that lists the whole author list twice, once without accents and once with them, now lists each person once: the Vietnam Journal of Diabetes and Endocrinology's records list 'Quach, Huu Trung', 'Mac, Thi Luu', then 'Quách, Hữu Trung', 'Mạc, Thị Lựu' (W7220400018). Only whole lists repeated in the same order are merged; a list where any name differs by more than its accents is left alone. (oxjob #1155)
* Authorships: a work whose records name no author at all (an empty Crossref deposit, no landing page to read) can now get the byline printed on the work itself, through a curation (users-api `POST /curations`, entity `works`, property `byline`, a JSON list of `{name, orcid}` in printed order; site curators). It only fills an empty byline: once any record names an author, the record wins. Author ids follow through the usual matching, and a claim on a curated name binds once its seat exists. The first 9 works carry one. (oxjob #1815)
* Works: copies harvested through DiVA's national feeds (DiVA is the repository platform shared by Swedish universities) are now credited to the member university's own repository source when that member has one: 30 members, among them Uppsala, Linköping, KTH, Umeå, Borås and the Swedish Defence University. The national feeds serve every member's records under the same ids (`oai:DiVA.org:<member>-<n>`) the members' own feeds use, and we keep one copy per id, so a member's record could be listed under KTH's source or the national DiVA source instead of its own (University of Borås papers showed KTH). About 448,000 DiVA copies on 439,000 works change source; members with no feed of their own keep the national DiVA source. `updated_date` moved for these works. (oxjob #952)
* Works: copies harvested through IRDB, Japan's institutional repository database, no longer show a CC BY-NC-ND licence their record never states, and show the version their record declares. Every WEKO3 / JAIRO Cloud record page carries its search form's list of licence options (CC0 to CC BY-NC-ND 4.0), and we read that list as the item's licence, so about 1.12M IRDB copies showed `cc-by-nc-nd`; they now take the licence their own record states, or none. Their record's version (JPCOAR `oaire:version`, AM or VoR) also now outranks the `submittedVersion` our page and PDF parsers assume, so about 1.37M copies change from `submittedVersion` to `publishedVersion` and about 93,000 to `acceptedVersion`. Whether a copy counts as open is unchanged. About 1.96M works change; `updated_date` moved for them. (oxjob #891)
* Works: a repository copy's PDF link is no longer one of a Digital Commons record's "additional files" (URLs with `/type/additional/` or `type=additional`). When a record lists several files, the link could fall on a supplementary file, such as a results table or the university's deposit agreement form, instead of the article (W4404547497 linked Plymouth's PEARL copy to a results file). The record's main file now supplies the link when we have fetched it; otherwise the copy has no PDF link. About 2,400 repository copies change, 969 of them in PEARL; whether a copy counts as open is unchanged. (oxjob #1854)
* Works: copies in Oxford's repository, ORA (ora.ox.ac.uk), are now listed. We harvested ORA in its plain `oai_dc` format, which carries no link to the record or its file and no DOI, so every ORA record was dropped. Since 3 November 2026 we harvest ORA's `unpaywall_dc` format, which carries the file link, the DOI and the version: 158,637 ORA copies are listed, 178,167 works list an ORA copy (51,175 before, most of them through the DataCite DOIs ORA registers for theses), and ORA is the best open copy of 80,019 works. About 9,900 ORA records that we had first read in the old format, and that ORA has not changed since, follow after a one-time re-parse; about 450 of their works read `closed` today. `updated_date` moved for the works that gained a copy. (oxjob #1960)
* Works: repository copies we re-read from their stored records in a one-time re-parse (Oxford's ORA and the University of Antwerp's IRUA) now show the version their own record declares. Since 16 November a version a repository record states (an accepted manuscript, the published version) outranks the `submittedVersion` our landing-page and PDF parsers assume, but re-parsed records were left out of that rule, so once we had parsed their file they read `submittedVersion`: Oxford's accepted manuscript of 10.1073/pnas.2611548123 (W7203481173) did. About 19,100 copies change version (14,765 in IRUA, 4,374 in ORA). Whether a copy counts as open is unchanged. `updated_date` moved for these works. (oxjob #1960)
* Works: a work's `language` can now be corrected by a curation (users-api `POST /curations`, entity `works`, property `language`, any code listed at /languages); the curated value wins over every copy's declared or detected language. The first 830 curations fix works whose language was misread: 827 served as Latvian (`lv`) whose text is English (765, mostly American Physical Society physics papers whose formula fragments such as 'MoS 2' read as Latvian), French (55), Indonesian (4) or German (3), and 3 others, among them a Korean article served as English (oxjob #1703, #1731).
* Authorships: about 233,000 works no longer show an ORCID inside an author's name. Some university repository exports (Pure) append the ORCID to the byline name ('Luo, Yonglun; id_orcid 0000-0002-0007-7759'), and the name was served as is in `raw_author_name`; the '; id_orcid ...' part is now dropped. The author link, which already came from that ORCID, is unchanged. Support can also now correct one author's name on one work, or remove a byline entry that is not an author of the work (a volume editor, a thesis supervisor), when the work itself prints it otherwise.
* Authorships: on papers whose publisher deposits one affiliation statement for the whole paper, keyed by the authors' initials ("From the Department of Neurology (S.M., D.R.), All India Institute of Medical Sciences, New Delhi, India; Brain Health Unit (K.K., T.D.), World Health Organization, Geneva"), each author now keeps only the parts that name their initials. Neurology, the American Heart Association journals, Radiology, NEJM and others repeat that statement under every author, so every author showed every institution on the paper. About 101,000 works change. An author the statement does not name (for example one listed after the point where Crossref cut a long statement short) gets no institution from it.
* Works: `fwci` and `citation_normalized_percentile` no longer count citations from datasets, the same as `cited_by_count` and `counts_by_year`, which have left them out since November 2025. Dataset records that cite papers, above all the Large Helical Device shot records of Japan's National Institute for Fusion Science, gave a few instrument papers an FWCI in the thousands (W4393041245: `cited_by_count` 2, `fwci` 14,117, top 1%) and inflated the citation baseline of their cohorts. In 2023-24 Nuclear and High Energy Physics, about half the citations in the FWCI window came from these records, so the FWCI of ordinary articles there goes up: the median 2024 article's from 0.08 to 0.23. Measured on 16 November 2026: about 412,000 works' FWCI moves by 0.1 or more (53,000 of them published 2023-25), about 4,500 works gain or lose a top-1% flag and about 33,000 a top-10% flag. `cited_by_percentile_year` already read `counts_by_year` and does not change. A change in these values does not move `updated_date`. (oxjob #1561)
* Authorships: when a work's Crossref record lists an author's affiliations starting with an entry that carries only a ROR id and no name (Optica Publishing Group deposits them this way), the named affiliations after it now reach the author ("Vilnius University"). The whole list used to be dropped, so about 9,600 works, mostly Optica's, had authors with no affiliation. The ROR id itself still does not assign an institution; institutions come from the affiliation text.
* Authorships: American Physical Society papers (DOIs 10.1103/...) from 2024 on show each author's whole affiliation line again ("Laboratoire Jacques-Louis Lions, Sorbonne Université, 4 Place Jussieu, 75005 Paris, France", not "Sorbonne Université"). Since 2024 APS deposits in Crossref only the part of each line it links to a ROR id, so the lab, department and address were lost, and some lines with no such link were lost entirely. Where the article's page lists the same authors in the same order, its printed lines are now used; link tags in them are removed. About 31,700 works change (153,000 author affiliations, most of them a fragment becoming the whole line). `updated_date` moved for these works. (oxjob #1062)
* Works: `open_access.any_repository_has_fulltext` is now always `true` or `false`, and `true` for every green work. It was null on about 9.87M works (1.52M outside the expansion pack): those whose open copy had no known source, when no repository copy was open. A copy that `oa_status` calls green (a repository record, or a preprint with no known journal) now counts as a repository copy. About 8.4M works become `true` and 1.5M `false`. Most of the `true` ones are Crossref components (figures, tables, supporting files), which now take the flag from their article, as they already take its open-access status; 37 components that read `false` become `true` that way. No other work that read `true` or `false` changes. `updated_date` moved for these works. (oxjob #1096)
* Works: a Zenodo deposit that Zenodo marks restricted, closed or embargoed is no longer an open copy through its Zenodo DOI (10.5281/zenodo.<record>) either, as its Zenodo repository record already is not. The DOI's DataCite record read open whatever Zenodo said, and about 35,000 Zenodo repository records still read open from before Zenodo restricted them, so about 309,000 copies on 235,000 works were open copies with no file a reader can open. They stay listed, as closed copies; about 6,900 works whose only open copies were such deposits now read closed, 3,700 of them Zootaxa articles whose Biodiversity Literature Repository deposit is restricted (10.11646/zootaxa.3321.1.3, W2367255862). (oxjob #1628)
* Works: a repository copy's licence is the one its own repository record states. Where the record names a specific licence (its `dc:rights`), that licence now outranks a different one read from the item's web page or PDF; the page or PDF still fills a missing or generic (`other-oa`) licence. Repository pages often show a site default or a menu of every licence beside the item's own (UNSWorks item pages carry CC BY-NC-ND 3.0 AU on items licensed CC BY), and we read those. About 678,000 repository copies change licence; on random copies whose page shows the item's own licence, the repository record's licence was the right one in 44 of 48. `updated_date` moved for these works. (oxjob #1096)
* Authorships: raw affiliation strings from Crossref now include the place the publisher deposited with them. Crossref affiliations can carry a city and country (`place`) apart from the name, and we read only the name, so about 1.76M works showed affiliations such as "University of South Carolina" with no place, and the institution matcher sometimes chose a same-name institution in another country. The string is now the name followed by its place ("University of South Carolina, Columbia, United States"); a place the name already contains is not repeated. Most affected: Taylor & Francis, Oxford University Press, ACS, IEEE, Frontiers, Emerald and eLife. On the changed strings, links to an institution in a different country from the deposited place fall by more than half. Where the older affiliation table held a copy of the same affiliation cut short at a comma on the same author, only the fuller string is kept (about 1.4M works). Curations on the old strings carry over to the new ones. `updated_date` moved for these works. (oxjob #1242)
* Sources, publishers, institutions, funders and authors: `summary_stats.2yr_mean_citedness` now follows its documented, journal-impact-factor-style definition: the citations received in the last full calendar year by the entity's works published in the two years before it, divided by the number of those works (in 2026: the 2025 citations to works published in 2023 and 2024, read from each work's `counts_by_year`; the window moves on 1 January). Before, it was the mean all-time `cited_by_count` of the works published in the current year and the two before it, so it mixed citation windows of different lengths, could not be rebuilt from `counts_by_year`, and moved every day. Most values go down (about: Nature 26.1 to 19.8, The Lancet 25.7 to 18.6, The Journal of Finance 22.2 to 18.4). Every work still counts in the denominator (sources and publishers leave out peer reviews), unlike the Journal Impact Factor, which divides by articles and reviews only. Institutions now count each work once, not once per author from the institution. `updated_date` moved for the entities whose value changed. (oxjob #1851)
* Authorships: co-authors who share a surname and first initial (Seabert and Skyler Mao, Chinmay and Chandan Chakraborty) no longer each carry the other's affiliations. Affiliations were joined to authors by surname and first initial, so such co-authors each got the union of both; each now takes the affiliations listed at their own place in the record. About 1M works change. Where Crossref lists affiliations per author, the corrected authors now match it 99.7% of the time (44.5% before).
* Authorships: affiliation strings with no letter in them (empty strings, ',', '-', '?', page, phone or postcode digits) are dropped from `raw_affiliation_strings` and `affiliations`. About 1.1M works carried one; 769,000 of them had no other affiliation string, and 62 lose an institution such a string had been matched to.
* Works: where a preprint and its published version are one work, the work now shows the published version's abstract. Both abstracts were on the work and the one shown was whichever text sorted first, so about 383,000 works showed the preprint's wording (bioRxiv/medRxiv 94K, SSRN 92K, Research Square 69K, Copernicus discussion papers 46K, Preprints.org 36K, PubMed's own records of preprints 10K, ChemRxiv 9K), which often differs from the article's after peer review. The published abstract comes from the article's Crossref or PubMed record and is passed over only when it is a fragment (under 100 characters, or a PubMed text our PubMed reader cut short). Springer and Elsevier articles whose abstract we withhold still show their Research Square preprint's own abstract, as before. The text that search reads now always comes from the same record as `abstract_inverted_index`; on about 1.46M more works that text changes and the abstract shown does not. `updated_date` moved for these works. (oxjob #2142)
* Works: `citation_normalized_percentile` no longer marks uncited works as highly cited. Works of the same type, year and subfield with the same citation count in the FWCI window now share one `value` (the share of those works with fewer citations), so a work with no citations in the window reads 0 and is never `is_in_top_10_percent` or `is_in_top_1_percent`. Before, ties were spread out by work ID, and about 1.69M works with no citations read top 10% (88,000 top 1%). From 10 November 2026: 4.86M works lost a flag and none gained one; the `value` of about 216M uncited works moved to 0. A change in `value` alone does not move `updated_date`, so if you keep a copy current by `updated_date`, reload `citation_normalized_percentile` from a full snapshot made after 10 November 2026.
* Works: `is_retracted` is now true for every work whose DOI the Retraction Watch database lists as a retracted paper (and not as reinstated). We read the database itself each day, as Crossref publishes it. Before, we saw its retractions only through Crossref's records, which carry one only when Retraction Watch names the retraction notice's DOI, so about 1,300 retractions without a notice DOI (a notice that was never registered, or a paper simply withdrawn) never reached us unless the publisher changed the title: 411 works in OpenAlex read false, among them the Lancet letter 10.1016/s0140-6736(20)30730-3. `is_retracted` is still also true when Crossref or PubMed records a retraction or the title starts "Retracted". `updated_date` moved for the works that changed. (oxjob #737)
* Works: a Crossref item typed `other` that carries an ISBN (a chapter some publishers deposit as 'other': Wiley's reference-work entries, Agenda Publishing's 'Syria' chapters 10.1017/9781788217002.*) no longer lands on a journal that merely shares its book's title. Since October 2026 a book or chapter whose container title matches a journal's name is attached to that journal only when the journal has no ISSN, but the rule went by type, so about 30,700 such locations still sat on 1,391 same-named ISSN journals (W4405018140 on the journal Syria, S131235000). They now take the publisher's ebook platform (Wiley eBooks, Agenda Publishing eBooks) or no source. About 1,600 of them name an ISSN book series whose ISSN they don't carry and leave it, as typed chapters already do. (oxjob #828)
* Authors: about 59,000 author profiles minted from a library-catalogue heading are named with the name the heading holds, on the profile and on the bylines of their ~205,000 works ('Mazet, Georges. Auteur du texte' had become the profile 'Georges. Auteur du texte Mazet'; now 'Georges Mazet'). New profiles are named this way when they are minted. Matching does not change: the name profiles are matched on stays as it was. The byline cleaner also stops cutting names short ('Charles René Fourcroy de Ramecourt' showed as 'Charles Re Fourcroy de Ramecourt', 'John Randolph' as 'Joh Randolph'), keeps name words a feed separated with wide spaces ('Matheus de Oliveira Silva', not 'Matheus de Silva'), and drops BnF qualifiers such as '(1885-19.. ; médecin)', second roles ('. Auteur de lettres. Cartographe') and descriptors ('Billot, médecin de l'Empereur'): about 30,000 byline names on up to ~69,000 works. Blind-judged on fresh random samples: renamed profiles 98% right (vs 1% before), changed byline names 94% right (vs 32%). (oxjob #1944)
* Authorships: a byline entry that is only an ORCID or Lattes CV link is no longer listed as an author. Repository and some publisher feeds list a person's identifiers as creators of their own, right after the person's name (`Aurilene Ferreira da Silva`, `http://lattes.cnpq.br/0252786455741071`), so about 25,000 works showed a link as a co-author. The link entry now folds into the person it follows; where every name in the byline is followed by its own identifiers, its ORCID iD becomes that author's `raw_orcid` (and `author.orcid` when the author has no profile), which agreed with ORCID's own record for 99.3% of 8,179 authors. A work whose only author is a link keeps it for now. (oxjob #1925)
* Works: a new arXiv preprint's DOI (10.48550/arXiv.<id>, which arXiv registers with DataCite) now joins the work that holds the preprint's arXiv record. It named its paper only in the DOI, so it could join only by title, and about 1 in 23 arXiv preprints (139,059) became two works: `/works/doi:10.48550/arXiv.<id>` answered with a second, thin copy, and a `doi:` filter or a citation to the arXiv DOI found it instead of the paper. 94,830 of those pairs are now one work (the thin copy merged into the paper's work; blind-checked on 327 pairs, all the same paper); about 44,000 where the preprint's arXiv record sits on another publication's work stay apart for now. (oxjob #1940)
* Authors: about 25,000 author profiles a week stop getting a new id. A weekly re-check meant to find papers bound to the wrong person read every byline written 'Surname, Given' or 'Organisation, Place' as empty, so it unbound those papers, matched them again and minted a new profile under a new id, deleting the old one (which then answered 404). Most were organisation names from library catalogues ('Hamilton College (Clinton, N.Y.)', '7 Stages Theatre (Atlanta, Ga.)'); some were people written surname first. Ids now stay as they are.
* Works: abstracts and titles are no longer cut short at a "<" that is text. The cleaner read any "<" followed by a letter as the start of an HTML tag and deleted everything up to the next ">" or to the end of the text, so an inequality ("(<I00 days old)", "0<x<1"), a detection limit ("<LOD"), a placeholder ("<script_name.py>") or an angle-quoted title ("Film <The Throne>", "<Hygrophila phlomoides> Nees") cut the rest of the sentence or of the abstract. About 659,000 abstracts and 80,000 titles regain the text (38M words; 135K had lost half or more), from every source: PDFs, landing pages, repositories, DataCite, Crossref, PubMed and the legacy abstract table. Real markup (HTML, JATS, MathML) is still removed, and nothing that could run in a browser is kept. Swapped tags in some repository titles (">i<Pinus>/i<") are repaired. `updated_date` moved for the works whose abstract or title changed. (oxjob #1928)
* Works: about 220,000 Research Square preprints (220,104) show their abstract again. Research Square licenses every preprint CC BY in its own Crossref record, but Crossref names its publisher Springer, so the rule that withholds Springer and Elsevier abstracts hid them. A work now shows a Research Square abstract only when it is the text of that preprint's own Crossref record, under that record's CC BY licence. Where a preprint is merged with its published version, the work shows the preprint's abstract; the published paper's abstract stays hidden, even when the two differ. (oxjob #1926)
* Works: `mesh` matches the work's current PubMed record: each heading once, with NLM's major-topic marks. Before, a work carried one copy of its headings for every PubMed record version we had stored, so 33.4M of the 33.6M works with MeSH listed each heading 2-3 times (1.30B entries for 406M distinct), 284K held copies that disagreed on `is_major_topic`, and about 577K kept headings NLM has since removed. And `is_major_topic` read only the descriptor's mark, so a pair NLM marks on its qualifier ("Neoplasms/drug therapy*") showed false: 72.2M pairs on 24.2M works. Now only the latest record version is read and `is_major_topic` is true when NLM marks the descriptor or that qualifier: 100 of 100 random works match NCBI's current record (0 of 100 before). About 14K works whose current record has no MeSH now have none. `updated_date` moved for the ~33.4M works whose `mesh` changed. (oxjob #1920)
* Authorships: each affiliation is listed once per author. About 16.3M works listed the same affiliation two or more times on one author, the copies differing only in punctuation, quote marks or spacing ("Xi’an" / "Xi'an", "Royal Holloway University of London  Egham UK" / "Royal Holloway University of London, Egham, UK"), most often one copy from the work's record and one from the older affiliation table. `raw_affiliation_strings` and `affiliations` now keep one copy per affiliation: the best-formed one (parts separated, no doubled spaces or stray marks), carrying the institution ids of every copy, so no institution is lost and `institutions` does not change. 50.2M copies leave 46.8M authorships. `updated_date` moved for these works. (oxjob #1911)
* Works: about 7.06M works that had no `publication_year` (6.1M of them in the expansion pack, most of them digitised older material from library and repository collections) now carry the year their own repository or DataCite records give. A model read every record we hold for each of the 14.8M works with no year, and a year is kept only when the model was sure (about half of them). An independent check against other catalogues found about 1.5% of the kept years off by more than a year (the years Crossref gives for old works: 1.4%). Years before 1900 are now kept, which replaces the 2025-11-01 OAI-PMH rule. A DataCite placeholder year of 10000 no longer outranks a real date. `fwci`, `citation_normalized_percentile` and `cited_by_percentile_year` compare a work with others of the same year, subfield and type, so works published before 1950 see their values move where these collections fill their cohorts; for works published since 1950, 99.9% of FWCI values move by less than 10%. `updated_date` moved for the works that gained a year. (oxjob #1644)
* Authorships: names from Figshare no longer end in the person's Figshare account number. Figshare's harvest feed writes each creator as 'Tao Zou (662198)', and a line break inside a name arrived as a literal '\n' that the byline dropped ('GuillaumeN. Rivière'). A Figshare name now drops a trailing '(digits)' when a letter stays in it, and keeps the space. About 132,000 works with a Figshare copy change (W6940960599, W7112009718).
### Removed
* Works: `sustainable_development_goals_aurora`, the previous model's (Aurora SDG-BERT) tags frozen when `sustainable_development_goals` switched to the new classifier in October 2026, is removed as announced (2 November 2026). It was API-only and never in the snapshot. Use `sustainable_development_goals`. (oxjob #1300)

## [2026-08-18]
### Fixed
* Authorships: raw affiliation strings with double-encoded UTF-8 (mojibake, e.g. `UniversitÃ©`) are repaired to their proper form (`Université`) — ~172K distinct strings on ~189K works, mostly publisher-deposited Crossref metadata; a garbled string and its clean twin on the same author collapse into one entry. Institution matching now keys on the repaired string: where the garbled form and its clean twin had been matched differently, the clean form's match stands (the garbled-text matches were systematically degraded), and garbled strings with no clean twin are re-matched on the clean text. Expect small institution works_count shifts (largest: junk institutions losing works, e.g. "Anna Needs Neuroblastoma Answers" −1K; real universities gaining, e.g. University of Concepción +1.2K). RAS curations keyed on a garbled string follow it to the repaired form. (oxjob #801)

## [2025-11-02]
### Fixed
* Citations: corrected `cited_by_percentile_year` by computing proper per-year distributions and bounds; min/max now reflect true placement across years.

## [2025-11-01]
### Changed
* OAI-PMH: set `publication_date` to the earliest available date; if the earliest parsed date is < 1900, leave `publication_date` null.
* Keywords: restrict derived keywords to concepts with `level > 0` (more changes may be required).

## [2025-10-30]
### Changed
* Sources: minor curation (removed one problematic source), which may shift `primary_location.source` for a small subset of works.

## [2025-10-29]
### Changed
* References: stabilize `referenced_works` by unioning backfilled and parsed sources rather than overwriting.

### Added
* Authorships: populate `author.id` (OpenAlex URL) and `author.orcid`; fill `affiliations.institution_ids`.

## [2025-10-28]
### Added
* Authorships: populate `affiliations.institution_ids` in authorships.

## [2025-10-27]
### Changed
* Topics: for merged works, include topics from `merge_into_id` while keeping the original `work_id` assignment.

## [2025-10-26]
### Fixed
* Added `raw_native_type` field to `superlocations` table for all provenances (crossref, repo, datacite, mag, pubmed) to ensure proper schema alignment in UNION operations

## [2025-10-22]
### Fixed
* 160,083 new PubMed and Arxiv records ingested and linked to existing works; ongoing ingest and mapping fixed
* Adjusted `created_date` so it is stable and does not reset every time a location is added to a work

## [2025-10-21]
### Added
* Added Support for Works Magnet Institutes Moderation

### Fixed
* `issn` field added to `Work.locations.source`, ensuring all issns displayed
* 384,969 bioRxiv works matched to source ID S4306402567 in `Work.locations.source`
* `Work.locations.source` is null if no source ID is found
* PubMed host organization was set to 'PubMed' within `Work.locations.source` but has been changed to National Institutes of Health
* Remove 'mag' from `ids` object if its value is null

## [2025-10-20]
### Added
* Add roles attribute to Funders and Publishers

## [2025-10-17]
### Aded
* Add roles, repositories and associated_institutions attributes to Institutions

### Fixed

* Corrected a data type issue in the `crossref_parsed` table schema.

## [2025-10-07]

### Added

* New notebook `sync_funders.ipynb` for ad-hoc analysis.
* Updated `CreateOpenAlexWorks` to insert more Funder data.

## [2025-10-02]

### Added

* **Publishers:** Added `lineage`, `summary_stats`, and `updated_date` attributes for Elastic Search sync.
* **Funders:** Added `summary_stats`, `grants_count`, `works_count`, `cited_by_count`, `counts_by_year`, and `roles` attributes for Elastic Search sync.

## [2025-10-01]

### Added

* **Authors:** Added `topics` array (top 5) for Elastic Search sync.
* **Sources:** Added `host_organization`, `lineage`, `summary_stats`, `societies`, `alternate_titles`, `topics`, and `topics_share` attributes.

### Removed

* **Sources:** Removed the `country` attribute.

## [2025-09-25]

### Added

* New notebook `analyze_fwci_distribution.ipynb` for ad-hoc analysis.
* Utility function `get_latest_partition()` to simplify reading from time-series data.

### Changed

* The `crossref_deduplicated` table now reads from a temporary table to improve DLT graph analysis.
* Increased the number of partitions for the `work_sdg` ingestion job to 32.

### Fixed

* Corrected `work_id` assignment logic to properly use the `mid.work.MERGE_INTO_work_id` table.

## [2025-09-21]

### Fixed

* Resolved a `NULL` handling bug in the `issued_date` transformation logic.

## [2025-09-15]

### Added

* Implemented language detection for work titles.
* Added documentation explaining the biblio merge normalization algorithm.

### Changed

* Updated logic to use DOI prefixes for connecting ODi-assigned works to sources.

## [2025-08-24]

### Added

* FWCI is now calculated and available in the `works` table.
* Added new keyword generation logic.
* Concepts are now processed and included in the dataset.

### Fixed

* Resolved stability issues with `work_id` and `updated_date` assignments.
* Corrected OpenSearch sync process.

## [2025-08-13]

### Added

* Initial implementation of the `keywords` feature.

## [2025-07-16]

### Added

* Implemented a backfill for `topics` from the production environment.

### Fixed

* Resolved an issue where the abstract inverted index was not appearing in the API.

## [2025-07-07]

### Added

* Sources are now available in the API with basic counts.

## [2025-06-26]

### Fixed

* Implemented logic to merge duplicate ISSN sources.
