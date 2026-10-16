DEFERRED MERGES: 59; active merges in this switch 0; owner Rohan.

Coordinator decision 2026-10-13: defer every one of the 59 twin merges from the switch. NICHD F4320337364 → F4320337611 is an additional, separately deferred row and is not counted below. No merge may return automatically after a date. NICHD is eligible for a new reviewed release only Sun 10-18 after final fold checks.

The active CSV is header-only; the daily registry SQL, release stage and reported-funder reader contain typed empty merge relations. The audit-only `deferred_merges.csv` retains the reviewed 59 decisions and reasons. All 204 aliases and hierarchy/name rules remain active. No existing funder fields, DOI bytes or award owners change in this switch.

All 59 currently have served funder entities. Together they own 52,596 published award documents on the retained cut. In the no-merge switch, users continue to see these owners in awards and can resolve their existing funder entities; HTTP redirects are unnecessary. The old merge build would have removed 59 entity rows at the independently scheduled API/ES rebuild while leaving their awards attached to absent owners. SQL redirects alone do not populate the `merge-funders` index used by the HTTP API: an absent loser without such a mapping becomes a 404. Live ES mappings were not inspected, and no redirect behavior is certified for a future merge release.

Later release design, to be reviewed and proved before restoring any row:

1. Choose an explicit identity contract per merge. The preferred first step is a durable served loser redirect/stub contract: retain the loser entity or publish a verified `merge-funders` loser→winner mapping before any entity exclusion, with no chain or cycle, and define whether GET /funders/F<loser> returns a 301 or a stable stub. Verify the survivor is served. A SQL redirect by itself is insufficient. Test API and ES GET, search, filters, embedded award.funder and work.funders behavior; search must not double count twins, and award-owner IDs must remain resolvable.
2. Alternatively, re-point awards to winners through a coordinated award-registry/basis release. Inventory all awards, API documents, work-award links, aliases, identity keys, source tables and hard-coded owners. Patch CFI, TWAS, Gulbenkian, Beijing NSF and other source producers in the same frozen release so the next ingest cannot reintroduce loser owners. Preserve public award IDs and links; resolve cross-funder key/DOI collisions explicitly. A blind update to awards_api would be overwritten by the next Nightly and is not an implementation.
3. For either design, stage immutable per-merge before/after journals and exact affected work hashes; prove no unrelated owner/ID/link changes, no unresolvable owner, zero unexpected removals, no redirect loops and correct repeat-run behavior. Recount on the release cut. Include API/index publication ordering and actual readback, stale-delete accounting, awards/index versions and a recovery that restores served documents and mappings after partial publication. The current paused pre-night table inverse cannot roll back an already published merge.
4. Only after the design gates pass: new source/SQL/index bytes, hashes, dev rehearsal, combined pre-clear, fresh per-action approval and pause-safe forward/inverse deployment. Do not label any referenced loser safe to delete. Keep redirect/stub retention explicit even if awards are later re-pointed.

Per-merge measured inventory (frozen published cuts: awards v1028, awards_api v1272, funders_api v647). `published works` is funders_api.works_count, not a deduplicated sum across twins. No-merge candidate document counts are included to expose normal replay differences. The verbatim queries/results are `queue/q9400_g2_proof_deferred_inventory.*` and `queue/q9400_g2_final_deferred_works.*` in the funder-fixes-20261013 lane.

| Loser | Survivor | Name | Awards | Published documents | Candidate documents | Published works |
|---|---|---|---:|---:|---:|---:|
| F4320320995 | F4320319952 | Canada Foundation for Innovation | 14212 | 14212 | 14212 | 1364 |
| F4320334977 | F4320322919 | Beijing Municipal Natural Science Foundation | 9890 | 9890 | 9891 | 12707 |
| F4320322650 | F4320322511 | Narodowym Centrum Nauki | 7989 | 7989 | 7990 | 28151 |
| F4320307846 | F4320320338 | Human Frontier Science Program | 5387 | 5387 | 5387 | 2219 |
| F4320321793 | F4320321572 | Beijing Municipal Education Commission | 1473 | 1473 | 1473 | 3582 |
| F4320327387 | F4320322928 | Danmarks Frie Forskningsfond | 1432 | 1432 | 1432 | 2888 |
| F4320325698 | F4320320876 | Helmholtz Association | 967 | 967 | 967 | 5407 |
| F4320326272 | F4320326716 | Department of Science and Technology, Philippines | 936 | 936 | 936 | 2284 |
| F4320326187 | F4320335408 | Health and Family Planning Commission of Sichuan Province | 891 | 891 | 891 | 945 |
| F4320325829 | F4320326442 | Kementerian Riset Teknologi Dan Pendidikan Tinggi Republik Indonesia | 870 | 870 | 870 | 2620 |
| F4320321078 | F4320324112 | The World Academy of Sciences | 722 | 722 | 722 | 1073 |
| F4320320176 | F4320333685 | Chief Scientist Office | 529 | 529 | 529 | 1423 |
| F4320323335 | F4320320052 | Fundação Calouste Gulbenkian | 528 | 528 | 528 | 510 |
| F4320326292 | F4320310064 | Śląski Uniwersytet Medyczny w Katowicach | 484 | 484 | 484 | 1533 |
| F4320321680 | F4320321034 | New Energy and Industrial Technology Development Organization | 454 | 454 | 454 | 2663 |
| F4320321673 | F4320307844 | EMBO | 416 | 416 | 416 | 778 |
| F4320307847 | F4320320919 | International Foundation for Science | 398 | 398 | 399 | 568 |
| F4320322784 | F4320321983 | Ministry for Business Innovation and Employment | 387 | 387 | 387 | 1096 |
| F4320328324 | F4320325338 | Shanghai Shenkang Hospital Development Center | 385 | 385 | 385 | 558 |
| F4320323001 | F4320309751 | AFM-Téléthon | 362 | 362 | 362 | 743 |
| F4320319963 | F4320307845 | Heart and Stroke Foundation of Canada | 335 | 335 | 335 | 1195 |
| F4320326825 | F4320321020 | Rannís | 273 | 273 | 274 | 1044 |
| F4320307871 | F4320324112 | The World Academy of Sciences | 258 | 258 | 258 | 584 |
| F4320320098 | F4320335087 | Technology Strategy Board | 247 | 247 | 247 | 573 |
| F4320314289 | F4320309751 | French Muscular Dystrophy Association | 245 | 245 | 245 | 728 |
| F4320328788 | F4320326721 | Polit National Laboratory for Marine Science and Technology | 228 | 228 | 228 | 1123 |
| F4320309718 | F4320315324 | Fondazione Banco di Sardegna | 203 | 203 | 203 | 518 |
| F4320324289 | F4320312952 | Worldwide Cancer Research | 183 | 183 | 183 | 248 |
| F4320320317 | F4320307849 | North Atlantic Treaty Organization | 158 | 158 | 158 | 341 |
| F4320328144 | F4320323069 | Fondation contre le Cancer | 154 | 154 | 154 | 1382 |
| F4320325396 | F4320320569 | NSW Ministry of Health | 122 | 122 | 122 | 4044 |
| F4320311361 | F4320324284 | Srinakharinwirot University | 112 | 112 | 113 | 235 |
| F4320326715 | F4320313933 | Ministère de l'Enseignement supérieur, de la Recherche et de l'Innovation | 111 | 111 | 111 | 1180 |
| F4320318872 | F4320322932 | Manipal Academy of Higher Education | 105 | 105 | 105 | 1302 |
| F4320310170 | F4320310129 | Baylor College of Medicine | 97 | 97 | 97 | 354 |
| F4320325265 | F4320314250 | Ministry for Primary Industries | 90 | 90 | 90 | 329 |
| F4320309712 | F4320320889 | Fondation Jérôme Lejeune | 79 | 79 | 79 | 187 |
| F4320325432 | F4320312931 | United Kingdom Space Agency | 76 | 76 | 76 | 159 |
| F4320324765 | F4320311542 | Breast Cancer Now | 72 | 72 | 72 | 130 |
| F4320334968 | F4320309729 | Université Catholique de Louvain | 69 | 69 | 69 | 708 |
| F4320321384 | F4320312942 | Yorkshire Cancer Research | 68 | 68 | 68 | 194 |
| F4320325681 | F4320320569 | NSW Health | 58 | 58 | 58 | 462 |
| F4320320685 | F4320311280 | University of the Witwatersrand, Johannesburg | 52 | 52 | 52 | 916 |
| F4320313286 | F4320321612 | Defence Research and Development Canada | 51 | 51 | 51 | 783 |
| F4320322448 | F4320313290 | Ontario Institute for Cancer Research | 51 | 51 | 51 | 431 |
| F4320328779 | F4320322242 | National Foundation of Elites | 48 | 48 | 48 | 319 |
| F4320316508 | F4320307813 | Medtronic | 47 | 47 | 47 | 1106 |
| F4320337562 | F4320337378 | Office of Integrative Activities | 45 | 45 | 45 | 62 |
| F4320335936 | F4320335827 | University College London Hospitals Biomedical Research Centre | 39 | 39 | 39 | 300 |
| F4320320087 | F4320312961 | Higher Education Funding Council for England | 35 | 35 | 35 | 323 |
| F4320321723 | F4320322992 | Ferring | 35 | 35 | 35 | 402 |
| F4320332609 | F4320310135 | University of Texas Medical Branch at Galveston | 34 | 34 | 34 | 151 |
| F4320310106 | F4320310061 | Kansas State University | 27 | 27 | 27 | 149 |
| F4320323847 | F4320323846 | Ministry of Education, Lifelong Learning and Religious Affairs | 18 | 18 | 18 | 90 |
| F4320324766 | F4320310551 | Brunel University London | 18 | 18 | 18 | 305 |
| F4320326476 | F4320325069 | Université Libre de Bruxelles | 14 | 14 | 14 | 153 |
| F4320327549 | F4320324520 | Alzahra University | 14 | 14 | 14 | 1384 |
| F4320313292 | F4320321802 | Ontario Mental Health Foundation | 11 | 11 | 11 | 140 |
| F4320321396 | F4320312924 | Elekta | 2 | 2 | 2 | 165 |

GOFIX3 follow-up: name_aliases.csv maps “kementerian riset, teknologi dan pendidikan tinggi republik indonesia” to active deferred loser 4320325829. A later merge must re-point this alias to the reviewed winner or serve its redirect; include alias targets in per-merge referential checks.
