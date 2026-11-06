"""Cultural-heritage feeds OpenAlex no longer harvests (oxjob #1647).

Jason + Kyle, 6 Nov 2026: stop harvesting the heritage collections their holders already split off -- whole
OAI-PMH endpoints, OAI-PMH sets inside endpoints we keep, and DataCite clients (repositories). Each unit was
sample-checked before it was listed. 'Harvest less', not a per-record rule.

GENERATED -- do not edit by hand. Source: openalex.works.oxjob1647_heritage_drop_units (snapshot openalex.works.oxjob1647_heritage_units_shipped, 2026-11-06T17:10:16.956Z),
written by oxjobs #1647 work/heritage/build/gen_lists.py at 2026-11-06 11:14 CT.
87 endpoints, 2246 sets on 268 endpoints, 8 DataCite clients.

Readers: repo_filters.apply_endpoint_filters (endpoints + sets; sets match the setSpec EXACTLY, unlike the
prefix carves in ENDPOINT_SETSPEC_DELETE) and notebooks/ingest/DataCite.py (clients). Records harvested before
the change leave through CreateSuperLocations' anti-join on openalex.works.oxjob1647_heritage_removed.
Revert: empty these three structures (or revert the commit); oxjobs #1647 work/heritage/build/REVERT.md.
"""

HERITAGE_ENDPOINTS_TO_DELETE = frozenset({
    '002e2f399be615570ad',  # Mississippi State University Libraries - Digital Collections
    '00b7fe92c2c2bb57638',  # The W&M Digital Archive (The College of William & Mary)
    '01645fe2006eabd488e',  # 
    '06e2edf79e530ca659b',  # Filmoteca
    '0778874f3e0f2c14953',  # 
    '0d5f2d9cd368eb933c3',  # University of Pittsburgh Digital Library
    '0d9f4f7c3cb75d1e23a',  # 
    '0fefc56b77220fdea4e',  # South Dakota Digital Archives
    '13aff79f476ea1ff38a',  # North Carolina Digital Collections, Department of Natural and Cultural Resources
    '14513fa63d485bf8863',  # ARTstor OAICatMuseum Repository
    '16c858a9ee675dfa4a7',  # Lux Scholarship And Creativity At Lawrence University (Lawrence University)
    '18d30433be445330573',  # 
    '23a335716b6dcd39e35',  # The Seattle Public Library
    '288d45ac16053d2d677',  # Milwaukee Public Library Digital Collections
    '28cfd21853ea0d0f5b2',  # Bok Tower Gardens Library Digital Collections
    '2e4529425fad7034895',  # 
    '30db98600b851cc0692',  # Digital Commons - USU (Utah State University)
    '32d8ada0fd0a68248f6',  # CONTENTdm Server Respository
    '34eace1aec611ad4833',  # 
    '356c292f415c4b8e373',  # Butler University Digital Collections
    '3895ae87935b50dc078',  # Digital Commons @ Olivet (Olivet Nazarene University)
    '3b8cc3c5d4067e35b9e',  # Cardinal Scholar (Ball State University)
    '3e89ec12ea56f928e59',  # The North Jersey History and Genealogy Center Digital Collections
    '3ec21ea5cbd1540a5c8',  # ACPL Digital Collections
    '3f0cdec94e3d678baba',  # Southern Utah University Digital Collections
    '3f501ea8f807be4d02a',  # Saint Michaels College Archives
    '41908a71fe57aa9a767',  # CONTENTdm Server Respository
    '439d37efc8448ffac14',  # UNM’s Digital Repository (University of New Mexico)
    '46411b64c9ca213e010',  # Digital Howard (Howard University)
    '49ea1177be6bc9c2a0a',  # CONTENTdm digital collection (cdm16681.contentdm.oclc.org)
    '4d7d0f61cd3de0c6ec7',  # 
    '51adfe741794848bb25',  # SA Health Libr serv
    '54d8fb3c570b0bf96ba',  # Knowledge@UChicago (University of Chicago)
    '55bde43a9b044bcf9a6',  # CONTENTdm digital collection (cdm16630.contentdm.oclc.org)
    '5896b7c8406be1c1769',  # University of San Francisco Gleeson Library Digital Collections
    '59001904ef511356b0d',  # Newton Gresham Library Digital Collections
    '5c4142e075ac13d08b0',  # Digital Maryland
    '5eaf1b20917f666c236',  # The University of Akron Online Repository
    '63ec245f32387c778fb',  # Salem Public Library Historic Photograph Collection
    '65420f47ceca7a94758',  # e-codices
    '65d5b9d8f419075f85a',  # Entrepot OAI-PMH de la Bibliotheque municipale de Toulouse : collections numeriques
    '6812302304a21934310',  # CORE Scholar (Wright State University)
    '6abb462cbe7f95e3d36',  # PALNI Digital Collections (Butler University)
    '6ad32fec09a9d32a30d',  # Western Michigan University Libraries ContentDM Collections
    '709c370af40e3ba3598',  # 
    '80d81792d5392981a0d',  # The Digital Library project by the National Documentation Center (EKT) (National Documenta
    '84497aa62a3c1bfe498',  # 
    '86fcb7038464c2035a6',  # REAL-MS (Magyar Tudományos Akadémia)
    '87c130d66c4940cc3de',  # 
    '8956d3baf1ad96b9e7e',  # 
    '8d4d127590b0906fa62',  # Georgia State University Repository
    '91c3623f0439b140c9e',  # Southern Utah University Digital Collections
    '961c4d855ed10bb11eb',  # Biblioteca Virtual del Principado de Asturias
    '970460b468ea2521352',  # University of the Pacific Respository
    '9c63022c096c9bfaff8',  # dcmny.org Repository
    'a8a2a5da54857b14ea7',  # National Fairground Archive
    'aa43b8fc9a54d78c0de',  # EngagedScholarship @ Cleveland State University (Cleveland State University)
    'ac23462bf3a6f3e11ee',  # Whitworth Digital Commons (Whitworth University)
    'ac9bd88a3a897c995d1',  # Special Collections and Archives Digital Collections (Kent State University)
    'afbf335566cbf9e9a84',  # Pueblo City-County Library District Digital Collections
    'b4e5d9763d1ad9d6ad4',  # DUGi Fons Especials Digital Repository (University of Girona)
    'c00a2c0a077b86136cf',  # CARLI Digital Collections (Consortium of Academic and Research Libraries in Illinois)
    'c144a2869134aa1519b',  # Pflugerville Public Library Repository
    'c15aee624fe46115794',  # Missouri State University Digital Collections
    'c430cfbb7e1ad782027',  # 
    'c5153ab5b5401677800',  # HPL - CONTENTdm Server Respository
    'cb57b69f42743f451ff',  # Tennessee State Library and Archives
    'cd7ce75471383f4baa8',  # Digital Scholarship - UNLV (University of Nevada Reno)
    'ce223372a68b86042f5',  # 
    'ce2fca01acb1d3f2f45',  # The Booth Library Postcard Collection (Eastern Illinois University)
    'd1913f350e4cbabe459',  # Theodore Roosevelt Center
    'd582d24fea2b5a87732',  # TUScholarShare (Temple University)
    'd87bdf27adb885d007f',  # Seymour Johnson Base Library
    'db54a37f180c6dff57e',  # UNICAMP Digital Library (Universidade Estadual de Campinas)
    'dd3965700fa745bc4f6',  # Colburn School Piatigorsky Archives
    'df79ea91299978d1183',  # 
    'e02fa9431d1bff08ab9',  # 
    'e5babeac2c361a0dff7',  # UC Irvine
    'ea9f435e218e0df8a1a',  # Scholar Commons (Santa Clara University)
    'ec9ec1841dc810acae6',  # 
    'ed1e75547452810e23b',  # JCSU Digital Archives
    'ee807b4cdcba7c160ea',  # The AMICA Library
    'ee82aa1dd3f635c3be4',  # 
    'eeaa1aa95d5bb3e69bc',  # 
    'f78f8316170374b317b',  # HPLD Digital Archives
    'fe216083dee73cf8741',  # Madison County (Illinois) Digital Collections
    'ffaf9f6bb907699d705',  # W. S. Hoole Special Collections Library Manuscript Collections
})

# endpoint -> exact setSpecs whose records are removed; the endpoint's other sets stay.
HERITAGE_ENDPOINT_SETSPEC_DELETE = {
    '0186ff199419958777f': (  # Handschriften
        'ulbdms', 'ulbdtheaterzettel',
    ),
    '01f00eab890a3a033f8': (  # University Archives
        'publication:archives', 'publication:forum', 'publication:lanthorn', 'publication:student-newspapers',
        'publication:tjc-pubs',
    ),
    '04890587a556cf801c9': (  # Archives and Special Collections
        'publication:asc', 'publication:asc-archival', 'publication:asc-special', 'publication:diocese',
        'publication:newsreleases', 'publication:pcfrance', 'publication:pcgermany', 'publication:pcitaly',
        'publication:pcunitedstates', 'publication:pcus-pacificwestern', 'publication:postcards',
        'publication:southern-cross', 'publication:student-newspapers', 'publication:usdnews', 'publication:vista',
    ),
    '0560a8a6cc40aee10bb': (  # Archives & Special Collections
        'publication:archives', 'publication:goddardphotographs', 'publication:universityarc',
        'publication:videoarchive',
    ),
    '05a799fb4714114db94': (  # Dordt Diamond
        'publication:dordt_diamond', 'publication:dordt_voice', 'publication:fall2005', 'publication:fall2007',
        'publication:fall2011', 'publication:fall2013', 'publication:fall2017', 'publication:fall2020',
        'publication:fall2024', 'publication:grotenhuis_choral', 'publication:spring2015', 'publication:spring2017',
        'publication:spring2022', 'publication:syll1993', 'publication:syll1994', 'publication:syll1996',
        'publication:syll1997', 'publication:syll1998', 'publication:syll1999', 'publication:syll2000',
        'publication:syll2001', 'publication:syll2002', 'publication:syll2003', 'publication:syll2004',
        'publication:syll2005', 'publication:syll2006', 'publication:syll2007', 'publication:syll2008',
        'publication:syll2009', 'publication:syll2010', 'publication:syll2012', 'publication:syll2013',
        'publication:syll2015', 'publication:syll2021', 'publication:syll2023', 'publication:voice',
    ),
    '05c42a5ef18745cacd4': (  # The Comment
        'publication:comment', 'publication:stud_news',
    ),
    '07bd579def86fc4158f': (  # publication:athletics-images
        'publication:athletics-images', 'publication:ms008woodbury-letterheads',
        'publication:ms008woodbury-letterheads-atoz', 'publication:wpiarchives', 'publication:wpihistoricalimages',
        'publication:wpihistory-images',
    ),
    '082b3874c70a15966cc': (  # Associated Students, Incorporated records
        'publication:asi', 'publication:blackvoice', 'publication:bridges-photographs',
        'publication:bridges-project', 'publication:campus-newspaper', 'publication:cap-cim', 'publication:cba',
        'publication:coyote-chronicle', 'publication:facultysenate', 'publication:fridaybulletin',
        'publication:hispanicnews', 'publication:howe-photographs', 'publication:lbh-images',
        'publication:lbhp-main', 'publication:pawprint', 'publication:specialcollections',
        'publication:universityarchives',
    ),
    '094675915c28003b8ec': (  # publication:newspaper_coverage
        'publication:newspaper_coverage', 'publication:sheppard_newspaper',
    ),
    '095e533a4c6ef137e4a': (  # Alumni News/Connecticut College Magazine
        'publication:alumnews', 'publication:ccnews', 'publication:learcenter', 'publication:sheetmusic',
    ),
    '09f01b83e11a72aac7b': (  # llevantVolums
        'llevantVolums', 'premsaForanaMallorca',
    ),
    '0a2b5e31960118ace9e': (  # publication:archives
        'publication:archives', 'publication:gavelgazette', 'publication:history_archives',
        'publication:lectures_pre_arch_archives', 'publication:photos_nyls',
    ),
    '0b43172bd40bf3be624': (  # publication:bx_arch_minutes
        'publication:bx_arch_minutes', 'publication:lacuny_meeting_minutes',
    ),
    '0b7652e76c8de4904a2': (  # Graphic Design
        'col_2144_2423',
    ),
    '0ba602c3105ef29dd0c': (  # Calliope
        'publication:calliope', 'publication:catalogs', 'publication:commencements', 'publication:hasc',
        'publication:hsp', 'publication:jaic', 'publication:jedediah-strong-smith', 'publication:jmc',
        'publication:jmc-jpegs', 'publication:jmc1908', 'publication:jmc1909', 'publication:jmc1910',
        'publication:jmc1913', 'publication:jmcl', 'publication:jmd', 'publication:jmj', 'publication:jmj-all',
        'publication:jmj-individual', 'publication:jmn-images', 'publication:moscone', 'publication:muir',
        'publication:muir-correspondence', 'publication:muir-notebooks', 'publication:newsletters',
        'publication:pacific-review', 'publication:pacifican', 'publication:postcards',
        'publication:school-newsletters', 'publication:stockton-photographs', 'publication:student-newspaper',
        'publication:studentnews1940-1949', 'publication:studentnews1960-1969', 'publication:ua',
        'publication:ua-athletics', 'publication:ua-events', 'publication:ua-football',
        'publication:ua-photographs', 'publication:wa', 'publication:yoshikawa',
    ),
    '0c0b491e9a13b6821f8': (  # College of Fine Arts
        'publication:cofa', 'publication:music', 'publication:music_ensembles',
        'publication:music_graduate_recitals', 'publication:music_junior_recitals', 'publication:music_programs',
        'publication:music_recitals', 'publication:unlv_communications', 'publication:unlv_publications',
    ),
    '0ca29b8db920c2f1a8e': (  # Peter Britt Photographs
        'p15013coll1', 'p16085coll11', 'p16085coll21',
    ),
    '0e7480153f196a2f7c0': (  # p16057coll37
        'p16057coll37',
    ),
    '0ed473bcbc6f37e0d36': (  # publication:university_documents
        'publication:university_documents',
    ),
    '0ef9aa4cd18142685bb': (  # abogados
        'abogados', 'arganda', 'badajoz', 'bvandalucia', 'caceres', 'elektra', 'historico_castellon',
        'realbiblioteca', 'reino_valencia',
    ),
    '10b3da7b0fc1948e5c5': (  # Archives
        'publication:archives', 'publication:communications', 'publication:component2c', 'publication:criterion1',
        'publication:criterion2', 'publication:criterion3', 'publication:criterion4', 'publication:performances',
        'publication:pressreleases', 'publication:snc_magazine', 'publication:snc_magazine_archives_2013-2018',
        'publication:snc_magazine_archives_2019-2023',
    ),
    '10e4b9f6451f62e44e8': (  # phototheque
        'phototheque',
    ),
    '119ed21f2743477efd2': (  # Archival and Special Collections
        'publication:acir', 'publication:bcim', 'publication:g1_s1', 'publication:general_corr',
        'publication:mission', 'publication:tennellycorr',
    ),
    '123aeab8ea17a8cce01': (  # col_10898_1655
        'col_10898_1655', 'com_10898_346', 'com_10898_3790',
    ),
    '12bef34239c3cff7cfb': (  # University Archives
        'publication:archives', 'publication:baptist-historical-collections',
        'publication:baptist-historical-collections-first-baptist-church-shelby',
        'publication:faculty-meeting-documents', 'publication:faculty-meeting-materials',
        'publication:fay-webb-gardner', 'publication:fay-webb-gardner-genealogical-research',
        'publication:gardner-webb-campus-buildings-and-grounds', 'publication:gardner-webb-history',
        'publication:gardner-webb-newscenter-archive', 'publication:gardner-webb-pub',
        'publication:greater-cleveland-county-baptist-association', 'publication:gwu-today',
    ),
    '136c0ddccbf3f22a2c3': (  # publication:archives_books
        'publication:archives_books', 'publication:archives_home',
    ),
    '1377113e94933c182b6': (  # University Archives
        'publication:archives', 'publication:thecord',
    ),
    '14755a28101a78e9e4c': (  # Bracero Oral History Project
        'publication:bracero', 'publication:casasola', 'publication:digit-archives', 'publication:edge_texas',
        'publication:oral_history', 'publication:photo_coll', 'publication:photo_db', 'publication:prospector',
        'publication:spec_coll',
    ),
    '176507404fd50c605fc': (  # Academic Senate
        'publication:academicsenate', 'publication:calpoly_magazine', 'publication:campus', 'publication:pao',
        'publication:pao_pr', 'publication:pao_rpt', 'publication:senateagendas', 'publication:senateminutes',
        'publication:senateresolutions', 'publication:studentnewspaper', 'publication:universityarchives',
    ),
    '182437ee5d57d2e6f01': (  # Alumni Association
        'publication:alumni', 'publication:cc_minutes', 'publication:convocations_docs', 'publication:cupp_mins',
        'publication:facgov', 'publication:facgov_cupp', 'publication:facgov_meet', 'publication:facgov_staff',
        'publication:favgov_cc', 'publication:iwumag', 'publication:news', 'publication:oralhistory',
        'publication:press_releases', 'publication:press_releases_1980', 'publication:press_releases_1990',
        'publication:press_releases_2000', 'publication:press_releases_2010', 'publication:press_releases_2020',
        'publication:production_image_collections', 'publication:sen_mins', 'publication:stugov',
        'publication:stugov_sen', 'publication:theatre_arts_productions', 'publication:theatre_images',
        'publication:theatre_productions_images', 'publication:theatre_productions_uponmattress2010_images',
        'publication:univcom', 'publication:univoffices',
    ),
    '1a1ba1332fafe36c86f': (  # publication:law_archives
        'publication:law_archives', 'publication:law_archives_life',
    ),
    '1df8d3867a196e491e3': (  # com_10784_1336
        'com_10784_1336',
    ),
    '1e57f881575bd4af778': (  # The Utah Statesman
        'publication:newspapers', 'publication:university_archives',
    ),
    '1fdacb3ff02093245cc': (  # Black American Sheet Music Collection
        'publication:aa_sheet_music', 'publication:aabg', 'publication:admin_financial_services',
        'publication:administration', 'publication:altrusa', 'publication:altrusa_records', 'publication:asturiano',
        'publication:asturiano_membership', 'publication:bdj_studioat620', 'publication:bdj_studioat620_postcards',
        'publication:burgert', 'publication:challenger', 'publication:columbia', 'publication:columbia_collection',
        'publication:convocations_graduations_celebrations', 'publication:crime_reports_alerts_info',
        'publication:crows_nest', 'publication:darfuri_drawings', 'publication:dunn_phototouring',
        'publication:dunn_postcards', 'publication:fac_senate', 'publication:fac_senate_committees',
        'publication:fac_senate_meetings', 'publication:fac_senate_meetings_minutes',
        'publication:fl_digital_postcards', 'publication:fl_studies', 'publication:florida_sentinel_bulletin',
        'publication:floridiana', 'publication:fs', 'publication:fs_pubs', 'publication:gandy',
        'publication:gandy_aerials', 'publication:gandy_collection', 'publication:gandy_commercial',
        'publication:gandy_environmental', 'publication:gandy_street', 'publication:graber',
        'publication:hillsborough_marriage', 'publication:hillsborough_naturalization', 'publication:inside_usf',
        'publication:institutional_research_acl', 'publication:institutional_research_docs_alcs_stats',
        'publication:jwb_meetings', 'publication:lgbtq', 'publication:npml_outreach_events_publicity',
        'publication:osterweil', 'publication:pizzo', 'publication:regional_chancellor',
        'publication:robertson_and_fresh', 'publication:saskia', 'publication:sg', 'publication:sg_exec_pubs',
        'publication:sg_leg_pubs', 'publication:sheet_music', 'publication:showmens_museum',
        'publication:speleo_spiel', 'publication:stpete_administration', 'publication:tampa_administration',
        'publication:thc', 'publication:twc', 'publication:university_administration',
        'publication:university_advancement', 'publication:university_governance', 'publication:university_history',
        'publication:university_history_campus', 'publication:university_history_campus_pubs',
        'publication:usf_archives_spc', 'publication:usf_libraries_spc', 'publication:usf_oracle',
        'publication:usf_oracle_spc', 'publication:usf_photo', 'publication:usf_photos',
        'publication:usf_student_pub', 'publication:usfsp_news_press_releases', 'publication:wagner',
        'publication:zion_cemetery', 'publication:zion_genealogical_records',
    ),
    '1fe454a4cdd1380a711': (  # publication:finley-letters
        'publication:finley-letters',
    ),
    '20fdaffba782ae30b4d': (  # Digitisation of medieval manuscripts
        'rosdok-collection:MaHandschriften', 'rosdok-collection:NachlassTychsen',
    ),
    '22530ba65e17f9c9b16': (  # rcin.org.pl:partnerCollections:iaie:ethnographicalarchive
        'rcin.org.pl:partnerCollections:iaie:ethnographicalarchive',
        'rcin.org.pl:partnerCollections:iaie:ethnographicalarchive:photogrophicmaterials',
    ),
    '22f16ad5a9cb2c02031': (  # publication:alumni_magazines
        'publication:alumni_magazines', 'publication:bot-corp-records', 'publication:findingaids',
        'publication:kendall_1930s', 'publication:kendall_1940s', 'publication:kendall_albums',
        'publication:kendall_collection', 'publication:kendall_images', 'publication:kendall_virtgall',
        'publication:maine_women_pubs_serial', 'publication:mainecampus', 'publication:manuscripts',
        'publication:mmb-ps', 'publication:nafoh_gallery', 'publication:ne_archives', 'publication:photographs',
        'publication:postcards', 'publication:postcards_virtgall', 'publication:president',
        'publication:sewell_aerial', 'publication:sewell_aerial_all', 'publication:sewell_aerial_penobscot',
        'publication:spec_photos', 'publication:univ_photos', 'publication:univ_publications',
        'publication:univ_publications_index', 'publication:univ_pubs_academic_affairs',
        'publication:univ_pubs_athletics', 'publication:univ_pubs_presidents_office',
        'publication:univ_pubs_student_life', 'publication:univ_pubs_vprdgs',
    ),
    '23c617cf15efffbec47': (  # Panoràmiques del Patrimoni Cultural
        'col_10687_10', 'col_10687_121401', 'col_10687_124266', 'col_10687_124268', 'col_10687_124269',
        'col_10687_124270', 'col_10687_124271', 'col_10687_124273', 'col_10687_124274', 'col_10687_124275',
        'col_10687_124277', 'col_10687_124279', 'col_10687_124280', 'col_10687_124282', 'col_10687_124283',
        'col_10687_124284', 'col_10687_124286', 'col_10687_124288', 'col_10687_124289', 'col_10687_124290',
        'col_10687_124291', 'col_10687_124292', 'col_10687_124293', 'col_10687_230556', 'col_10687_461584',
        'com_10687_121400', 'com_10687_122972', 'com_10687_124264', 'com_10687_124294', 'com_10687_24508',
        'com_10687_32124', 'com_10687_4', 'com_10687_53054', 'com_10687_6120', 'com_10687_9', 'com_10687_91366',
        'com_10687_91886',
    ),
    '25ca4900e6c7c073570': (  # shansi
        'shansi',
    ),
    '2628d8c1c3aa81f1d71': (  # Fotografías
        'col_11362_44794', 'com_11362_42729',
    ),
    '26e593ad79f5329b7bf': (  # 7374617475733D756E707562
        '7374617475733D756E707562', '74797065733D657068656D657261',
    ),
    '26e7c3329bf6daa5d69': (  # Al Sharq Newspaper
        'col_10576_29072', 'col_10576_33241', 'col_10576_51521', 'col_10576_5866', 'com_10576_29071',
    ),
    '27c2ffae14f1e3ba729': (  # publication:boardminutes
        'publication:boardminutes',
    ),
    '28add901d0a1bca8e09': (  # Dr. Mary T. Dooley Digital Map Library
        'publication:maps', 'publication:maps-becpw', 'publication:maps-mnco',
        'publication:maps-mnco-blue-earth-1971', 'publication:maps-mndot7-air-photos', 'publication:mn_co_air',
        'publication:mndot7-1975-75s-21', 'publication:mndot7-1986-86s-21', 'publication:mndot7-1987-87s-38',
        'publication:mndot7-1998-98s-97',
    ),
    '299fd8c8b0a456ad2ae': (  # Acervo de Fotos do Setor de Patrimônio Histórico
        'col_10183_29996', 'com_10183_40500', 'hdl_10183_29996', 'hdl_10183_8993', 'hdl_10183_8995',
    ),
    '29d668faa744351dabf': (  # collections:125
        'collections:125', 'collections:4',
    ),
    '2b4417084cd38d06873': (  # Alumni Publications
        'publication:alumni-publications',
    ),
    '2b8c6caad8ec3530f28': (  # Music Materials, Scores
        'dLibraDigitalLibrary:Zasoby:TreasuresOfChurchLibraries:MusicMaterials',
        'dLibraDigitalLibrary:kolekcje_uczestnikow:benedyktynki:Muzykalia',
    ),
    '2c0e87983f6b245435f': (  # Saint Mary’s Journal
        'col_01_26136', 'col_01_32275', 'com_01_4',
    ),
    '2cf2a4a9de35d1bd8c1': (  # All Theater Production Images
        'publication:production_images', 'publication:productions', 'publication:spartan_daily',
        'publication:spartan_daily_1930s', 'publication:spartan_daily_1940s', 'publication:spartan_daily_1950s',
        'publication:spartan_daily_1960s', 'publication:spartan_daily_1970s', 'publication:spartan_daily_1980s',
        'publication:spartan_daily_1990s', 'publication:spartan_daily_2000s', 'publication:spartan_daily_2010s',
        'publication:spartandaily',
    ),
    '2d82ae6d952f79a32ea': (  # hdl_10598_10194
        'hdl_10598_10194', 'hdl_10598_26193',
    ),
    '2f3c7eb913de9b05375': (  # publication:law_school_newspapers
        'publication:law_school_newspapers',
    ),
    '2nhihxmth4mwfyiuunth': (  # col_1885_107369
        'col_1885_107369', 'col_1885_114586', 'col_1885_140511', 'col_1885_165054', 'col_1885_227801',
        'col_1885_264022', 'com_1885_107367', 'com_1885_107368', 'com_1885_112364', 'com_1885_133765',
        'com_1885_140509', 'com_1885_143324', 'com_1885_165047', 'com_1885_165048', 'com_1885_165067',
        'com_1885_165259', 'com_1885_202226', 'com_1885_22', 'com_1885_227798', 'com_1885_4',
    ),
    '2s5m6brdcs6725x87xtz': (  # Board of Regents Minutes
        'col_11244_325266', 'col_11244_337813', 'com_11244_301245', 'com_11244_321763', 'com_11244_337812',
    ),
    '300b9d02527f44f3ecb': (  # publication:images
        'publication:images', 'publication:mac_suzuki', 'publication:txhealthfacpostc',
    ),
    '30b0fd7875d9b290af2': (  # ARC
        'ARC',
    ),
    '3105450374b2c0b3161': (  # Andrews University Video
        'publication:auvideo', 'publication:campus-news', 'publication:campus-news-images',
        'publication:campusministries', 'publication:luh', 'publication:luh-news', 'publication:luh-pubs',
    ),
    '319c66e31d70c2d3627': (  # publication:theatre_images
        'publication:theatre_images',
    ),
    '31a74f9a3f9166bd178': (  # University Archives
        'publication:archives_publications', 'publication:beacon', 'publication:facsen', 'publication:facsen_bills',
        'publication:facsen_execcom', 'publication:lib_sc', 'publication:pell_neh', 'publication:pell_neh_I',
        'publication:ts_rpts',
    ),
    '325dee0480a48ba60bf': (  # 7374617475733D696E7072657373
        '7374617475733D696E7072657373', '74797065733D6E65777370617065725F636C697070696E67',
    ),
    '32c1af8b5d0e1403c66': (  # fotos
        'fotos',
    ),
    '33b08e6ca3ce6b2bd3c': (  # Images
        'publication:baker_images', 'publication:law_archives', 'publication:law_archives_life',
        'publication:rwunews', 'publication:rwunews_weekatroger', 'publication:student_pubs',
        'publication:weekatroger_featured_news',
    ),
    '34a11689f7d0f9286dd': (  # UNF Campus Art Collections - Combined Gallery
        'publication:campus_art', 'publication:campus_art_collections', 'publication:eartha_materials',
        'publication:eartha_white', 'publication:flablue', 'publication:flablue_text', 'publication:newsletters',
        'publication:rghw', 'publication:rghw_business', 'publication:saffy', 'publication:saffy_text',
        'publication:sga', 'publication:sgaleg', 'publication:spec_coll', 'publication:special_collections',
        'publication:spinnaker', 'publication:spinnaker-images', 'publication:unf_hist',
    ),
    '3606d9230d1dc0ee13e': (  # publication:archives
        'publication:archives', 'publication:facsenate', 'publication:facsenatecomms', 'publication:facsenatedocs',
        'publication:facsenmins', 'publication:governance', 'publication:newsandevents', 'publication:reporter',
        'publication:ritarchives', 'publication:staffcouncil', 'publication:studentgovt', 'publication:unipubs',
    ),
    '36174eaa1e23509e9d5': (  # Student Newspapers
        'publication:student_newspapers', 'publication:uni_docs',
    ),
    '373aa6154c0f2f03777': (  # Administrative and Service Units Archive
        'publication:admin', 'publication:collegian', 'publication:collegian_1969-1969',
        'publication:collegian_1970-1979', 'publication:dc_hobo-day', 'publication:digital_collections',
        'publication:finding_aids', 'publication:registrar',
    ),
    '37f421ef8f3e13029ce': (  # publication:archive
        'publication:archive',
    ),
    '395cb8b7c2e0c403dfb': (  # Daily Egyptian Archives
        'publication:de', 'publication:ua', 'publication:ua_bcc',
    ),
    '3ac76da87f1be6ec310': (  # publication:alumni
        'publication:alumni', 'publication:commencement', 'publication:commencement_photos',
        'publication:gavelgazette', 'publication:history_archives', 'publication:lectures_pre_arch_archives',
        'publication:photos', 'publication:photos_nyls',
    ),
    '3cffdeda4aa3ef4cd56': (  # HELIN Consortium Archives
        'publication:archive', 'publication:as220_arts', 'publication:as220_gallery', 'publication:baxter_gallery',
        'publication:capeverdeancollection', 'publication:collegearchives', 'publication:helin_comm',
        'publication:loans_mo', 'publication:newspapers', 'publication:specialcollections',
        'publication:the_anchor',
    ),
    '3ddfaa3b6966a6768b3': (  # com_10062_4892
        'com_10062_4892',
    ),
    '3e74651c3a7a0956544': (  # Archives
        '50c0f666-49e7-4c6e-857d-72652e9dd097',
    ),
    '3f287b86cb39685eb7f': (  # Biblical Recorder
        'col_10339_112318', 'col_10339_48', 'col_10339_63998', 'col_10339_93533', 'com_10339_3',
    ),
    '4079a206fb3f5c2af5a': (  # University Archives
        'publication:University_Archives', 'publication:mathews_collection', 'publication:mbf_collection',
        'publication:menu_collection', 'publication:menucollection_denver', 'publication:menus',
        'publication:newspaper_pvd',
    ),
    '4136e4a4f6eb0e9242c': (  # map
        'map',
    ),
    '42e189ffa8d8bb2f955': (  # publication:board_of_regents
        'publication:board_of_regents', 'publication:intercom', 'publication:studentnewspapers',
    ),
    '4466d750439492c9b57': (  # University Archives
        'publication:archives', 'publication:bot', 'publication:bot_agendas', 'publication:dcamp',
        'publication:spub',
    ),
    '44da9ba32477629ca6d': (  # FKA
        'FKA',
    ),
    '4735a1725f7ec8a29ea': (  # m0399
        'm0399',
    ),
    '473b11c32060542d742': (  # Donación Rieta
        'col_10251_72038', 'com_10251_72037',
    ),
    '47c8649131c5197362b': (  # publication:archives
        'publication:archives',
    ),
    '4811677f2c931d0bb76': (  # Technic By Year
        'publication:rosetechnic', 'publication:rosethorn', 'publication:studentnewspaper', 'publication:technic',
        'publication:therosethorn',
    ),
    '491227629c44c2c7b13': (  # hdl_123456789_14
        'hdl_123456789_14', 'hdl_123456789_16', 'hdl_123456789_9',
    ),
    '4a968b956a0f6657f8e': (  # publication:beaumont_correspondence
        'publication:beaumont_correspondence',
    ),
    '4bf703b0c3208451249': (  # Type:Contribution
        'Type:Contribution',
    ),
    '4c02da7184e49eb0058': (  # com_10391_41
        'com_10391_41', 'com_10391_42', 'com_10391_5377',
    ),
    '4cb5a362a0d9c61c6a7': (  # tuflood
        'tuflood',
    ),
    '4d6b2138f9f10d1a1a1': (  # publication:images
        'publication:images', 'publication:mac_suzuki', 'publication:txhealthfacpostc',
    ),
    '4ec06045e6c629a3fe7': (  # College Publications
        'publication:codpubs', 'publication:courier', 'publication:marcom', 'publication:marcom-buildings',
        'publication:marcom-studentlife', 'publication:marcom-studentlife-events',
    ),
    '4yh23j3ttcumkut9ncyh': (  # usbdruckgrafiken
        'usbdruckgrafiken', 'zeugnissammlung',
    ),
    '50cd65f932d82506609': (  # Alaskan Missions Collection
        'publication:alaskan_mission', 'publication:archives', 'publication:archives_nwym',
        'publication:archives_pearson', 'publication:archives_people', 'publication:athletics',
        'publication:gfu_photos', 'publication:gfu_photos_1975_1979', 'publication:levi_pennington',
        'publication:museum', 'publication:museum_oregon', 'publication:nwym_photos', 'publication:perisho_alaska',
        'publication:the_crescent',
    ),
    '5105551bd181adf0762': (  # publication:ups_archives
        'publication:ups_archives',
    ),
    '52be64dd0b2094c979e': (  # Image Collection
        'image', 'newspaper_article',
    ),
    '53c47757db32eb0300b': (  # publication:theatre_imagearchive
        'publication:theatre_imagearchive', 'publication:theatre_images',
    ),
    '53e71f5fb2966c07629': (  # Evidence File 2: Academic Affairs (2020)
        'col_10057_12709', 'col_10057_12971', 'col_10057_30470', 'col_10057_30528', 'col_10057_30529',
        'col_10057_30602', 'col_10057_30610', 'col_10057_30622', 'col_10057_30857', 'com_10057_12061',
        'com_10057_12132', 'com_10057_12541', 'com_10057_14151', 'com_10057_16735', 'com_10057_29952',
        'com_10057_30467', 'com_10057_30468', 'com_10057_30510', 'com_10057_30512', 'com_10057_30514',
        'com_10057_30516', 'com_10057_30517', 'com_10057_30571', 'com_10057_30599', 'com_10057_30600',
        'com_10057_30604', 'com_10057_30720', 'com_10057_6370', 'com_10057_6375',
    ),
    '53fc94e94d32d9a092d': (  # col_20.500.14008_89730
        'col_20.500.14008_89730',
    ),
    '5416cbf1d4262ebcccf': (  # col_2152_21545
        'col_2152_21545', 'col_2152_29559', 'col_2152_29560', 'col_2152_29711', 'col_2152_38030', 'col_2152_38031',
        'col_2152_38804', 'col_2152_46929', 'com_2152_11681', 'com_2152_21544', 'com_2152_29557', 'com_2152_32261',
    ),
    '555e1eb5e128cfdc4f6': (  # col_1911_43629
        'col_1911_43629', 'col_1911_82046', 'col_1911_96594', 'col_1911_99828', 'col_1911_99831', 'com_1911_39097',
        'com_1911_64041', 'com_1911_64803', 'com_1911_69922', 'com_1911_71868', 'com_1911_75155',
    ),
    '55ff313f6bff468c2aa': (  # Daily Egyptian Archives
        'publication:de',
    ),
    '56cc905f68524c362c5': (  # School of Music Concert Programs
        'col_1773_25915', 'col_1773_38709', 'col_1773_55601',
    ),
    '57bc39ececc376af960': (  # University Archives
        'publication:archives', 'publication:forum', 'publication:lanthorn', 'publication:student-newspapers',
        'publication:tjc-pubs',
    ),
    '5812db7c9e39b46ce34': (  # Approved Course Proposal Library
        'publication:approved_course_proposals', 'publication:approved_course_proposals_face_to_face',
        'publication:approved_course_proposals_modality', 'publication:approved_course_proposals_synchronous',
        'publication:approved_course_proposals_title', 'publication:archives_faculty_sz',
        'publication:archives_images', 'publication:caa', 'publication:caa_minutes',
        'publication:caa_program_revisions_additions', 'publication:commencement', 'publication:den',
        'publication:eiu_housing_dining', 'publication:english_syllabi2006', 'publication:events',
        'publication:facgov', 'publication:hd_images_2010s', 'publication:housing_dining_images',
        'publication:lib_events', 'publication:press_releases_1981', 'publication:press_releases_1982',
        'publication:press_releases_1983', 'publication:press_releases_1984', 'publication:press_releases_1985',
        'publication:press_releases_1986', 'publication:press_releases_1987', 'publication:press_releases_1988',
        'publication:press_releases_1989', 'publication:press_releases_1990', 'publication:press_releases_1991',
        'publication:press_releases_1992', 'publication:press_releases_1993', 'publication:press_releases_1994',
        'publication:press_releases_1995', 'publication:press_releases_1996', 'publication:press_releases_1997',
        'publication:press_releases_1998', 'publication:press_releases_1999',
        'publication:press_releases_collection', 'publication:theatre', 'publication:theatre_productions',
        'publication:theatre_productions_images', 'publication:umac', 'publication:years_of_service',
    ),
    '5aa6e8e31ca9b94882b': (  # Accreditation & Assessment
        'publication:accreditation_assessment', 'publication:archives', 'publication:archives_guardian',
        'publication:archives_senate', 'publication:archives_ua_photos',
        'publication:archives_wsu_historical_photos', 'publication:emancipation', 'publication:emancipation_all',
        'publication:emancipation_montgomery', 'publication:guardian', 'publication:guardian_1970s',
        'publication:guardian_1980s', 'publication:naturalization', 'publication:naturalization_all',
        'publication:naturalization_auglaize', 'publication:naturalization_champaign',
        'publication:naturalization_clark', 'publication:naturalization_darke', 'publication:naturalization_greene',
        'publication:naturalization_logan', 'publication:naturalization_mercer', 'publication:naturalization_miami',
        'publication:naturalization_montgomery', 'publication:naturalization_shelby',
        'publication:office_of_marketing_comm', 'publication:restein_exhibitions',
        'publication:restein_exhibitions_all', 'publication:restein_gallery', 'publication:sca_all',
        'publication:sca_ua', 'publication:special', 'publication:special_ms1', 'publication:special_ms1_keywords',
        'publication:special_ms1_photographs', 'publication:special_ms223', 'publication:special_ms223_photographs',
        'publication:special_ms344_photographs', 'publication:student_affairs_comm',
        'publication:wsu_historical_photos', 'publication:wwi',
    ),
    '5aee1b3c3af727575e8': (  # Royal Commonwealth Society Essay Competition
        'col_1810_225222', 'col_1810_227602', 'col_1810_319896', 'com_1810_225198', 'com_1810_227599',
        'com_1810_227601',
    ),
    '5b37b760024647d32fc': (  # com_11285_621258
        'com_11285_621258',
    ),
    '5d3887acfcb76a89e26': (  # Archives
        'publication:archives', 'publication:asae', 'publication:athletics', 'publication:berman_museum',
        'publication:event_programs', 'publication:fmhw', 'publication:fmhw_assistant', 'publication:grizzlynews',
        'publication:independent', 'publication:independent_montgomery', 'publication:joyce_henry',
        'publication:lib_events', 'publication:news_magazines', 'publication:newspapers', 'publication:providence',
        'publication:special_collections', 'publication:trappe_historical', 'publication:ursinusiana',
        'publication:weekly',
    ),
    '5def7d71f5e393837c3': (  # alumni
        'alumni',
    ),
    '5e3c32daa26631180b5': (  # Maine Women Writers Collection
        'publication:mwwc', 'publication:neohc', 'publication:special', 'publication:wchc',
        'publication:wchc_photos', 'publication:wchc_photos_students', 'publication:wchc_photos_students1960s',
    ),
    '5f5c7ea53ed1c2c4c54': (  # Iconography
        'bcpw:18', 'bcpw:18:arch_sakr', 'bcpw:18:lotnicze', 'bcpw:18:sarp', 'bcpw:18:sarp_son2023',
        'bcpw:18:sarp_son2024', 'bcpw:BaruckiT',
    ),
    '5f63768d9f5c71320d3': (  # Ace Alagna Photograph Collection
        'publication:acealagna', 'publication:archives', 'publication:faculty-senate',
        'publication:faculty-senate-agendas', 'publication:italian-american-newpapers',
        'publication:italian-tribune-restricted', 'publication:la-voce-coloniale-restricted',
        'publication:setonian',
    ),
    '5f8c120e4184ce2732e': (  # The Stevens Family Papers
        'col_1813_52635', 'com_1813_3004', 'com_1813_51690',
    ),
    '5fd402771ef1f1c7a94': (  # Archives and Special Collections
        'publication:asc', 'publication:asc-archival', 'publication:asc-special', 'publication:bookplates',
        'publication:bookplates-artists', 'publication:diocese', 'publication:faculty-news',
        'publication:faculty-newsnotes', 'publication:newsreleases', 'publication:pcfrance',
        'publication:pcunitedstates', 'publication:postcards', 'publication:southern-cross',
        'publication:student-newspapers', 'publication:usdnews', 'publication:vista',
    ),
    '60f6285d890c6dfcbf8': (  # col_2437_418
        'col_2437_418', 'col_2437_6284', 'col_2437_81090', 'col_2437_88252', 'com_2437_378892', 'com_2437_6276',
    ),
    '610877e1e4c21027f2d': (  # Finding Aids
        'publication:crs_finding_aids', 'publication:crs_manuscripts_papers', 'publication:findingaids',
        'publication:hot', 'publication:hot_audio', 'publication:on_campus', 'publication:prentice_meador_papers',
        'publication:prentice_meador_papers_audio', 'publication:saunders', 'publication:saunders_audio',
        'publication:theophilus_brown_larimore_papers', 'publication:univ_arch',
    ),
    '62e00d5bc847f5b23ac': (  # col_10919_11385
        'col_10919_11385', 'com_10919_11335',
    ),
    '6333d6bcffb78f92839': (  # Items with collection equal to ana-dig-photos
        'collection:ana-dig-photos', 'collection:animationandcartoons', 'collection:archiveitpartners',
        'collection:archivo-nacional-aruba', 'collection:aruba', 'collection:community',
        'collection:computerchronicles', 'collection:election_2004', 'collection:fav-rr1955',
        'collection:fav-sciencefictionfan2008', 'collection:fav-super_yiayia', 'collection:feature_films',
        'collection:globallibraries', 'collection:link-tv-archive', 'collection:mosaic',
        'collection:moviesandfilms', 'collection:newsandpublicaffairs', 'collection:opensource_movies',
        'collection:prelinger', 'collection:tvprograms', 'collection:universal_newsreels',
        'collection:videogameprev', 'mediatype:image',
    ),
    '63c014a4c1d396c8100': (  # publication:archives
        'publication:archives', 'publication:archives-image-gallery', 'publication:archives-newspapers',
        'publication:archives-prescott-arizona-campus',
        'publication:archives-world-war-II-flight-technical-training', 'publication:avion',
        'publication:british-flying-training-school', 'publication:hazy-library',
    ),
    '63cb9984026d518baf1': (  # com_1853_7354
        'com_1853_7354',
    ),
    '64f03046e23c1f6931a': (  # Type = Ephemera
        '74797065733D657068656D657261',
    ),
    '6734f5b9ff61914dfd3': (  # Subject = G Geography. Anthropology. Recreation: GV Recreation Leisure: GV557-1198.995 Spo
        '7375626A656374733D47:4756:47563535372D313139382E3939355F53706F727473', '7375626A656374733D48:4831',
        '7375626A656374733D4C:4C42:4C4232333030', '7375626A656374733D4C:4C47', '74797065733D6E6577737061706572',
        '74797065733D756D735F6E657773',
    ),
    '678a6b3e1bbaa069aaf': (  # Music Materials, Scores
        'dLibraDigitalLibrary:Zasoby:TreasuresOfChurchLibraries:MusicMaterials',
        'dLibraDigitalLibrary:kolekcje_uczestnikow:benedyktynki',
        'dLibraDigitalLibrary:kolekcje_uczestnikow:benedyktynki:Muzykalia',
    ),
    '695491c10853c114d85': (  # col_i_6088
        'col_i_6088', 'com_i_2868', 'com_i_6080', 'com_i_6081',
    ),
    '6ac48167fa5c9d3780d': (  # Ace Alagna Photograph Collection
        'publication:acealagna', 'publication:archives', 'publication:faculty-senate',
        'publication:italian-american-newpapers', 'publication:italian-tribune-restricted',
        'publication:la-voce-coloniale-restricted', 'publication:setonian',
    ),
    '6c3f9cdf7c7bb8dc66c': (  # Communications and Media Relations
        'publication:communications-media', 'publication:news-releases',
        'publication:office-enrollment-and-marketing', 'publication:tuxedolit',
    ),
    '6c72973f1321c84ac3e': (  # Panola Oral Histories
        'p15279coll1',
    ),
    '6d5a9a54c46b085b851': (  # Bryant West Indies Slides Collection
        'publication:bwi-slide-collection', 'publication:centralfloridafuture', 'publication:cfm',
        'publication:cfm-bcu', 'publication:cfm-careyhand', 'publication:cfm-catalogs',
        'publication:cfm-ch-memoranda', 'publication:cfm-ch-memoranda-1922', 'publication:cfm-ch-memoranda-1923',
        'publication:cfm-ch-memoranda-1924', 'publication:cfm-ch-memoranda-1925',
        'publication:cfm-ch-memoranda-1926', 'publication:cfm-ch-memoranda-1927',
        'publication:cfm-ch-memoranda-1928', 'publication:cfm-ch-memoranda-1929',
        'publication:cfm-ch-memoranda-1930', 'publication:cfm-ch-memoranda-1931',
        'publication:cfm-ch-memoranda-1932', 'publication:cfm-ch-memoranda-1933',
        'publication:cfm-ch-memoranda-1934', 'publication:cfm-ch-memoranda-1935',
        'publication:cfm-ch-memoranda-1936', 'publication:cfm-ch-memoranda-1937',
        'publication:cfm-ch-memoranda-1938', 'publication:cfm-ch-memoranda-1939',
        'publication:cfm-ch-memoranda-1940', 'publication:cfm-ch-memoranda-1941',
        'publication:cfm-ch-memoranda-1942', 'publication:cfm-ch-memoranda-1943',
        'publication:cfm-ch-memoranda-1944', 'publication:cfm-ch-memoranda-1945',
        'publication:cfm-ch-memoranda-1946', 'publication:cfm-ch-register', 'publication:cfm-ch-register-vol03',
        'publication:cfm-ch-register-vol04', 'publication:cfm-ch-register-vol05',
        'publication:cfm-ch-register-vol06', 'publication:cfm-ch-register-vol07',
        'publication:cfm-ch-register-vol10', 'publication:cfm-ch-register-vol11',
        'publication:cfm-ch-register-vol12', 'publication:cfm-ch-register-vol13',
        'publication:cfm-ch-register-vol14', 'publication:cfm-ch-register-vol15',
        'publication:cfm-ch-register-vol16', 'publication:cfm-ch-register-vol17',
        'publication:cfm-ch-register-vol18', 'publication:cfm-ch-register-vol19',
        'publication:cfm-ch-register-vol20', 'publication:cfm-ch-register-vol21',
        'publication:cfm-ch-register-vol22', 'publication:cfm-ch-register-vol23',
        'publication:cfm-ch-register-vol24', 'publication:cfm-ch-register-vol25',
        'publication:cfm-ch-register-vol26', 'publication:cfm-ch-register-vol27',
        'publication:cfm-ch-register-vol28', 'publication:cfm-ch-register-vol29',
        'publication:cfm-ch-register-vol30', 'publication:cfm-ch-register-vol31',
        'publication:cfm-ch-register-vol32', 'publication:cfm-ch-register-vol33',
        'publication:cfm-ch-register-vol34', 'publication:cfm-images', 'publication:cfm-images-bcu',
        'publication:cfm-images-stetson', 'publication:cfm-images-stlukes', 'publication:cfm-newspapers',
        'publication:cfm-newspapers-ocls', 'publication:cfm-newspapers-rollins',
        'publication:cfm-newspapers-stetson', 'publication:cfm-partners', 'publication:cfm-sandspur',
        'publication:cfm-stcloudtribune', 'publication:cfm-stetsoncollegiate', 'publication:cfm-stlukes',
        'publication:cfm-texts-rollins', 'publication:cfm-vitalrecords', 'publication:flcensus-blockmaps-1980',
        'publication:florida-frontiers', 'publication:florida-frontiers-radio',
        'publication:fulbrightargentina2022-images', 'publication:fulbrightargentina2022-images-art',
        'publication:fulbrightargentina2022-images-buenosaires',
        'publication:fulbrightargentina2022-images-history', 'publication:fulbrightrussia2018-images',
        'publication:fullbrightargentina2023-images', 'publication:historicalbuildings', 'publication:lib-news',
        'publication:lib-social', 'publication:printmigrationnetwork-doopsgezinde-565a', 'publication:rosen_images',
        'publication:rosencollege_photos', 'publication:smca', 'publication:ucf-forum', 'publication:ucf-today',
        'publication:ucfreport', 'publication:univarchives', 'publication:univphotocollection',
        'publication:uspsstaffcouncil', 'publication:veteranshistoryproject',
        'publication:veteransoralhistories-notpow', 'publication:whlf2017photos', 'publication:wucf',
    ),
    '6e4595a5b3f605eaefa': (  # publication:alumnipublications
        'publication:alumnipublications', 'publication:campus_news', 'publication:digitalarchives',
        'publication:studentpublications', 'publication:thecollegian', 'publication:universityarchives',
        'publication:universitypublications',
    ),
    '6f98a8ecfd6701d34f9': (  # Board of Regents Documents
        'publication:bor', 'publication:coun_acad_dean', 'publication:dlsc_fa_fin_aid',
        'publication:dlsc_fa_oral_hist', 'publication:dlsc_mss', 'publication:dlsc_mss_fin_aid',
        'publication:dlsc_ua', 'publication:dlsc_ua_athletics', 'publication:dlsc_ua_fin_aid',
        'publication:dlsc_ua_newspaper', 'publication:dlsc_ua_records', 'publication:dlsc_ua_wku_gov',
        'publication:dlsc_wku_hist', 'publication:kmlsc_exhibits', 'publication:president',
        'publication:president_pubs', 'publication:sga', 'publication:staff_council', 'publication:stu_org',
        'publication:univ_senate', 'publication:wc_ky_marriage_records', 'publication:wku_timeline',
    ),
    '707203132a876d2106d': (  # Fisher: Correspondence
        'col_2440_10706', 'com_2440_14778', 'com_2440_79833',
    ),
    '721e6d76e8d2aff0c2c': (  # com_10474_3
        'com_10474_3',
    ),
    '727fb717ad7884638f9': (  # col_1911_21756
        'col_1911_21756', 'col_1911_43629', 'col_1911_43630', 'col_1911_68494', 'col_1911_72238', 'col_1911_77130',
        'col_1911_82046', 'com_1911_12341', 'com_1911_21840', 'com_1911_64803', 'com_1911_69922', 'com_1911_71868',
        'com_1911_9219',
    ),
    '73f54e67150106437bd': (  # Actualidad (Boletín)
        'col_10818_51176', 'col_10818_67278', 'com_10818_102', 'com_10818_11495', 'com_10818_11496',
        'com_10818_11497', 'com_10818_11503', 'com_10818_11514', 'com_10818_11522', 'com_10818_11758',
        'com_10818_11759', 'com_10818_11760', 'com_10818_11769', 'com_10818_11998', 'com_10818_40847',
        'com_10818_50518', 'com_10818_53235', 'com_10818_67651',
    ),
    '74ea586a01403fd1e30': (  # publication:BBC-SGA
        'publication:BBC-SGA', 'publication:com_archival', 'publication:dpla_harvest', 'publication:fiu_video',
        'publication:sga-archive', 'publication:sgabbc-archive', 'publication:sgabbc_images', 'publication:spcua',
        'publication:student_newspaper', 'publication:theatre_images', 'publication:theatre_photoarchive',
        'publication:theatreproductions',
    ),
    '75342eb9af2bcf7824f': (  # col_10217_184486
        'col_10217_184486', 'col_10968_3882', 'com_10217_100047',
    ),
    '76267cf400f992a2db0': (  # publication:bg-news
        'publication:bg-news', 'publication:bot', 'publication:campus', 'publication:monitor',
    ),
    '79a3e81a39ffbe15021': (  # publication:archives
        'publication:archives', 'publication:post_archives',
    ),
    '7bc56f87fb0e7f4ae9b': (  # UNH Publications and Documents
        'publication:admin', 'publication:archives', 'publication:athletic_photos',
        'publication:becky_field_photography', 'publication:bowen_collection', 'publication:bremer_pond',
        'publication:campus_journal', 'publication:faculty_senate', 'publication:faculty_senate_agendas_minutes',
        'publication:faculty_senate_motions', 'publication:ivorey_cobb', 'publication:jacobi',
        'publication:jacobi_ussr_central_asia', 'publication:moran_photos', 'publication:news',
        'publication:nh_history', 'publication:pond_photos', 'publication:team_photos',
        'publication:thenewhampshire', 'publication:tnh_digital_archive', 'publication:unh_today',
        'publication:unhfactbook',
    ),
    '7daeb562b3b4cdc69d9': (  # 4.C. Archivo de la Sociedad Colombina Onubense
        'com_10334_784',
    ),
    '7dd788b6de5004561d5': (  # publication:newsletters
        'publication:newsletters', 'publication:suarchives',
    ),
    '7e3912749987b05091b': (  # Fairleigh Dickinson University History
        'p15157coll5', 'p16322coll2', 'p16322coll7',
    ),
    '7e6e1c45f2d79ce40b7': (  # "Утро" (1906–1916 гг.)
        'col_1237075002_1282', 'col_1237075002_3148', 'com_1237075002_1281',
    ),
    '7e84e0930583c59995b': (  # First World War Central Power POW Camps
        'publication:wwi_pow_camps',
    ),
    '7f3ec14947b885ee7a8': (  # com_10797_246
        'com_10797_246',
    ),
    '7xztnyxjckrm9psuhf9n': (  # "Medical Bulletin"
        'col_11299_121985', 'col_11299_1308', 'col_11299_161593', 'col_11299_172724', 'col_11299_176896',
        'col_11299_189868', 'col_11299_2000', 'col_11299_212312', 'col_11299_219626', 'col_11299_220446',
        'col_11299_242978', 'col_11299_250572', 'col_11299_252827', 'col_11299_254583', 'col_11299_257723',
        'col_11299_262112', 'col_11299_266435', 'col_11299_268394', 'col_11299_3179', 'col_11299_3881',
        'col_11299_43231', 'col_11299_4522', 'col_11299_48023', 'col_11299_48701', 'col_11299_5', 'col_11299_50340',
        'col_11299_56', 'col_11299_57523', 'col_11299_62230', 'col_11299_948', 'com_11299_148009',
        'com_11299_149255', 'com_11299_208292', 'com_11299_212301', 'com_11299_212303', 'com_11299_213542',
        'com_11299_241870', 'com_11299_242971', 'com_11299_257206', 'com_11299_265507', 'com_11299_3',
        'com_11299_484', 'com_11299_48686',
    ),
    '80bf0cb2bdb30270188': (  # col_11125_27996
        'col_11125_27996', 'col_11125_55815', 'com_11125_1', 'com_11125_13903', 'com_11125_18181',
        'com_11125_21383', 'com_11125_23704', 'com_11125_28475', 'com_11125_30962', 'com_11125_53575',
    ),
    '83568c9890d3b3c0495': (  # col_10365_26091
        'col_10365_26091', 'col_10365_26092', 'col_10365_26093', 'col_10365_26151', 'col_10365_26167',
        'col_10365_26169', 'col_10365_26183', 'col_10365_28263', 'col_10365_31012', 'com_10365_26082',
        'com_10365_26083', 'com_10365_26084', 'com_10365_26087', 'com_10365_26147', 'com_10365_26149',
        'com_10365_28257', 'com_10365_31011',
    ),
    '8445f14ab08a4d74b74': (  # Administrative Offices, Programs and Centers
        'publication:acaffairs', 'publication:alumni', 'publication:alumni_towers', 'publication:archives',
        'publication:archives_alumnitowers', 'publication:archives_bk', 'publication:archives_img',
        'publication:archives_towers', 'publication:athletics', 'publication:cornell',
        'publication:cornell_ephemera', 'publication:costumes', 'publication:costumes_1870-1925',
        'publication:costumes_1926-present', 'publication:costumes_all', 'publication:health_sports_images',
        'publication:health_sports_images_all', 'publication:images_1961-1970', 'publication:images_1971-1980',
        'publication:images_1981-1990', 'publication:images_1991-2000', 'publication:images_2001-2010',
        'publication:images_2011-2020', 'publication:journal_his', 'publication:journals', 'publication:mus_gall',
        'publication:special_collections', 'publication:tancardinal', 'publication:theatre_dance',
        'publication:theatre_dance_gallery', 'publication:theatre_dance_images',
        'publication:theatre_dance_productions', 'publication:towers', 'publication:udoc',
    ),
    '85132d6b074b00545e4': (  # University Archives
        'publication:archives', 'publication:archives-mirror', 'publication:archives-pubs',
        'publication:archives-stag', 'publication:archives-studentnewspapers',
        'publication:archivesandspecialcollections', 'publication:artmuseum',
        'publication:fairfielduniversityartmuseum',
    ),
    '85f0845d09fd1d4095f': (  # UNF Campus Art Collections - Combined Gallery
        'publication:campus_art', 'publication:campus_art_collections', 'publication:eartha_materials',
        'publication:eartha_white', 'publication:flablue', 'publication:flablue_text', 'publication:newsletters',
        'publication:rghw', 'publication:rghw_business', 'publication:saffy', 'publication:saffy_text',
        'publication:sga', 'publication:sgaleg', 'publication:spec_coll', 'publication:special_collections',
        'publication:spinnaker', 'publication:spinnaker-images', 'publication:unf_hist',
    ),
    '864e5e70aee61e806e9': (  # Division of University Advancement
        'publication:advancement', 'publication:alumni', 'publication:csc', 'publication:cscminutes',
        'publication:senate', 'publication:senateminutes', 'publication:sharedgovernance', 'publication:somp',
        'publication:student-activities', 'publication:umc', 'publication:ur',
    ),
    '86cff6d456585b779e1': (  # publication:academic_year_music_programs
        'publication:academic_year_music_programs', 'publication:archives_dixon_media', 'publication:athletics',
        'publication:athletics_programs', 'publication:baseball', 'publication:basketball', 'publication:cedars',
        'publication:cedars_university_publications', 'publication:cedarville_facilities',
        'publication:cedarville_herald', 'publication:concert_and_recital_programs', 'publication:cross_country',
        'publication:dixon_collection', 'publication:event_archive', 'publication:homecoming',
        'publication:homecoming_image_galleries', 'publication:image_and_media_galleries',
        'publication:junior_and_senior_recitals', 'publication:local_newspapers', 'publication:mens_basketball',
        'publication:mens_basketball_statistics', 'publication:mens_cross_country', 'publication:mens_golf',
        'publication:mens_golf_statistics', 'publication:mens_soccer', 'publication:mens_soccer_statistics',
        'publication:mens_tennis', 'publication:mens_tennis_statistics', 'publication:mens_track_and_field',
        'publication:miscellaneous_facstaff_photographs', 'publication:miscellaneous_public_relations_photographs',
        'publication:music_and_worship_performances', 'publication:news_releases',
        'publication:ohio_independent_baptist', 'publication:personal_collections', 'publication:soccer',
        'publication:softball', 'publication:softball_statistics', 'publication:strobridge_collection',
        'publication:strobridge_images', 'publication:tennis', 'publication:track_and_field',
        'publication:university_publications', 'publication:volleyball', 'publication:volleyball_statistics',
        'publication:white_coat_ceremonies', 'publication:womens_basketball',
        'publication:womens_basketball_statistics', 'publication:womens_cross_country', 'publication:womens_soccer',
        'publication:womens_tennis', 'publication:womens_track_and_field',
        'publication:womens_track_and_field_statistics',
    ),
    '88ab2b50c4be4a1c6c8': (  # collection:BLACKA
        'collection:BLACKA', 'collection:BRAC', 'collection:CALLNO', 'collection:CWADP', 'collection:DFWCS',
        'collection:DJART', 'collection:KXAS', 'collection:LGBTC', 'collection:LYVAP', 'collection:TOHC',
        'collection:TXFC', 'collection:UNTART', 'collection:UNTCVA', 'collection:UNTOH', 'collection:WWII',
        'partner:UNTA', 'partner:UNTML', 'partner:UNTOHP',
    ),
    '89eb3909f2f2338b54f': (  # com_123456789_163523
        'com_123456789_163523',
    ),
    '89f67ecc6d95add887a': (  # col_1874_213365
        'col_1874_213365', 'col_1874_213366',
    ),
    '8a5b7ee7f0a72affa38': (  # ammft
        'ammft',
    ),
    '8dd761399de033ebd74': (  # Film Segment
        'film-segment',
    ),
    '8dd860d4ea704ec33c0': (  # NFGT Historic Image Gallery
        'publication:nfgt_general', 'publication:nfgt_images',
    ),
    '925ca1370e04b458bba': (  # p15897coll22
        'p15897coll22',
    ),
    '9309be00b4e3315c846': (  # Brock University Student Newspaper Collection, 1964-2020
        'col_10464_18421', 'col_10464_2683', 'com_10464_1140', 'com_10464_6430', 'com_10464_6431', 'com_10464_9',
    ),
    '9340525adcb700e5818': (  # publication:athletics
        'publication:athletics', 'publication:bell_collection', 'publication:chapman_events',
        'publication:cu_athletics', 'publication:dance_concerts', 'publication:homecoming',
        'publication:jonathan_silent_film', 'publication:music_performances', 'publication:music_programs',
        'publication:press_releases', 'publication:specialcollections', 'publication:temianka',
        'publication:temianka_correspondence', 'publication:theatre_photos', 'publication:theatre_productions',
        'publication:upi_photos', 'publication:wilkinson', 'publication:wla-ww2',
    ),
    '966fcd91a2df3d327eb': (  # DBC:dbc:DolnoslaskaDigitalLibrary:C_of_T_Ossolinski_Institute:RKPS_W
        'DBC:dbc:DolnoslaskaDigitalLibrary:C_of_T_Ossolinski_Institute:RKPS_W',
        'DBC:dbc:DolnoslaskaDigitalLibrary:PWR:Kat.Pwr', 'DBC:dbc:DolnoslaskaDigitalLibrary:PWR:Kat.Pwr:DokArch',
        'DolnoslaskaDigitalLibrary:C_of_T_Ossolinski_Institute:RKPS_W',
    ),
    '979a1f6209d74a08c9a': (  # RISD Archives
        'publication:archives', 'publication:bookcontest', 'publication:bookcontest6th2020',
        'publication:dazzleprints', 'publication:experimentalandfoundationstudies_history',
        'publication:experimentalandfoundationstudies_history_facultyandstaffportraits',
        'publication:gelmangallery', 'publication:naturelab_loeb_designscienceteachingcollection',
        'publication:picturecollection', 'publication:picturecollection_pulpfictioncovers',
        'publication:picturecollection_ripostcards', 'publication:picturecollection_victorianholidaycards',
        'publication:risdposters', 'publication:specialcollections_artistsbookscollection',
        'publication:specialcollections_reedandbarton',
        'publication:specialcollections_reedandbarton_technicaldrawings',
        'publication:specialcollections_zinecollection', 'publication:studentnewspapers',
        'publication:studentnewspapers_collection', 'publication:studentnewspapers_collection_blockprint',
        'publication:vmrc_graphicdesignarchive', 'publication:vmrc_swarovski', 'publication:woods-gerrygallery',
    ),
    '993bfac21be1ec01242': (  # BFA Exhibitions
        'publication:bfa_exhibits', 'publication:student_newspapers', 'publication:uni_docs',
    ),
    '9b66441c19f045dc3fd': (  # com_10211.2_311
        'com_10211.2_311', 'com_10211.3_124755', 'com_10211.3_124934', 'com_10211.3_124935',
    ),
    '9dfafe202fe224df1e0': (  # p15054coll1
        'p15054coll1',
    ),
    '9e49db930763cf32582': (  # All Theater Production Images
        'publication:production_images', 'publication:productions', 'publication:sjsu_erfa',
        'publication:spartan_daily', 'publication:spartan_daily_1930s', 'publication:spartan_daily_1940s',
        'publication:spartan_daily_1950s', 'publication:spartan_daily_1960s', 'publication:spartan_daily_1970s',
        'publication:spartan_daily_1980s', 'publication:spartan_daily_1990s', 'publication:spartan_daily_2000s',
        'publication:spartan_daily_2010s', 'publication:spartan_daily_2020s', 'publication:spartandaily',
    ),
    '9ee8fd05fec9ff060f5': (  # The Bulletin (formerly the Jefferson Medical College Alumni Bulletin)
        'publication:alumni_bulletin', 'publication:jefferson_alumni_faculty', 'publication:lecturetickets',
    ),
    'a0cb2f1a99014c672f9': (  # publication:archive
        'publication:archive',
    ),
    'a107a25b2f2cad6ca00': (  # publication:archives
        'publication:archives', 'publication:bulletin',
    ),
    'a129d1d7009974f22a8': (  # University Archives Online
        'publication:archives', 'publication:athletic_programs', 'publication:buildings',
        'publication:campus_bldgs', 'publication:commencement', 'publication:cosmosphere', 'publication:dodgecity',
        'publication:halstead', 'publication:harvey', 'publication:mens_basketball_photos', 'publication:mercury',
        'publication:military_science_rotc', 'publication:sen_all', 'publication:special_collections',
        'publication:stafford_county', 'publication:tiger_baseball', 'publication:tiger_daily_archive',
        'publication:tiger_sports', 'publication:tiger_weekly', 'publication:timjohnsoncollection',
        'publication:tj_postcards', 'publication:ua_photographs', 'publication:university_leader',
        'publication:womens_basketball_photos',
    ),
    'a139912e22a0da606de': (  # publication:admin
        'publication:admin', 'publication:registrar',
    ),
    'a299f78c0ca6b48837a': (  # publication:ww2letters
        'publication:ww2letters',
    ),
    'a34a0a03443ff3fa535': (  # In The News
        'publication:in_the_news', 'publication:lectures_and_presentations',
        'publication:lectures_pre_arch_archives', 'publication:photos', 'publication:press_releases',
        'publication:public_relations', 'publication:sibley_lectures',
    ),
    'a44625b7811bcad0415': (  # publication:Library_Challenge_Archive
        'publication:Library_Challenge_Archive', 'publication:west_bend_library_challenge',
    ),
    'a4468ac274f8ae99e90': (  # publication:archives
        'publication:archives', 'publication:senateminutes',
    ),
    'a4696f5d23fc0505790': (  # handwritten
        'handwritten',
    ),
    'a59902b72ea023cf7d6': (  # PCOM Yearbooks
        'publication:all_yearbooks', 'publication:do_2020', 'publication:images',
        'publication:pcom_archives_images', 'publication:virtual_yearbooks',
    ),
    'a6a09dbdc335cddb0d3': (  # 74797065733D657374616D7065
        '74797065733D657374616D7065',
    ),
    'a74308b34ef76f3973f': (  # UNI Alumni Association
        'publication:alumni', 'publication:amg', 'publication:athletics', 'publication:class_schedules',
        'publication:clothesline_photo_tshirts', 'publication:clothesline_photos',
        'publication:commencement_programs', 'publication:facsenate', 'publication:facsenate_agendas',
        'publication:facsenate_documents', 'publication:facsenate_minutes', 'publication:flw_images',
        'publication:grad_faculty', 'publication:gradcouncil', 'publication:gradcouncil_documents',
        'publication:gradcouncil_minutes', 'publication:ia_historical', 'publication:iowamaps',
        'publication:ire_documents', 'publication:panther_athletics', 'publication:pen',
        'publication:recital_programs', 'publication:rodmaps', 'publication:student_publications',
        'publication:the_photos', 'publication:uniphotos', 'publication:uniphotos_cam',
    ),
    'a7b6106cc16f6faf59c': (  # Malaspina College Bulletin (Nanaimo)
        'col_10613_1110',
    ),
    'a9336de25f274c320d1': (  # uwmphoto
        'uwmphoto',
    ),
    'aa34f59c26598773759': (  # publication:anglican_diocese_all_images
        'publication:anglican_diocese_all_images',
    ),
    'aa861a12b73a9d3349f': (  # History of Asbury Theological Seminary
        'publication:atshistory', 'publication:communications', 'publication:ecommonsatschapelservices',
        'publication:ecommonsevents', 'publication:holinessphotos', 'publication:images',
        'publication:rogerhedlundpapers', 'publication:specialcollections',
    ),
    'ab302ed65df3b51b22e': (  # Archives and University History
        'publication:arch_history', 'publication:archway', 'publication:bryant_goes_to_war',
        'publication:honorarydegreerecipients', 'publication:ww2civ', 'publication:ww2letters',
    ),
    'ab94ab82b3a6285d844': (  # publication:archives
        'publication:archives',
    ),
    'absw6u9r5ysg5p36bleo': (  # Menu Collection
        'publication:menus', 'publication:menus21c', 'publication:speccoll',
    ),
    'ad6dbb3bc1756ea9f8f': (  # publication:as220_arts
        'publication:as220_arts', 'publication:as220_gallery', 'publication:as220_painting',
        'publication:baxter_gallery', 'publication:capeverdeancollection', 'publication:collegearchives',
        'publication:newspapers', 'publication:smolski', 'publication:smolski_images_home',
        'publication:smolski_region', 'publication:specialcollections', 'publication:the_anchor',
    ),
    'adb1f08e73f73352cd7': (  # Commencement
        'publication:commencement_photos', 'publication:history_archives', 'publication:miner_court_of_appeals',
        'publication:photos_nyls', 'publication:steel_papers', 'publication:student_ledgers',
    ),
    'adf672304c8fb37fdb0': (  # Stony Brook Press [SBU]
        'col_1951_25510', 'col_1951_43838', 'col_1951_44270', 'col_1951_58363', 'col_1951_58490', 'col_1951_58520',
        'col_1951_58524', 'col_1951_6639', 'col_1951_66490', 'col_1951_76733', 'col_1951_82280', 'col_1951_85005',
        'col_1951_85561', 'col_1951_86483', 'com_1951_25236', 'com_1951_36462', 'com_1951_44269', 'com_1951_58362',
        'com_1951_58485', 'com_1951_58519', 'com_1951_66488', 'com_1951_66489', 'com_1951_67540', 'com_1951_85450',
    ),
    'aea9c724e28185f08d1': (  # Archives of Appalachia
        'publication:archives-appalachia', 'publication:etsu-news', 'publication:faculty-senate',
        'publication:faculty-senate-agendas-minutes', 'publication:governance-organizations',
        'publication:staff-senate-minutes', 'publication:umc',
    ),
    'af993342db424eb5cd2': (  # Exhibits
        'publication:exhibits', 'publication:historical-photograph-collection', 'publication:news_and_notes',
        'publication:newsletters', 'publication:tri-institutional-noon-recitals',
    ),
    'af996282fc145250b19': (  # Academic Program and Policy Committee Meeting Minutes, Agendas and Annual Reports
        'publication:appc', 'publication:facultyminutes', 'publication:provost',
    ),
    'ayovsuw5bsxptbpfqsde': (  # col_10822_1043059
        'col_10822_1043059', 'col_10822_1057169', 'col_10822_1061281', 'col_10822_557394', 'col_10822_558685',
        'com_10822_1061279', 'com_10822_1061280', 'com_10822_549417',
    ),
    'b0e450b4bde47d1bd48': (  # Gallery Shows
        'publication:artsgallery', 'publication:dory', 'publication:lca', 'publication:linfield_magazine',
        'publication:newspapers', 'publication:owha', 'publication:owha_ohphotos', 'publication:owha_photos',
        'publication:owha_willamette', 'publication:owha_willamette_ohphotos',
    ),
    'b358deda76b334a1c77': (  # Mugar Memorial Library and Branches
        'com_2144_69',
    ),
    'b41d94affce12d9f6cb': (  # Gerard Moerdyk Collection
        'col_2263_46311', 'com_2263_2401',
    ),
    'b5c715cc5aa3bf447d2': (  # CERN Pauli Archives records
        'cerncds:pauli-archive',
    ),
    'b60d592e22221d477c2': (  # publication:arbn
        'publication:arbn', 'publication:archives', 'publication:obu_alum', 'publication:ouachitalk',
        'publication:press_releases', 'publication:sam_and_jesse', 'publication:univ_offices',
        'publication:universityarchives', 'publication:vaught_archive', 'publication:wiley_records',
    ),
    'b9f46f272dbd19a67d0': (  # Campus Chronicle
        'publication:campus_chronicle',
    ),
    'ba39c5ce521069083c9': (  # Memphis World
        'col_10267_31334', 'col_10267_34390', 'col_10267_36090', 'com_10267_10971', 'com_10267_30956',
    ),
    'bb7834da33ebecae57b': (  # United States Forest Service Aerial Photographs
        'publication:aerial_photographs', 'publication:aerial_photographs_1932',
        'publication:aerial_photographs_1934', 'publication:aerial_photographs_1935',
        'publication:aerial_photographs_1937', 'publication:aerial_photographs_1938',
        'publication:aerial_photographs_1939', 'publication:asc', 'publication:asum', 'publication:asum_archive',
        'publication:asum_asc', 'publication:asum_minutes', 'publication:asum_resolutions',
        'publication:baucus_speeches', 'publication:facultysenate', 'publication:facultysenate_minutes',
        'publication:facultysenate_minutes_asc', 'publication:flynn_photographs_asc',
        'publication:mansfield_speeches', 'publication:mansfieldartifacts', 'publication:max_baucus_papers',
        'publication:mike_mansfield_papers', 'publication:montanaconstitution1972', 'publication:newsreleases',
        'publication:newsreleases_asc', 'publication:oral_histories', 'publication:photographs',
        'publication:registrar', 'publication:student_government', 'publication:studentnewspaper',
        'publication:studentnewspaper_asc', 'publication:syllabi', 'publication:syllabi1990-2010',
        'publication:syllabi2016-2020', 'publication:syllabi2021-2025', 'publication:syllabi2026-2030',
    ),
    'bd9ff6a76cf2857b801': (  # Eastern Health Board Minutes
        'col_10147_48883',
    ),
    'be0eb7992831985ee14': (  # Programs
        'publication:athl_programs', 'publication:athletics', 'publication:clemson_board',
        'publication:fball_prgms', 'publication:manuscripts', 'publication:sca', 'publication:spec_coll',
        'publication:ssm', 'publication:staff_senate', 'publication:strom', 'publication:tgc',
        'publication:tiger_newspaper', 'publication:trustees_minutes', 'publication:ua_pubs',
    ),
    'bf052d2f6affdf3afe6': (  # University Archives and Special Collections
        'publication:cwu_archives_special_collections', 'publication:cwu_athletics', 'publication:cwu_images',
        'publication:cwu_oralhist', 'publication:cwu_pubs', 'publication:cwu_sports_statistics',
        'publication:cwu_student_newspaper', 'publication:cwu_track_field', 'publication:cwura_interviews',
        'publication:edward_nolan', 'publication:epl', 'publication:fac_senate', 'publication:frederick_krueger',
        'publication:fsminutes', 'publication:glenn_hogue_photos', 'publication:john_foster_photos',
        'publication:local_collections', 'publication:nicholson_photographs', 'publication:regional_collections',
        'publication:rufus_woods', 'publication:suncadia', 'publication:thorp_mill',
        'publication:thorp_mill_preservation',
    ),
    'c22ae432d9aa2f60dbd': (  # Dirección General de Comunicación Universitaria
        'com_20.500.11799_21957',
    ),
    'c51bcd43a3026763e29': (  # MSS 0109 - George S. Messersmith papers
        'col_19716_5973', 'com_19716_10676', 'com_19716_10883', 'com_19716_24346', 'com_19716_9949',
    ),
    'c7d9e9a1ae26fbd93a7': (  # Faculty Exams: 1944-1973
        'publication:exams', 'publication:facmeeting', 'publication:lawnewsletters', 'publication:scinscriptions',
        'publication:specialcoll',
    ),
    'c8aef2bf63c5d0d987d': (  # Archives
        'publication:archives', 'publication:beacon-newspaper', 'publication:lecocq',
        'publication:northwesterncollegepubs', 'publication:nwcarchives', 'publication:regionalarchives',
        'publication:schuiteman', 'publication:schuitemanethiopia', 'publication:schuitemansudan',
        'publication:schuitemanzambia',
    ),
    'c8b2fae6e1b47ed79e3': (  # publication:msum_archives
        'publication:msum_archives', 'publication:studentnewspapers', 'publication:university_archives',
    ),
    'ca29664ebcf646d317a': (  # The Echo
        'publication:echo', 'publication:furman-magazine', 'publication:secession-editorials',
    ),
    'cacecac7a88bb44494e': (  # Publications
        'publication:campus-publications', 'publication:cp-newsletters', 'publication:cp-newspapers',
        'publication:culturalheritage', 'publication:long-island-black-and-white-aerial-photographs-collection',
        'publication:maps-and-aerials', 'publication:sbpress', 'publication:sbstatesman',
        'publication:university-history',
    ),
    'cafbc6e2f4a9a6e9a09': (  # publication:academic_affairs
        'publication:academic_affairs', 'publication:challenger', 'publication:crows_nest',
        'publication:fac_senate', 'publication:hillsborough_marriage', 'publication:inside_usf',
        'publication:jwb_meetings', 'publication:stpete_arts_alliance', 'publication:univ_admin_history',
        'publication:university_advancement', 'publication:university_governance', 'publication:university_history',
        'publication:university_history_campus', 'publication:university_history_campus_pubs',
        'publication:usfsp_news_press_releases',
    ),
    'cc3f72dedb33cbf9a02': (  # Abraham S. Fischler College of Education and School of Criminal Justice College Archive
        'publication:abe_pgcoursecatalogs', 'publication:athletics', 'publication:athletics_pressreleases',
        'publication:campuslife', 'publication:dateline_health', 'publication:dor_images',
        'publication:dor_images_all', 'publication:edu_archive', 'publication:hpd_coursecatalogs',
        'publication:nasfl', 'publication:nasflmuseum', 'publication:nsu_catalogs',
        'publication:nsudigital_flwimages', 'publication:nsudigital_harrison', 'publication:nsudigital_photos',
        'publication:nsudigital_university_publications', 'publication:sportsnews', 'publication:stuey_images',
        'publication:ug_convocation_images', 'publication:ug_convocation_images_2018',
        'publication:ug_convocation_images_all', 'publication:uschool', 'publication:uschool_litmag',
    ),
    'cd40c1ff1ef10f214e1': (  # Photographs
        'col_10315_1478', 'col_10315_8004', 'com_10315_580',
    ),
    'cxzlm4ex6pyvdw5kds6f': (  # Border Studies Archive
        'publication:bsa', 'publication:bwbsphotographs', 'publication:camargoreg', 'publication:cbarrera',
        'publication:church', 'publication:collegian', 'publication:curanderismo',
        'publication:curanderismophotographs', 'publication:ftbrown', 'publication:ftbrownrecords',
        'publication:hidalgocounty', 'publication:hidalgohist_aa', 'publication:kikadelagarza',
        'publication:kikadelagarzageneralnews', 'publication:kikadelagarzanews',
        'publication:miscphotosbrownsville', 'publication:miscphotosedinburg', 'publication:panamerican',
        'publication:rgvoralhistories', 'publication:rider', 'publication:som_arch', 'publication:speccoll',
        'publication:speccoll_ier', 'publication:speccoll_mg', 'publication:speccoll_pi',
        'publication:speccoll_ppor', 'publication:ua', 'publication:unipub', 'publication:unipubleg',
        'publication:utbmedia', 'publication:utbphotographs', 'publication:utpamedia',
        'publication:utpaphotographs', 'publication:utrgvphotographs', 'publication:utrgvphotographs-2018',
        'publication:utrgvphotographs-2019', 'publication:utrgvphotographs-2022',
        'publication:utrgvphotographs-2023', 'publication:utrgvphotographs-2024',
    ),
    'd04a7ff96073b45d51c': (  # John H. Herrick Archives
        'col_1811_24059', 'col_1811_50853', 'col_1811_51200', 'col_1811_62038', 'col_1811_74007', 'col_1811_90925',
        'com_1811_25152', 'com_1811_278', 'com_1811_31955', 'com_1811_45489', 'com_1811_51199', 'com_1811_74006',
    ),
    'd0f4df2c67edd1d2a6c': (  # dLibraDigitalLibrary:KatalogiKartkowe
        'dLibraDigitalLibrary:KatalogiKartkowe', 'dLibraDigitalLibrary:KatalogiKartkowe:KatalogAlfabetyczny',
    ),
    'd27cswvyzkks7w7ul3fs': (  # 4_MU-Albisteak
        'col_20.500.11984_1972', 'com_20.500.11984_7009',
    ),
    'd2cb94d245613ab70eb': (  # Geraldine Ferraro Archive
        'publication:geraldine_ferraro_archive', 'publication:pdd', 'publication:vice_presidential_campaign_1984',
    ),
    'd305486e43734f108b8': (  # Brooks Hays Materials
        'BrooksHays', 'Civilrights', 'FPH', 'SC_Gallery', 'p17212coll2', 'photocatalog',
    ),
    'd34d63d9b8c69b246a7': (  # p15255coll3
        'p15255coll3',
    ),
    'd45d6705ea3ba641907': (  # publication:thetrail
        'publication:thetrail', 'publication:thetrail_all', 'publication:ups_archives',
    ),
    'd4ccc15bd510677d117': (  # Faces of IMSA
        'publication:faces_of_imsa', 'publication:facmain', 'publication:itmc_foi',
        'publication:special_collections', 'publication:stures',
    ),
    'd553aee11591f139620': (  # წერილები ფრონტიდან
        'col_1234_166577', 'col_1234_17469', 'col_1234_3946', 'col_1234_582705', 'com_1234_3943',
    ),
    'd672cbfea5e69301088': (  # Malaspina College Bulletin (Nanaimo)
        'col_10613_1110', 'col_10613_42', 'com_10613_2638', 'com_10613_2695',
    ),
    'dc700d5f87e43e4a6d1': (  # CSUMB Photographs
        'publication:csumb-photos', 'publication:fortord', 'publication:fortord_images', 'publication:historical',
        'publication:hornbeck', 'publication:hornbeck_cgb_14', 'publication:hornbeck_cgb_16',
        'publication:hornbeck_usa_4', 'publication:newspapers',
    ),
    'dd93c9feb933546ec52': (  # Photographs
        'col_10315_1478', 'col_10315_8004', 'com_10315_580',
    ),
    'de6708a1f143d2e884c': (  # Activities
        'publication:activities', 'publication:breeze', 'publication:garber', 'publication:i19701979',
        'publication:i19801989', 'publication:i19901999', 'publication:i20002009', 'publication:i20102019',
        'publication:madisonmagazine', 'publication:oldcommonwealth', 'publication:rjs', 'publication:sull',
        'publication:ucm',
    ),
    'djfnlyh6xbnupwvrqw72': (  # Albuquerque Citizen, 1891-1906
        'publication:abq_citizen_news', 'publication:abq_eh_news', 'publication:abq_mj_news',
        'publication:archives', 'publication:bor_minutes', 'publication:brazil_slides',
        'publication:campus_publications', 'publication:cb_current_news', 'publication:collections',
        'publication:cswr_dc', 'publication:daily_lobo', 'publication:daily_lobo_1951-1960',
        'publication:daily_lobo_1961-1970', 'publication:daily_lobo_1971-1980', 'publication:daily_lobo_1981-1990',
        'publication:deming_headlight_news', 'publication:estancia_news', 'publication:faculty_senate',
        'publication:fs_minutes', 'publication:hillerman', 'publication:lv_gazette_news',
        'publication:lv_independiente_news', 'publication:lvdo_news', 'publication:lwl_news',
        'publication:news_minute', 'publication:nm_composer_archive', 'publication:nm_newspapers',
        'publication:nuevo_mexicano_news', 'publication:ofc_provost', 'publication:revista_taos_news',
        'publication:roswell_record_news', 'publication:sc_advocate_news', 'publication:sfnm_news',
        'publication:socorro_c_news', 'publication:tucumcari_news', 'publication:unm_regents',
        'publication:unm_weekly', 'publication:voz_dp_news',
    ),
    'e12c94a4d75c2e30699': (  # com_10361_18066
        'com_10361_18066', 'com_10361_18343', 'com_10361_18621', 'com_10361_18637', 'com_10361_18844',
        'com_10361_19418', 'com_10361_19490', 'com_10361_916',
    ),
    'e217a8603c2f5d4376d': (  # CERN Pauli Archives records
        'cerncds:pauli-archive',
    ),
    'e368060ce3fb48f03f8': (  # Great VCU Bike Race Book
        'publication:bike', 'publication:bike_images', 'publication:bov', 'publication:brandcenter_icons',
        'publication:com', 'publication:digital_collections', 'publication:far', 'publication:jwh',
        'publication:jwh_photos', 'publication:musc_student_recitals', 'publication:postcard', 'publication:ras',
        'publication:rca', 'publication:rcp', 'publication:rg60', 'publication:stu',
    ),
    'e3a4ca28a9349ff2fd6': (  # publication:arc
        'publication:arc', 'publication:itcimg', 'publication:itcimg2', 'publication:itcimppeople',
        'publication:scmessenger', 'publication:spelmanimg',
    ),
    'e4944347f97a196cc79': (  # Cultura
        'col_10915_27297', 'com_10915_25224', 'com_10915_27296',
    ),
    'e5971820d7236f12a25': (  # Reports of the Home Demonstration Agents (AZ 302)
        'col_10150_290808', 'col_10150_680438', 'com_10150_107278', 'com_10150_107812', 'com_10150_107836',
    ),
    'e5f95b3aff66e8c9dc6': (  # Colby Alumnus
        'publication:alumnus', 'publication:colbiana', 'publication:colbyecho', 'publication:colbymagazine',
        'publication:eastern_mail', 'publication:porter_collection', 'publication:special_collections',
        'publication:waterville', 'publication:waterville_mail',
    ),
    'e6444488cadbc108038': (  # publication:manuscript_coll
        'publication:manuscript_coll', 'publication:newspapers', 'publication:studentnewspapers',
    ),
    'e74676fc1ba3c6dcec5': (  # Photographs
        'col_10315_1478', 'col_10315_8004', 'com_10315_580',
    ),
    'e7ba69c09103a700c9d': (  # Worktype: Artwork
        'Worktype:Artwork',
    ),
    'ebfb8190d2a1f9a8534': (  # runyon
        'runyon', 'stardate',
    ),
    'ecec8383a6670776eb2': (  # Rysunek
        'DigitalLibraryZielonaGora:CultureFineArtsss:ryse', 'DigitalLibraryZielonaGora:Museum',
        'DigitalLibraryZielonaGora:Museum:DocuMat', 'DigitalLibraryZielonaGora:Museum:People',
        'DigitalLibraryZielonaGora:OtMiKonkRySat', 'DigitalLibraryZielonaGora:RegionalMaterialss',
        'DigitalLibraryZielonaGora:RegionalMaterialss:Czasregee',
        'DigitalLibraryZielonaGora:RegionalMaterialss:Pocze',
    ),
    'ed4f2a10e6032f0b0a7': (  # publication:newspapers
        'publication:newspapers', 'publication:owha_ohphotos', 'publication:owha_willamette_ohphotos',
    ),
    'ef43583d5c7425d6560': (  # Administrative Offices (EIU)
        'publication:admin', 'publication:archives_collections', 'publication:archives_images',
        'publication:boothlibrary_community_collaborations', 'publication:commencement', 'publication:den',
        'publication:digital_archives', 'publication:fac_senate', 'publication:facgov', 'publication:facsen_mins',
        'publication:haexhibits_2003_hogpentohearth', 'publication:haexhibits_2003_hogpentohearth_galleries',
        'publication:history_collections', 'publication:illinois_farm_history', 'publication:lib_events',
        'publication:little_theatre', 'publication:little_theatre_1968', 'publication:little_theatre_1969',
        'publication:little_theatre_1970', 'publication:little_theatre_1971', 'publication:little_theatre_shows',
        'publication:press_releases', 'publication:press_releases_1985', 'publication:press_releases_1986',
        'publication:press_releases_1991', 'publication:press_releases_1998',
        'publication:press_releases_collection', 'publication:stdtgov', 'publication:stdtgov_minutes',
        'publication:stdtgov_resolutions', 'publication:students', 'publication:umac',
        'publication:with_the_colors', 'publication:with_the_colors_1945', 'publication:years_of_service',
    ),
    'f30d4e8e0ae47bad6df': (  # Archival concert programs (MU)
        'col_10355_103921', 'col_10355_108145', 'col_10355_76062', 'col_10355_81001', 'com_10355_1297',
        'com_10355_15206', 'com_10355_485', 'com_10355_53753', 'com_10355_53773', 'com_10355_60693',
    ),
    'f442cf9c1e125c57c05': (  # publication:brownandgold
        'publication:brownandgold', 'publication:highlander', 'publication:specialcollections',
    ),
    'f4dd0f8feba35244f30': (  # publication:fs_approvedminutes
        'publication:fs_approvedminutes',
    ),
    'f506c7ddc2ac9249c9e': (  # publication:abq_citizen_news
        'publication:abq_citizen_news', 'publication:abq_eh_news', 'publication:abq_mj_news',
        'publication:archives', 'publication:bor_minutes', 'publication:brazil_slides',
        'publication:campus_publications', 'publication:cswr', 'publication:daily_lobo',
        'publication:daily_lobo_1951-1960', 'publication:daily_lobo_1961-1970', 'publication:daily_lobo_1971-1980',
        'publication:daily_lobo_1981-1990', 'publication:deming_headlight_news', 'publication:estancia_news',
        'publication:faculty_senate', 'publication:fs_minutes', 'publication:lv_gazette_news',
        'publication:lv_independiente_news', 'publication:lwl_news', 'publication:news_minute',
        'publication:nm_composer_archive', 'publication:nm_newspapers', 'publication:nuevo_mexicano_news',
        'publication:ofc_provost', 'publication:revista_taos_news', 'publication:roswell_record_news',
        'publication:sc_advocate_news', 'publication:sfnm_news', 'publication:tucumcari_news',
        'publication:unm_regents', 'publication:unm_weekly', 'publication:voz_dp_news',
    ),
    'f77764ee200824b9996': (  # col_10024_111885
        'col_10024_111885', 'col_10024_150967', 'col_10024_188215', 'col_10024_38071', 'col_10024_38074',
        'col_10024_95872', 'col_10024_97219', 'com_10024_109232', 'com_10024_111861', 'com_10024_111884',
        'com_10024_149498', 'com_10024_37381', 'com_10024_37993', 'com_10024_37995', 'com_10024_78800',
        'com_10024_97216',
    ),
    'f7a3efabb2b4be596b0': (  # University Archives
        'publication:archives', 'publication:current', 'publication:current1990s', 'publication:current2000s',
        'publication:newspaper', 'publication:photo', 'publication:photos',
    ),
    'f860bde705e68b0a796': (  # A Midwestern Review: A Magazine of the Arts and Issues at DePauw University
        'publication:amidwesternreview', 'publication:music', 'publication:music_recordings', 'publication:peeler',
        'publication:records_dept', 'publication:records_syllabi', 'publication:university_records',
        'publication:wgre',
    ),
    'f8m7rmoomvckbjgnmuwt': (  # Dalhousie University. Libraries. Library Council
        'col_10222_14627', 'col_10222_16037', 'col_10222_28094', 'com_10222_11480', 'com_10222_11488',
        'com_10222_13708', 'com_10222_21735',
    ),
    'fa9621202fc0bd3a29d': (  # Newsletters and Magazines
        'col_10294_16756', 'col_10294_16760', 'com_10294_16745',
    ),
    'fc54b4355c334f9a9be': (  # Photographs
        'publication:anthro_photos', 'publication:community', 'publication:kaiser', 'publication:mpla_committees',
        'publication:mpla_eos',
    ),
    'fc6dd04cf5c9b168826': (  # hdl_10514_19
        'hdl_10514_19',
    ),
    'fefe5c071e957339ace': (  # publication:accreditation_documents
        'publication:accreditation_documents', 'publication:accreditation_documents_2016',
        'publication:accreditation_documents_2020', 'publication:alumni_publications',
        'publication:cartwheelers_image_galleries', 'publication:centennial_cartwheelers', 'publication:homecoming',
        'publication:homecoming_image_galleries', 'publication:news_releases',
        'publication:student_media_contributions', 'publication:white_coat_ceremonies',
    ),
    'fngebvixvvxefcdjlphu': (  # publication:bulletin
        'publication:bulletin',
    ),
    'jm67gjrmxa6w5eg9ske8': (  # Образование
        'col_nls_10', 'col_nls_12', 'col_nls_8', 'com_nls_1', 'com_nls_2', 'com_nls_88',
    ),
    'm8igmk6gn9m7g4zbbcs9': (  # The Anchor
        'publication:anchor', 'publication:anchor_1920s', 'publication:archives_college_history',
        'publication:collection_registers', 'publication:college_publications', 'publication:hcn',
        'publication:hcn_1870s', 'publication:hcn_1880s', 'publication:hcn_1890s', 'publication:hcn_1900s',
        'publication:hcn_1910s', 'publication:hcn_1920s', 'publication:hcn_1930s', 'publication:hcn_1940s',
        'publication:hcn_1950s', 'publication:hcn_1960s', 'publication:hcn_1970s', 'publication:newspapers',
        'publication:oct', 'publication:vri', 'publication:vrp_1870s', 'publication:vrpapers',
    ),
    'oexu6ku2lzmwlpyzk2dj': (  # publication:photos
        'publication:photos', 'publication:photos2013', 'publication:photos2014', 'publication:universityarchives',
    ),
    'oqykwlgh5vtot2r5pmpz': (  # sc
        'sc',
    ),
    'oyinbcgfkwujnatagwpr': (  # col_20.500.12525_57
        'col_20.500.12525_57',
    ),
    'rxkvk85cjbozxgurx7si': (  # University Archives Photograph Collection
        'publication:archive-photos', 'publication:dakota-student', 'publication:daumier-prints',
        'publication:edwards-cartoons', 'publication:features-archive', 'publication:gf-city-photos',
        'publication:gf-collections', 'publication:gf-photos', 'publication:jsp-collections',
        'publication:jsp-earthworks', 'publication:jsp-papers', 'publication:lam', 'publication:lam-all',
        'publication:langer-papers', 'publication:mcdonald-cartoons', 'publication:nd-collections',
        'publication:nd-politics', 'publication:nd-politics-photos', 'publication:news-archive',
        'publication:performing-arts-photos', 'publication:pottery', 'publication:sun-multiples',
        'publication:u-letter', 'publication:uac-all', 'publication:und-alumni-review',
        'publication:und-alumni-review-all', 'publication:und-athletics', 'publication:und-athletics-pubs',
        'publication:und-commencement-programs', 'publication:und-events', 'publication:und-photos',
        'publication:und-senate-minutes', 'publication:writers-conference',
    ),
    't7u392zubyxskxn8qbgo': (  # Archives & Special Collections
        'publication:asc', 'publication:digitizednewspapers', 'publication:kabultimes', 'publication:oieportfolio',
        'publication:paul',
    ),
    'v2rknejitb8k6repmzzj': (  # archivos/cartasravi
        'archivos/cartasravi', 'archivos/quiroga', 'archivos/quiroga:CL6:14', 'archivos/quiroga:CL6:15',
        'archivos/quiroga:CL6:16', 'fotoexac',
    ),
    'wlh4o2474wl8kwcypeua': (  # publication:archive_iav
        'publication:archive_iav', 'publication:archive_image_sub', 'publication:archive_images',
        'publication:images_cc', 'publication:images_eve', 'publication:images_stu', 'publication:uniarchives',
    ),
    'xx98yyzrpz3e7wqb7nv7': (  # col_10024_2
        'col_10024_2', 'col_10024_5', 'col_11111_2520', 'col_11111_3774', 'col_11111_4224',
    ),
    'z3prdnah2ppvc7vukubs': (  # publication:ritarchives
        'publication:ritarchives',
    ),
}

# DataCite client ids (relationships.client.data.id, lower case) whose DOIs are not ingested.
HERITAGE_DATACITE_CLIENTS = frozenset({
    'ethz.da-ha',  # ETHZ Data Archive - Websites HA
    'ethz.epics-ba',  # E-Pics Bildarchiv
    'ethz.epics-pr',  # E-Pics Proveninez
    'ethz.gs',  # Graphische Sammlung
    'stdp.jwqddo',  # Hochschularchiv, SRP
    'tcd.digcolls',  # Digital Collections
    'tib.tub',  # Technische Universität Berlin – Universitätsbibliothek
    'ucdco.jjl',  # UCD Digital Library
})
