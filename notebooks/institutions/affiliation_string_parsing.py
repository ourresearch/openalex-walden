import re

def match_affiliation_to_institution_ids(aff_string):
    """
    Match a raw affiliation string to one or more known institution IDs
    using simple keyword and pattern heuristics.

    This method performs case-insensitive and partial-string matching
    (including Unicode-aware variants and common abbreviations) to
    map textual affiliations to canonical institution identifiers.

    Parameters
    ----------
    aff_string : str
        The raw affiliation string to match.

    Returns
    -------
    List[int]
        A de-duplicated list of matched institution IDs (`affs`), which may contain
        zero, one, or more numeric institution identifiers, depending
        on how many rules are matched.

    Examples
    --------
    >>> match_affiliation_to_institution_ids("Te Pūkenga Auckland Campus")
    [4387152882]

    >>> match_affiliation_to_institution_ids("Helsingin yliopisto")
    [133731052]

    >>> match_affiliation_to_institution_ids("Unknown Organization")
    []

    Notes
    -----
    - All string comparisons are performed in lowercase where appropriate.
    - The logic can be extended using regular expressions or fuzzy matching.
    - The returned institution IDs should correspond to canonical IDs used
      elsewhere in the system (e.g., OpenAlex or ROR).

    See Also
    --------
    - process_current_affiliation_with_ids
    """

    affs = []
    lower_aff_string = aff_string.lower()

    # Te Pukenga
    if 'kenga' in lower_aff_string:
        if any(word in aff_string for word in ['Te Pukenga','Te Pūkenga']):
            affs.append(4387152882)
    elif 'New Zealand' in aff_string:
        if any(word in aff_string for word in ['New Zealand Institute of Skills and Technology',
                                               'NZ Institute of Skills and Technology',
                                               'NZIST']):
            affs.append(4387152882)

    # University of Helsinki
    if 'elsingin yliopisto' in lower_aff_string:
        affs.append(133731052)

    # St. Xavier's University, Kolkata
    if 'xavier' in lower_aff_string:
        if any(word in aff_string
               for word in ["St. Xavier's University", "St. Xaviers University",
                            "St Xaviers University", "St Xavier's University"]):
            if 'Kolkata' in aff_string:
                affs.append(4400573289)

    # IIM Bodhgaya
    if 'bodhgaya' in lower_aff_string:
        if re.search('\\bIIM\\b', aff_string):
            affs.append(4400600926)
        elif any(word in lower_aff_string
               for word in ["indian institute of management"]):
            affs.append(4400600926)

    # Concordia University
    if 'concordia' in lower_aff_string:
        if any(word in lower_aff_string for word in ["université concordia", "universite concordia"]):
            affs.append(60158472)
        elif 'concordia university' in lower_aff_string:
            if any(word in aff_string.lower() for word in ["québec", "montréal", "quebec", "montreal",
                                                         "h3g ", "h3g1m8", "maisonneuve"]):
                affs.append(60158472)

    # University of Arizona
    if 'wyant college of optical science' in lower_aff_string:
        affs.append(138006243)

    # Cadi Ayyad University
    if 'cadi' in lower_aff_string:
        if any(word in aff_string
               for word in ['Cadi Ayyad','Cadi Ayad','Cadi-Ayyad','CADI AYYAD']):
            affs.append(119856527)

    if 'Marrakech' in aff_string:
        if any(word in aff_string for
               word in ['Caddi Ayyad','UCA']):
            affs.append(119856527)

    # Akademia Górniczo-Hutnicza University of Science and Technology
    if 'Poland' in aff_string:
        if re.search('\\bAGH\\b', aff_string):
            affs.append(686019)
        elif any(word in aff_string
                 for word in ['University of Science and Technolog', 'Akademia Górniczo-Hutnicza',
                              'Akademia Górniczo - Hutnicza']):
            affs.append(686019)

    if any(word in aff_string for word in ['Madrid','Spain','Espanha','Espana','España']):
        # Universidad Autónoma de Madrid
        if re.search('\\bUAM\\b', aff_string):
            affs.append(63634437)
        elif any(word in aff_string
                 for word in ['Universidad Autónoma de Madrid','Autónoma University of Madrid',
                              'Autonoma University of Madrid']):
            affs.append(63634437)

        # Carlos III University of Madrid
        if any(word in aff_string
               for word in ['University Carlos III','University Carlos III',
                            'Universitario Carlos III','Universidad Carlos III',
                            'Universidade Carlos III','UC3M','Juan March Institute of Social Science',
                            'Univ. Carlos III']):
            affs.append(50357001)

    # Universității Babeș-Bolyai
    if 'Bolyai' in aff_string:
        if any(word in aff_string for word in ['Universității Babeș-Bolyai','Bolyai University']):
            affs.append(3125347698)
        elif 'Cluj' in aff_string:
            if 'Napoca' in aff_string:
                affs.append(3125347698)

    if re.search('\\bBBU\\b', aff_string):
        if 'Cluj' in aff_string:
            if 'Napoca' in aff_string:
                affs.append(3125347698)

    # Benemérita Universidad Autónoma de Puebla
    if 'Puebla' in aff_string:
        if 'Benemérita' in aff_string:
            if any(word in aff_string
                   for word in ['Benemérita Universidad de Puebla','Benemérita Universidad Autonoma de Puebla',
                                'Benemérita Universidad Autónoma de Puebla']):
                affs.append(721619)
        elif 'Benemerita' in aff_string:
            if any(word in aff_string
                   for word in ['Benemerita Universidad de Puebla','Benemerita Universidad Autonoma de Puebla',
                                'Benemerita Universidad Autónoma de Puebla']):
                affs.append(721619)

    if 'University' in aff_string:
        # Boğaziçi University
        if 'Istanbul' in aff_string:
            if 'aziçi University' in aff_string:
                affs.append(4405392)
        elif any(word in aff_string
                 for word in ['Boğaziçi University','Bogaziçi University','Boğazici University',
                              'Bogazici University']):
            affs.append(4405392)

        # Brunel University - London
        if 'Brunel University' in aff_string:
            affs.append(59433898)

        # Western University (Canada)
        if 'Western University' in aff_string:
            if any(word in aff_string for word in ['London','Ontario','Canada','N6A 3K7']):
                affs.append(125749732)

    # Changchun University of Technology
    if 'Changchun' in aff_string:
        if any(word in aff_string for word in ['Changchun University of Technology','CCUT']):
            affs.append(4385474403)

    # Central China Normal University
    if 'Wuhan' in aff_string:
        if re.search('\\bCCNU\\b', aff_string):
            affs.append(40963666)
        elif 'Central China Normal University' in aff_string:
            affs.append(40963666)

    if 'China' in aff_string:
        # China Medical University (### SHOULD REPLACE CURRENT AFFS 91656880, 184693016, 4210126829,4210113902 if there)
        if 'China Medical University' in aff_string:
            if any(word in aff_string for word in ['Taichung','Taiwan','Taichung','TW','Yunlin','HsinChu']):
                if any(word in aff_string
                    for word in ['China Medical University Hospital',
                                    'China Medical University and Hospital',
                                    'China Medical University HsinChu Hospital']):
                            affs.append(4210126829)
                            affs.append(184693016)
                elif 'China Medical University Beigang Hospital' in aff_string:
                    affs.append(4210113902)
                    affs.append(184693016)
                else:
                    affs.append(184693016)
            elif any(word in aff_string for word in ['Shenyang','Liaoning']):
                affs.append(91656880)

    # China University of Petroleum, East China (### SHOULD REPLACE CURRENT AFFS 4210162190, 204553293 if there)
    if any(word in aff_string
           for word in ['China Petroleum University','University of Petroleum']):
        if any(word in aff_string
               for word in ['Qingdao','Shandong']):
            affs.append(4210162190)
        elif any(word in aff_string
               for word in ['Beijing']):
            affs.append(204553293)

    # Chinese Academy of Medical Sciences Peking Union Medical College
    if any(word in aff_string
           for word in ['Union Medical','Academy of Medic','Academy of Chinese Medical Sciences',
                        'Chinese Academy of Medicine Sciences','CAMS','PUMC','Fuwai Hospital',
                        'Chinese Academy Medical Sciences','Chinese Academic of Medical Science']):
        if any(word in aff_string for word in ['Kunming','Beijing','Beijng','Shenzhen','Bejing']):
            affs.append(200296433)
        elif(('Chinese Academy of Medical Sciences' in aff_string) &
             ('Dermatology Hospital' not in aff_string) &
             ('Cancer' not in aff_string)):
            affs.append(200296433)

    # Cornell University (need to remove Cornell College)
    if any(word in aff_string.lower() for word in ['cornell','boyce','weill','claudia cohen center']):
        if any(word in aff_string for word in ['Weill-Cornell','Weill Cornell','Boyce Thompson Institute',
                                               'WEILL CORNELL','Cornell Medical College','Weil Cornell',
                                               'Cornell University','Weill cornell','Boyce-Thompson Institute',
                                               'Cornell Weill','Cornell Univ','Weill Medical',
                                               'Boyce Thompson Institutute']):
            if ((re.search('\\bNY\\b', aff_string)) or
                (re.search('\\bUSA\\b', aff_string)) or
                ('United States' in aff_string) or
                ('New York' in aff_string) or
                (re.search('\\bNYC\\b', aff_string))):
                affs.append(205783295)
            elif any(word in aff_string for word in ['Doha','Qatar']):
                affs.append(4210152471)
            else:
                affs.append(205783295)
        elif ('cornell' in aff_string.lower()) | ('Ithica' in aff_string):
            affs.append(205783295)
        elif 'Weill Cornell Med' in aff_string:
            affs.append(205783295)
        elif (('Ronald O. Per' in aff_string) &
              ('man and Claudia Cohen Center for Reproductive Medicine' in aff_string)):
            affs.append(205783295)

    # Cukurova University
    if 'ukurova' in aff_string.lower():
        if any(word in aff_string.lower() for word in ['ukurova university','ukurovauniversity']):
            if any(word in aff_string for word in ['Adana','Turkey']):
                affs.append(55931168)

    # CY Cergy Paris University
    if 'Cergy' in aff_string:
        if any(word in aff_string for word in ['L2MGC','Université de Cergy','Université Cergy',
                                               'University of Cergy','University Cergy',
                                               'Cergy-Pontoise Univ','UnivCergyPontoise',
                                               'Universitede Cergy-Pontois','Universitè Cergy',
                                               'Universite de Cergy','université de Cergy-Pontoise',
                                               'UCP site Saint-Martin','Cergy Pontoise University',
                                               'Univ. de Cergy-Pontoise','Univ. Cergy-Pontoise',
                                               'Univ Cergy-Pontoise','ESSEC','EISTI','COMUE']):
            affs.append(4210142324)
        elif re.search('\\bUCP\\b', aff_string):
            affs.append(affs.append(4210142324))

    # Czech Technical University in Prague
    if 'Prague' in aff_string:
        if any(word in aff_string
               for word in ['CTU in Prague','CTU Prague','Czech Technical University','CVUT',
                            'University Centre for Energy Efficient Buildings of Technical University']):
            affs.append(44504214)
        elif re.search('\\bCTU\\b', aff_string):
            affs.append(44504214)
    elif 'CVUT v Praze' in aff_string:
        affs.append(44504214)

    # Czech University of Life Sciences Prague
    if 'Czech' in aff_string:
        if any(word in aff_string for word in ['Czech University of Life Sciences Prague',
                                               'CULS Prague','Life Sciences University Prague']):
            affs.append(205984670)

    # Częstochowa University of Technology
    if 'Czestochowa University of Technolog' in aff_string:
        affs.append(130294970)

    # Eastern Mediterranean University
    if any(word in aff_string for word in ['Eastern Mediterranean University',
                                           'Dogu Akdeniz University',
                                           'Eastern Meditteranean University']):
        if any(word in aff_string for word in ['Turkey','Mersin','Famagusta','TRNC','Cyprus']):
            affs.append(36515993)

    # École Polytechnique Fédérale de Lausanne
    if any(word in aff_string for word in ['Lausanne','Switzerland']):
        if any(word in aff_string for word in ['EPFL','École Fédérale Polytechnique de Lausanne',
                                               'École Polytechnique Fédérale de#N#Lausanne',
                                               'Swiss Federal Institute of Technology Lausanne',
                                               'EPF Lausanne','Ecole Polytechnique Fédérale Lausanne',
                                               'Brain Mind Institute','Epfl',
                                               'cole polytechnique fdrale de Lausanne',
                                               'École polytechnique fédérale de Lausanne',
                                               'Swiss Institute of Technology Lausanne',
                                               'Federal Institute of Technology, Lausanne',
                                               'Federal Institute of Technology in Lausanne',
                                               'ÉcolePolytehcniqueFédérale de Lausanne']):
            affs.append(5124864)
        elif (re.search('\\bEPF\\b', aff_string)) and ('Lausanne' in aff_string):
            affs.append(5124864)
        elif any(word in aff_string
                 for word in ['Swiss Federal Institute of Technology',
                              "L'Ecole polytechnique",
                              'Environmental Engineering Institute',
                              'Transport and Mobility Laboratory']) and ('Lausanne' in aff_string):
            affs.append(5124864)
        elif (re.search('\\bENAC\\b', aff_string)) and ('Swiss Federal Institute of Technology' in aff_string):
            affs.append(5124864)

    # Eötvös Loránd University
    if any(word in aff_string for word in ['Hungary','Budapest']):
        if any(word in aff_string for word in ['Eotvos Lordnd University','Eötvös University',
                                              'Eötvös Loránt University','Etvs Lornd University',
                                               'Eötvös Loránd University','University Eötvös Loránd',
                                               'Eotvos University',"EÃ¶tvÃ¶s LorÃ¡nd University",
                                               "Eï¿½tvï¿½s Lorï¿½nd University"]):
            affs.append(106118109)
        elif re.search('\\bELTE\\b', aff_string):
            affs.append(106118109)

    # Federal University of Rio Grande
    if any(word in aff_string for word in ['Brazil','Rio Grande','Brasil','BRASIL']):
        # Federal University Foundation of Rio Grande
        if (any(word in aff_string for word in ['Universidade Federal do Rio Grande',
                                                'Federal University of Rio Grande',
                                                'Universidade Federal Do Rio Grande',
                                                'Universidade Federal do Rio Grande',
                                                'Universidade Federal  do Rio Grande',
                                                'Universidade Federal de Rio Grande',
                                                'Universidade do Rio Grande',
                                                'UNIVERSIDADE FEDERAL DO RIO GRANDE']) and
            not any(word in aff_string for word in ['Sul','Norte','North','South'])):
            affs.append(126460647)
        elif re.search('\\bFURG\\b', aff_string):
            affs.append(126460647)
        elif (re.search('\\bIMEF\\b', aff_string)) and ('Universidade de Rio Grande' in aff_string):
            affs.append(126460647)

        # Federal University of Rio Grande do Sul
        if any(word in aff_string for word in ['Universidade Federal do Rio Grande do Sul','UFRGS',
                                               'Federal University of Rio Grande do Sul']):
            affs.append(130442723)

        # Federal University of Rio Grande do Norte
        if any(word in aff_string for word in ['Universidade Federal do Rio Grande do Norte','UFRN',
                                               'Federal University of Rio Grande do Norte']):
            affs.append(35046152)

    # Feng Chia University
    if any(word in aff_string for word in ['Feng Chia University','Feng-Chia University']):
        if any(word in aff_string for word in ['Taiwan','Taichung']):
            affs.append(4880106)

    # CY Cergy Paris University
    if 'Cergy' in aff_string:
        if any(word in aff_string for word in ['L2MGC','Université de Cergy','Université Cergy',
                                               'University of Cergy','University Cergy',
                                               'Cergy-Pontoise Univ','UnivCergyPontoise',
                                               'Universitede Cergy-Pontois','Universitè Cergy',
                                               'Universite de Cergy','université de Cergy-Pontoise',
                                               'UCP site Saint-Martin','Cergy Pontoise University',
                                               'Univ. de Cergy-Pontoise','Univ. Cergy-Pontoise',
                                               'Univ Cergy-Pontoise','ESSEC','EISTI','COMUE']):
            affs.append(4210142324)
        elif re.search('\\bUCP\\b', aff_string):
            affs.append(affs.append(4210142324))

    # French Institutions
    if any(word in aff_string for word in ['France','Villeurbanne','Lyon','Inserm','Tours','Reims']):

        if 'France' in aff_string:
            # Laboratoire Interdisciplinaire Sciences, Innovations, Sociétés
            if re.search('\\bLISIS\\b', aff_string):
                affs.append(4387156373)

            # Center for Training and Research in MathematIcs and Scientific Computing
            if re.search('\\bCERMICS\\b', aff_string):
                affs.append(4210128309)

            # Fédération de Recherche FCLAB
            if re.search('\\bFCLAB\\b', aff_string):
                affs.append(4210104533)
            
            # Bureau de Recherches Géologiques et Minières
            if re.search('\\bBRGM\\b', aff_string):
                affs.append(4210158893)
            elif any(word in aff_string.lower() for word in ['bureau de recherches géologiques et minières',
                                                             'bureau de recherches geologiques et minieres']):
                affs.append(4210158893)

            # Institut des Sciences de la Terre
            if re.search('\\bISTerre\\b', aff_string):
                affs.append(4210112832)
            elif re.search('\\bISTEEM\\b', aff_string):
                affs.append(4210112832)

            # Laboratoire Modélisation et Simulation Multi-Echelle
            if re.search('\\bMSME\\b', aff_string):
                affs.append(4210160945)

            # Laboratoire Ville Mobilité Transport
            if re.search('\\bLVMT\\b', aff_string):
                affs.append(4210152323)

            # Laboratoire d'Informatique Gaspard-Monge
            if re.search('\\bLIGM\\b', aff_string):
                affs.append(4210152518)

            # Laboratoire d'Ingénierie Circulation Transports
            if re.search('\\bLICIT\\b', aff_string):
                affs.append(4210111949)

            # Laboratoire d'Urbanisme
            if "Lab'Urba" in aff_string:
                affs.append(3019878935)

            # Laboratoire d’Analyse et de Mathématiques Appliquées
            if re.search('\\bLAMA\\b', aff_string):
                affs.append(4210144844)

            # Laboratory of Systems & Applications of Information & Energy Technologies
            if re.search('\\bSATIE\\b', aff_string):
                affs.append(4210136613)

            # Équipe de Recherche sur l’Utilisation des Données Individuelles en Lien avec la Théorie Économique
            if re.search('\\bERUDITE\\b', aff_string):
                affs.append(4210148266)

            # Dispositifs d'information et de communication à l'ère du numérique - Paris Ile-de-france
            if re.search('\\bDICEN-IDF\\b', aff_string):
                affs.append(4387154050)

            if any(word in aff_string for word in ['Créteil','Creteil']):
                # Institut de Recherche en Gestion
                if re.search('\\bIRG\\b', aff_string):
                    affs.append(4387154855)

            # Laboratoire d'électronique, systèmes de communication et microsystèmes
            if re.search('\\bESYCOM\\b', aff_string):
                affs.append(4387155313)

            # Unité Mixte de Recherche Epidémiologique et de Surveillance Transport Travail Environnement
            if re.search('\\bUMRESTTE\\b', aff_string):
                affs.append(4387155503)

            # Laboratoire Interdisciplinaire d'étude du Politique Hannah Arendt
            if re.search('\\bLIPHA\\b', aff_string):
                affs.append(4387155789)

            # Unité Mixte de Recherche en Acoustique Environnementale
            if re.search('\\bUMRAE\\b', aff_string):
                affs.append(4387154451)

            # Laboratoire de Psychologie et d’Ergonomie Appliquées
            if re.search('\\bLaPEA\\b', aff_string):
                affs.append(4387153539)
            elif re.search('\\bLAPEA\\b', aff_string):
                affs.append(4387153539)

            # Centre Nantais de Sociologie
            if re.search('\\bCENS\\b', aff_string):
                affs.append(4210153136)

            # Centre de Recherche en Cancérologie et Immunologie Intégrée Nantes Angers
            if re.search('\\bCRCNA\\b', aff_string):
                affs.append(4210092509)
            elif re.search('\\bCRCI2NA\\b', aff_string):
                affs.append(4210092509)

            # Chemistry And Interdisciplinarité, Synthesis, Analyze, Modeling
            if re.search('\\bCEISAM\\b', aff_string):
                affs.append(4210138474)

            # Federative Institute of Behavioral Addictions
            if re.search('\\bIFAC\\b', aff_string):
                affs.append(4210159912)

            # Fédération de Recherche PhotoVoltaïque
            if re.search('\\bFedPV\\b', aff_string):
                affs.append(4210161484)

            if 'Nantes' in aff_string:
                # Institut des Matériaux Jean Rouxel
                if re.search('\\bIMN\\b', aff_string):
                    affs.append(4210091049)

                # Droit et changement social
                if re.search('\\bDCS\\b', aff_string):
                    affs.append(4210100746)

                # Laboratoire de Planétologie et Géosciences
                if re.search('\\bLPG\\b', aff_string):
                    affs.append(4210146808)

                # Laboratoire de Thermique et Energie de Nantes
                if re.search('\\bLTN\\b', aff_string):
                    affs.append(4210109587)

                # École Centrale de Nantes
                if re.search('\\bECN\\b', aff_string):
                    affs.append(100445878)

                # Institut de Recherche en Génie Civil et Mécanique
                if re.search('\\bGeM\\b', aff_string):
                    affs.append(4210137520)

                # Centre François Viète
                if re.search('\\bCFV\\b', aff_string):
                    affs.append(4387153064)

            # Institute of Electronics and Telecommunications of Rennes
            if re.search('\\bIETR\\b', aff_string):
                affs.append(4210100151)

            # LabexMER
            if re.search('\\bLabexMER\\b', aff_string):
                affs.append(4210087604)

            # Laboratoire de Mathématiques Jean Leray
            if re.search('\\bLMJL\\b', aff_string):
                affs.append(4210153365)

            # Laboratoire de Physique Subatomique et des Technologies Associées
            if re.search('\\bSUBATECH\\b', aff_string):
                affs.append(4210109007)

            # Laboratoire de Planétologie et Géosciences
            if re.search('\\bLPGN\\b', aff_string):
                affs.append(4210146808)

            # Laboratoire de Psychologie des Pays de la Loire
            if re.search('\\bLPPL\\b', aff_string):
                affs.append(4210089331)

            # Laboratoire des Sciences du Numérique de Nantes
            if re.search('\\bLS2N\\b', aff_string):
                affs.append(4210117005)

            # PhysioPathologie des Adaptations Nutritionnelles
            if re.search('\\bPhAN\\b', aff_string):
                affs.append(4210162532)

            # Process Engineering for Environment and Food
            if re.search('\\bGEPEA\\b', aff_string):
                affs.append(4210148006)

            # Observatoire des Sciences de l'Univers Nantes Atlantique
            if re.search('\\bOSUNA\\b', aff_string):
                affs.append(4387153462)

            # Centre de Recherche en Archéologie, Archéosciences, Histoire
            if re.search('\\bCReAAH\\b', aff_string):
                affs.append(4387153012)

            if 'Rennes' in aff_string:
                # Espaces et Sociétés
                if re.search('\\bESO\\b', aff_string):
                    affs.append(4387153532)

            # Littoral, Environnement, Télédétection, Géomatique
            if re.search('\\bLETG\\b', aff_string):
                affs.append(4387153176)

            # Centre Atlantique de Philosophie
            if re.search('\\bCAPHI\\b', aff_string):
                affs.append(4387152714)

            # Centre de Recherche en Éducation de Nantes
            if re.search('\\bCREN\\b', aff_string):
                affs.append(4387152322)

            # Unité en Sciences Biologiques et Biotechnologies de Nantes
            if re.search('\\bUFIP\\b', aff_string):
                affs.append(4387154840)
            elif re.search('\\bUS2B\\b', aff_string):
                affs.append(4387154840)

            # Laboratoire de Linguistique de Nantes
            if re.search('\\bLLING\\b', aff_string):
                affs.append(4387152679)

            # Centre de Recherche sur les Identités, les Nations et l'Interculturalité
            if re.search('\\bCRINI\\b', aff_string):
                affs.append(4387153799)

            # LAMO - Littératures Antiques et Modernes
            if re.search('\\bLAMO\\b', aff_string):
                affs.append(4387152722)

            # Cibles et Médicaments des Infections et de l'Immunité
            if re.search('\\bIICiMed\\b', aff_string):
                affs.append(4387930219)

            # AGroecologies, Innovations & Ruralities
            if re.search('\\bAGIR\\b', aff_string):
                if any(word in aff_string for word in ['Amiens','Agent']):
                    pass
                else:
                    affs.append(4210111259)

            # Genomics and Biotechnology of the Fruits Laboratory
            if re.search('\\bGBF\\b', aff_string):
                affs.append(4210112218)

            # Institute of Fluid Mechanics of Toulouse
            if re.search('\\bIMFT\\b', aff_string):
                affs.append(4210110935)

            # Interuniversity Center of Materials Research and Engineering
            if re.search('\\bCIRIMAT\\b', aff_string):
                affs.append(4210135817)

            if re.search('\\bLGP\\b', aff_string):
                # Laboratoire Génie de Production
                if 'Tarbes' in aff_string:
                    affs.append(4210130517)

                # Laboratoire de Géographie Physique
                if 'Meudon' in aff_string:
                    affs.append(4210156486)

            if 'Toulouse' in aff_string:
                # Laboratoire de Génie Chimique
                if re.search('\\bLGC\\b', aff_string):
                    affs.append(4210087602)

                # Laboratoire Écologie Fonctionnelle et Environnement
                if re.search('\\bECOLAB\\b', aff_string):
                    affs.append(4210104620)

            # Laboratory for Analysis and Architecture of Systems
            if re.search('\\bLAAS\\b', aff_string):
                affs.append(190497903)

            # Laboratory on Plasma and Conversion of Energy
            if re.search('\\bLAPLACE\\b', aff_string):
                affs.append(4210120905)

            # École Nationale Supérieure d'Électrotechnique, d'Électronique, d'Informatique, d'Hydraulique et des Télécommunications
            if re.search('\\bENSEEIHT\\b', aff_string):
                affs.append(4387153255)

            # Dynamiques et écologie des paysages agriforestiers
            if re.search('\\bDYNAFOR\\b', aff_string):
                affs.append(4387155609)

            # CALMIP
            if re.search('\\bCALMIP\\b', aff_string):
                affs.append(4387153662)

            # Laboratoire d'Informatique de Paris Nord
            if re.search('\\bLIPN\\b', aff_string):
                affs.append(4210156583)

            if 'Paris' in aff_string:
                # Department of Mathematics and their Applications
                if re.search('\\bDMA\\b', aff_string):
                    affs.append(4210127506)

                # Institute of Ecology and Environmental Sciences Paris
                if re.search('\\bIEES\\b', aff_string):
                    affs.append(4210134846)

            # Institut de Biomécanique Humaine Georges Charpak
            if re.search('\\bIBHGC\\b', aff_string):
                affs.append(4210153840)

            # Laboratoire Analyse, Géométrie et Applications
            if re.search('\\bLAGA\\b', aff_string):
                affs.append(4210102686)

            # Laboratoire d’Ethologie Expérimentale et Comparée
            if re.search('\\bLEEC\\b', aff_string):
                affs.append(4210132853)

            # Laboratory for Vascular Translational Science
            if re.search('\\bLVTS\\b', aff_string):
                affs.append(4210134185)

            if 'Villetaneuse' in aff_string:
                # Laser Physics Laboratory
                if re.search('\\bLPL\\b', aff_string):
                    affs.append(4210129765)

                # CEPN - Centre d'Economie de l'Université Paris Nord
                if re.search('\\bCEPN\\b', aff_string):
                    affs.append(4387154094)

            # Nutritional Epidemiology Research Unit
            if re.search('\\bUREN\\b', aff_string):
                affs.append(4210154255)

            # Laboratoire Interuniversitaire Expérience, Ressources culturelles, Education
            if re.search('\\bEXPERICE\\b', aff_string):
                affs.append(4387152862)

            # Physiopathologie, cibles et thérapies de la polyarthrite rhumatoïde
            if re.search('\\bLi2P\\b', aff_string):
                affs.append(4387156304)

            # Unité transversale de recherche en psychogénèse et psychopathologie
            if re.search('\\bUTRPP\\b', aff_string):
                affs.append(4387155240)

            # Maison des Sciences de l'Homme Paris Nord
            if re.search('\\bMSHPN\\b', aff_string):
                affs.append(4387153895)

            # Unité de Recherche en Biomatériaux Innovant et Interfaces
            if re.search('\\bURB2i\\b', aff_string):
                affs.append(4387155632)

            # Chimie, Structures et Propriétés de Biomatériaux et d'Agents Thérapeutiques
            if re.search('\\bCSPBAT\\b', aff_string):
                affs.append(4387153222)

            # Centre of Research in Epidemiology and Statistics
            if re.search('\\bCRESS\\b', aff_string):
                affs.append(4387154308)

            # André Revuz Didactics Laboratory
            if re.search('\\bLDAR\\b', aff_string):
                affs.append(4210161656)

            # Centre d'Études et de Recherche en Thermique, Environnement et Systèmes
            if re.search('\\bCERTES\\b', aff_string):
                affs.append(4210093096)

            # East Paris Institute of Chemistry and Materials Science
            if re.search('\\bICMPE\\b', aff_string):
                affs.append(4210145484)

            # Laboratoire Cognitions Humaine et Artificielle
            if re.search('\\bCHArt\\b', aff_string):
                affs.append(4210117271)

            # Laboratoire Interuniversitaire des Systèmes Atmosphériques
            if re.search('\\bLISA\\b', aff_string):
                affs.append(4210135273)

            # Laboratoire Techniques, Territoires et Sociétés
            if re.search('\\bLISA\\b', aff_string):
                affs.append(4210159180)

            # Laboratoire de Recherche sur la Croissance Cellulaire, la Réparation et la Régénération Tissulaires
            if re.search('\\bCRRET\\b', aff_string):
                affs.append(4210139303)

            # Mondor Institute of Biomedical Research
            if re.search('\\bIMRB\\b', aff_string):
                affs.append(4210159433)

            # Vaccine Research Institute
            if re.search('\\bVRI\\b', aff_string):
                affs.append(4210119150)

            # Water, Environment and Urban Systems Laboratory
            if re.search('\\bLEESU\\b', aff_string):
                affs.append(4210126119)

            # École Nationale Vétérinaire d'Alfort
            if re.search('\\bENVA\\b', aff_string):
                affs.append(17606148)

            # Centre interdisciplinaire de recherche "Culture, éducation, formation, travail"
            if re.search('\\bCIRCEFT\\b', aff_string):
                affs.append(4387152707)

            # Dynamyc
            if re.search('\\bDYNAMYC\\b', aff_string):
                affs.append(4387156100)

            lower_aff_string = aff_string.lower()
            # Biologie du Chloroplaste et Perception de la Lumière chez les Microalgues
            if 'biologie du chloroplaste et perception de la lumière chez les microalgues' in lower_aff_string:
                affs.append(4389425217)
            elif re.search('\\bUMR[ -]?7141\\b', aff_string):
                affs.append(4389425217)

            # Saints-Pères Paris Institute for the Neurosciences (SPPIN)
            if any(word in lower_aff_string for
                word in ["Saints-Pères Paris Institute for the Neurosciences".lower(),
                            "Saints Pères Paris Institute for the Neurosciences".lower()]):
                affs.append(4387154016)
            elif re.search('\\bSPPIN\\b', aff_string):
                affs.append(4387154016)
            elif re.search('\\bUMR[ -]?8003\\b', aff_string):
                affs.append(4387154016)

            # NeuroDiderot
            if "neurodiderot" in lower_aff_string:
                affs.append(4387155530)
            elif re.search('\\bUMR[ -]?(1161|1129|1141)\\b', aff_string):
                affs.append(4387155530)

            # Centre de recherches interdisciplinaires sur les mondes ibériques contemporains
            if any(word in lower_aff_string for
                word in ["Center for Interdisciplinary Research on Contemporary Iberian Worlds".lower(),
                            "Centre de recherches interdisciplinaires sur les mondes ibériques contemporains".lower()]):
                affs.append(4389425383)
            elif re.search('\\bCRIMIC\\b', aff_string):
                affs.append(4389425383)

            # CENTRE DE RECHERCHE SUR L'EXTREME-ORIENT
            if "Centre de Recherche sur l'Extrême-Orient".lower() in lower_aff_string:
                affs.append(4389425318)
            elif re.search('\\bCREOPS\\b', aff_string):
                affs.append(4389425318)

            # CENTRE D'HISTOIRE  DU XIXE SIÈCLE
            if "Centre d'histoire du XIXe siècle" in lower_aff_string:
                affs.append(4389425375)
            elif re.search('\\bEA[ -]?3550\\b', aff_string):
                affs.append(4389425375)

            # Sciences et Technologies de la Musique et du Son
            if any(word in lower_aff_string for word in ["Science and Technology of Music and Sound Laboratory".lower(),
                                                        "Lab Sciences et Technologies de la Musique et du Son".lower()]):
                affs.append(4389425508)
            elif re.search('\\bSTMS\\b', aff_string):
                affs.append(4389425508)
            elif re.search('\\bUMR[ -]?9912\\b', aff_string):
                affs.append(4389425508)

            # Neurosciences Paris-Seine
            if any(word in lower_aff_string for word in ["neurosciences paris-seine", "neurosciences paris seine"]):
                affs.append(4389425265)
            elif re.search('\\bNPS\\b', aff_string) and re.search('\\bIBPS\\b', aff_string):
                affs.append(4389425265)
            elif re.search('\\bU[ -]?1130\\b', aff_string):
                affs.append(4389425265)
            elif re.search('\\bUMR[ -]?8246\\b', aff_string):
                affs.append(4389425265)

            # Fédération de Chimie et Matériaux de Paris-Centre
            if "Fédération de Chimie et Matériaux de Paris".lower() in lower_aff_string:
                affs.append(4389425323)
            elif re.search('\\bFCMat\\b', aff_string):
                affs.append(4389425323)
            elif re.search('\\bFR[ -]?2482\\b', aff_string):
                affs.append(4389425323)


            # Troubles psychiatriques et développement - GRC 15
            if "Troubles psychiatriques et développement".lower() in lower_aff_string:
                affs.append(4389425293)
            elif re.search('\\bPSYDEV\\b', aff_string):
                affs.append(4389425293)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?15\\b', aff_string):
                affs.append(4389425293)

            # Centre Expert en Endométriose - GRC 6
            if "Centre Expert en Endométriose".lower() in lower_aff_string:
                affs.append(4389425408)
            elif re.search('\\bC3E\\b', aff_string):
                affs.append(4389425408)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?(6|06)\\b', aff_string):
                affs.append(4389425408)

            # Robotique et Innovation Chirurgicale - GRC 33
            if any(word in lower_aff_string for word in ['GRC Robotique et Innovation Chirurgicale'.lower(),
                                                        'Robotics and Surgical Innovation'.lower()]):
                affs.append(4389425374)
            elif re.search('\\bGRC[ -]RIC\\b', aff_string):
                affs.append(4389425374)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?33\\b', aff_string):
                affs.append(4389425374)

            # Handicap moteur et cognitif et réadaptation - GRC 24
            if "Handicap moteur et cognitif et réadaptation".lower() in lower_aff_string:
                affs.append(4389425367)
            elif re.search('\\bHaMCRe\\b', aff_string):
                affs.append(4389425367)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?24\\b', aff_string):
                affs.append(4389425367)

            # Analyse, Recherche, Développement et Evaluation en Endourologie et Lithiase Urinaire - GRC 20
            if re.search('\\bARDELURO\\b', aff_string):
                affs.append(4389425347)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?20\\b', aff_string):
                affs.append(4389425347)

            # Fédération Ile de France de recherche sur l'environnement
            if re.search('\\bFR[ -]?3020\\b', aff_string):
                affs.append(4389425219)

            # Centre Roland Mousnier
            if "Centre Roland Mousnier".lower() in lower_aff_string:
                affs.append(4389425316)
            elif re.search('\\bUMR[ -]?8596\\b', aff_string):
                affs.append(4389425316)

            # Groupe de recherche interdisciplinaire sur les processus d'information et de communication
            if any(word in lower_aff_string for word in
                ["Groupe de Recherches Interdisciplinaires sur les Processus d'Information et de Communication".lower(),
                    "Interdisciplinary Research Group on Information and Communication Processes".lower()]):
                affs.append(4389425493)
            elif re.search('\\bGRIPIC\\b', aff_string):
                affs.append(4389425493)
            elif re.search('\\bEA[ -]?1498\\b', aff_string):
                affs.append(4389425493)

            # Adaptation Biologique et Vieillissement
            if "Adaptation Biologique et Vieillissement".lower() in lower_aff_string:
                affs.append(4389425262)
            elif re.search('\\bB2A\\b', aff_string):
                affs.append(4389425262)
            elif re.search('\\bUMR[ -]?8256\\b', aff_string):
                affs.append(4389425262)

            # CENTRE DE LINGUISTIQUE EN SORBONNE
            if "CENTRE DE LINGUISTIQUE EN SORBONNE".lower() in lower_aff_string:
                affs.append(4389425314)
            elif re.search('\\bCeLiSo\\b', aff_string):
                affs.append(4389425314)
            elif re.search('\\bEA[ -]?7332\\b', aff_string):
                affs.append(4389425314)

            # Equipe scientifique « Plasmas Froids » - Laboratoire de physique des plasmas
            if "Laboratoire de physique des plasmas".lower() in lower_aff_string:
                affs.append(4210151406)
            elif re.search('\\bLPP\\b', aff_string):
                affs.append(4210151406)

            # REanimation et Soins intensifs du Patient en Insuffisance Respiratoire aiguE - GRC 30
            if "REanimation et Soins intensifs du Patient en Insuffisance Respiratoire aiguE".lower() in lower_aff_string:
                affs.append(4389425468)
            elif re.search('\\bRESPIRE\\b', aff_string):
                affs.append(4389425468)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?30\\b', aff_string):
                affs.append(4389425468)

            # Biomarqueurs d’urgence et de réanimation - GRC 14
            if re.search('\\bBIOSFAST\\b', aff_string):
                affs.append(4389425476)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?14\\b', aff_string):
                affs.append(4389425476)

            # Groupe de REcherche en Cardio Oncologie - GRC 27
            if re.search('\\bGRECO\\b', aff_string):
                affs.append(4389425313)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?27\\b', aff_string):
                affs.append(4389425313)

            # Transplantation et Thérapies Innovantes de la Cornée - GRC 32
            if "Transplantation et Thérapies Innovantes de la Cornée".lower() in lower_aff_string:
                affs.append(4389425402)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?32\\b', aff_string):
                affs.append(4389425402)

            # Spectroscopies de Photoémission
            if re.search('\\bFR[ -]?2050\\b', aff_string):
                affs.append(4389425281)

            # MÉDIATIONS Sciences des lieux, sciences des liens
            if any(word in lower_aff_string for word in ["Laboratoire Médiations".lower(),
                                                        "Médiations - Sciences des lieux".lower()]):
                affs.append(4389425385)

            # Maladies génétiques d’expression pédiatrique
            if "Maladies génétiques d’expression pédiatrique".lower() in lower_aff_string:
                affs.append(4389425373)
            elif re.search('\\b(UMRS?[_ -]?[_ -]?S?|U)[ ]?933\\b', aff_string):
                affs.append(4389425373)

            # VOIX ANGLOPHONES : LITTÉRATURE ET ESTHÉTIQUE
            if "Voix Anglophones Littérature et Esthétique".lower() in lower_aff_string:
                affs.append(4389425434)
            elif re.search('\\bVALE\\b', aff_string):
                affs.append(4389425434)

            # Paris Centre for Quantum Computing
            if "Paris Center for Quantum".lower() in lower_aff_string:
                affs.append(4389425500)
            elif re.search('\\bFR[ -]?3640\\b', aff_string):
                affs.append(4389425500)

            # MICROSCOPIE FONCTIONNELLE DU VIVANT
            if "Microscopie Fonctionnelle du Vivant".lower() in lower_aff_string:
                affs.append(4389425365)
            elif re.search('\\bGDR[ -]?2588\\b', aff_string):
                affs.append(4389425365)

            # PremUP
            if re.search('\\bPremUP\\b', aff_string):
                affs.append(4389425249)

            # Interface Neuro-machine - GRC 23
            if any(word in lower_aff_string for word in ["Interface Neuro machine".lower(),
                                                        "Brain Machine Interface".lower()]):
                affs.append(4389425384)
            elif re.search('\\bNeurON\\b', aff_string):
                affs.append(4389425384)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?23\\b', aff_string):
                affs.append(4389425384)

            # Groupe de recherche clinique Amylose AA Sorbonne Université - GRC 28
            if "Groupe de recherche clinique amylose AA".lower() in lower_aff_string:
                affs.append(4389425424)
            elif re.search('\\bGRC AA SU\\b', aff_string):
                affs.append(4389425424)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?28\\b', aff_string):
                affs.append(4389425424)

            # Biomarqueurs Théranostiques des Cancers Bronchiques Non à Petites Cellules - GRC 4
            if re.search('\\btheranoscan\\b', lower_aff_string):
                affs.append(4389425474)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?(4|04)\\b', aff_string):
                affs.append(4389425474)

            # Tumeurs Thyroïdiennes - GRC 16
            if "Tumeurs Thyroïdiennes".lower() in lower_aff_string:
                affs.append(4389425398)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?16\\b', aff_string):
                affs.append(4389425398)

            # Groupe de Recherche Clinique en Anesthésie Réanimation médecine PEriopératoire - GRC 29
            if re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?29\\b', aff_string):
                affs.append(4389425478)

            # Groupe d’Étude sur l’HyperTension Intra Crânienne idiopathique - GRC 31
            if re.search('\\bE-HTIC\\b', aff_string):
                affs.append(4389425339)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?31\\b', aff_string):
                affs.append(4389425339)

            # Équipe Littérature et Culture italiennes
            if "Équipe Littérature et Culture Italiennes".lower() in lower_aff_string:
                affs.append(4389425225)
            elif re.search('\\bELCI\\b', aff_string):
                affs.append(4389425225)
            elif re.search('\\bEA[ -]?1496\\b', aff_string):
                affs.append(4389425225)

            # CENTRE DE RECHERCHE EN LITTERATURE COMPAREE
            if "centre de recherche en littérature comparée".lower() in lower_aff_string:
                affs.append(4389425459)
            elif re.search('\\bCRLC\\b', aff_string) and ('Sorbonne' in aff_string):
                affs.append(4389425459)
            elif re.search('\\bEA[ -]?4510\\b', aff_string):
                affs.append(4389425459)

            # SENS, TEXTE, INFORMATIQUE, HISTOIRE
            if "SENS, TEXTE, INFORMATIQUE, HISTOIRE".lower() in lower_aff_string:
                affs.append(4389425349)
            elif re.search('\\bSTIH\\b', aff_string):
                affs.append(4389425349)

            # Civilisations et littératures d'Espagne et d'Amérique du Moyen-Age aux Lumières
            if re.search('\\bEA[ -]?4083\\b', aff_string):
                affs.append(4389425437)

            # Centre de Recherche en Myologie
            if "Centre de Recherche en Myologie".lower() in lower_aff_string:
                affs.append(4389425387)
            elif re.search('\\b(UMRS?[_ -]?[_ -]?S?|U)[ ]?974\\b', aff_string):
                affs.append(4389425387)

            # INSTITUT DES SCIENCES DU CALCUL ET DES DONNEES
            if any(word in lower_aff_string for word in ["Institut des Sciences du Calcul et des Donnees".lower(),
                                                        "Institute of Computing and Data Sciences".lower()]):
                affs.append(4389425457)
            elif re.search('\\bISCD\\b', aff_string):
                affs.append(4389425457)

            # Fédération de recherche : Interactions fondamentales
            if re.search('\\bFR[ -]?2687\\b', aff_string):
                affs.append(4389425423)

            # Sciences, éthique, société
            if any(word in lower_aff_string for word in ["Science Norms Democracy".lower(),
                                                        "Sciences, Normes, Démocratie".lower()]):
                affs.append(4389425355)
            elif re.search('\\bUMR[ -]?8011\\b', aff_string):
                affs.append(4389425355)

            # La médecine de la femme et de l’enfant assistée par l’image - GRC 26
            if re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?26\\b', aff_string):
                affs.append(4389425216)

            # Drépanocytose : groupe de Recherche de Paris – Sorbonne Université - GRC 25
            if "Drépanocytose: groupe de Recherche de Paris".lower() in lower_aff_string:
                affs.append(4389425492)
            elif re.search('\\bDREPS\\b', aff_string):
                affs.append(4389425492)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?25\\b', aff_string):
                affs.append(4389425492)

            # Institut de Chimie Moléculaire de Paris Centre, organique, inorganique et biologique
            if re.search('\\bFR[ -]?2769\\b', aff_string):
                affs.append(4389425255)

            # Représentations et Identités. Espaces Germanique, Nordique et Néerlandophone
            if re.search('\\bREIGENN\\b', aff_string):
                affs.append(4389425433)

            # Réseau thématique de recherche avancée en sciences mathématiques
            if "Fondation Sciences mathématiques de Paris".lower() in lower_aff_string:
                affs.append(4389425253)

            # ETUDE ET EDITION DE TEXTES MEDIEVAUX
            if "Étude et Édition de Textes Médiévaux".lower() in lower_aff_string:
                affs.append(4389425483)

            # HISTOIRE ET DYNAMIQUE DES ESPACES ANGLOPHONES: DU RÉEL AU VIRTUEL
            if any(word in lower_aff_string for word in ["History and Dynamics of English Speaking Spaces".lower(),
                                                        "Histoire et Dynamique des Espaces Anglophones".lower()]):
                affs.append(4389425370)
            elif re.search('\\bHDEA\\b', aff_string):
                affs.append(4389425370)
            elif re.search('\\bEA[ -]?4086\\b', aff_string):
                affs.append(4389425370)

            # Institut de la Mer de Villefranche
            if any(word in lower_aff_string for word in ["Institut de la Mer de Villefranche".lower(),
                                                        "Villefranche Sea Institute".lower()]):
                affs.append(4389425234)
            elif re.search('\\bIMEV\\b', aff_string):
                affs.append(4389425234)
            elif re.search('\\bFR[ -]?3761\\b', aff_string):
                affs.append(4389425234)

            # Observatoire des sciences de l'Univers Paris-Centre Ecce Terra
            if 'Ecce Terra' in aff_string:
                affs.append(4389425445)
            elif re.search('\\b(UMS|UAR)[ -]?3455\\b', aff_string):
                affs.append(4389425445)
            elif re.search('\\bUMS[ -]?244\\b', aff_string):
                affs.append(4389425445)

            # Fédération de Recherche sur l'Energie Solaire
            if "Fédération de Recherche sur l’Energie Solaire".lower() in lower_aff_string:
                affs.append(4389425322)
            elif re.search('\\bFédESol\\b', aff_string):
                affs.append(4389425322)
            elif re.search('\\bFR[ -]?3344\\b', aff_string):
                affs.append(4389425322)

            # ENZYMOLOGIE DE L'ARN
            if "Enzymologie de l'ARN".lower() in lower_aff_string:
                affs.append(4389425420)
            elif re.search('\\bUR[ -]?(6|06)\\b', aff_string):
                affs.append(4389425420)

            # Laboratoire d'Informatique Médicale et d'Ingénieurie des Connaissances en e-Santé
            if "Laboratoire d’Informatique Médicale et d’Ingénierie des Connaissances en e-Santé".lower() in lower_aff_string:
                affs.append(4389425324)
            elif re.search('\\bLIMICS\\b', aff_string):
                affs.append(4389425324)
            elif re.search('\\b(UMRS?[_ -]?[_ -]?S?|U)[ ]?1142\\b', aff_string):
                affs.append(4389425324)

            # Institut Parisien de Chimie Physique et Théorique
            if "Institut Parisien de Chimie Physique et Théorique".lower() in lower_aff_string:
                affs.append(4389425456)
            elif re.search('\\bIP2CT\\b', aff_string):
                affs.append(4389425456)
            elif re.search('\\bFR[ -]?2622\\b', aff_string):
                affs.append(4389425456)

            # PRODUCTION ET ANALYSE DES DONNEES EN SCIENCES DE LA VIE ET EN SANTE
            if "Production et Analyse de données en Sciences de la Vie et en Santé".lower() in lower_aff_string:
                affs.append(4389425403)
            elif re.search('\\bUMS PASS\\b', aff_string):
                affs.append(4389425403)

            # NUTRITION ET OBESITES : APPROCHES SYSTEMIQUES (NUTRIOMIQUE)
            if 'nutriomics' in lower_aff_string:
                affs.append(4210096450)
            elif re.search('\\b(UMRS?[_ -]?[_ -]?S?|U)[ ]?1269\\b', aff_string):
                affs.append(4210096450)

            # HISTOIRE ET ARCHEOLOGIE MARITIMES
            if "d'histoire et d'archéologie maritime".lower() in lower_aff_string:
                affs.append(4389425223)
            elif re.search('\\bFED[ -]?4124\\b', aff_string):
                affs.append(4389425223)

            # Alzheimer Precision Medicine
            if "Alzheimer Precision Medicine".lower() in lower_aff_string:
                affs.append(4389425465)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?21\\b', aff_string):
                affs.append(4389425465)

            # Groupe de Recherche Clinique en Neuro-urologie - GRC 1
            if "Groupe de recherche clinique en neuro".lower() in lower_aff_string:
                affs.append(4389425312)
            elif re.search('\\bGREEN\\b', aff_string):
                affs.append(4389425312)

            # Complications Cardiovasculaires et Métaboliques chez les patients vivant avec le VIH - GRC 22
            if "complications cardiovasculaires et métaboliques chez les patients vivant avec le v" in lower_aff_string:
                affs.append(4389425418)
            elif re.search('\\bC2MV\\b', aff_string):
                affs.append(4389425418)
            elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?22\\b', aff_string):
                affs.append(4389425418)

            if 'Paris' in aff_string:
                # Onco-Urologie Prédictive - GRC 5
                if "Predictive Onco-Uro".lower() in lower_aff_string:
                    affs.append(4389425335)
                elif re.search('\\b(Groupe de Recherche Clinique|GRC){1}[ -]?(n|N|No|no)?°? ?(5|05)\\b', aff_string):
                    affs.append(4389425335)

                # Centre d'Acquisition et de Traitement des Images
                if "Centre d'Acquisition et de Traitement des Images".lower() in lower_aff_string:
                    affs.append(4389425361)
                elif re.search('\\bCATI\\b', aff_string):
                    affs.append(4389425361)
                elif re.search('\\bUAR[ -]?2031\\b', aff_string):
                    affs.append(4389425361)

                # AP-HP
                if re.search('\\bAPHP\\b', aff_string):
                    affs.append(4210097159)

                # UMS Phénotypage du petit animal
                ['UMS028','UMS 28','UMS28']
                if "Phénotypage du petit animal".lower() in lower_aff_string:
                    affs.append(4389425460)
                elif re.search('\\bUMS[ -]?(28|028)\\b', aff_string):
                    affs.append(4389425460)

                # Centre d'expérimentation en méthodes numériques pour les recherches en SHS
                elif re.search('\\bCERES\\b', aff_string):
                    affs.append(4389425386)

            if 'gif-sur-yvette' in aff_string.lower():
                # Centre d'Acquisition et de Traitement des Images
                if "Centre d'Acquisition et de Traitement des Images".lower() in lower_aff_string:
                    affs.append(4389425361)
                elif re.search('\\bCATI\\b', aff_string):
                    affs.append(4389425361)
                elif re.search('\\bUAR[ -]?2031\\b', aff_string):
                    affs.append(4389425361)

        if 'Paris' in aff_string:
            # CoRaKID
            if any(word in aff_string.lower() for word in ['corakid','maladies rénales fréquentes et rares',
                                                    'common and rare kidney disease']):
                affs.append(4390039341)

            # CEMA
            if any(word in aff_string.lower() for word in ["centre d'études médiévales anglaises"]):
                affs.append(4389425513)
            elif re.search('\\bCEMA\\b', aff_string):
                affs.append(4389425513)

        if 'banyuls-sur-mer' in aff_string.lower():
            # BIOM
            if any(word in aff_string.lower() for word in ['biologie intégrative des organismes marins',
                                                            'integrative biology of marine organisms']):
                affs.append(4210131549)
            elif re.search('\\bBIOM\\b', aff_string):
                affs.append(4210131549)

        if ('inserm' in aff_string.lower()) & ('U1028' in aff_string) & ('CNRS' in aff_string) & ('5292' in aff_string):
            affs.append(4210095118)

        if 'INSERM U1209' in aff_string:
            affs.append(899635006)

        # University de Reims Champagne-Ardenne
        if 'Reims' in aff_string:
            if any(word in aff_string for word in ['University de Reims Champagne-Ardenne','CHU de Reims',
                                                   'CHU Reims','University of Reims in Champagne-Ardenne',
                                                   'URCA']):
                affs.append(96226040)

        # Hospices Civils de Lyon
        if ('inserm' in aff_string.lower()) & ('U1060' in aff_string):
            affs.append(4210100596)
        elif any(word in aff_string for word in ['CETD','HCL','Lyon University Hospital',
                                                 'University Hospital of Lyon','hospices civils de Lyon',
                                                 'Civil Hospices of Lyon','CHU of Lyon',
                                                 'University Hospital, Lyon','CHU-Lyon',
                                                 'Hospice civils de Lyon']):
            affs.append(4210100596)

        # Lyon Neuroscience Research Center
        if any(word in aff_string for word in ['UMR 5292', 'UMR5292','INSERM 1028']):
            affs.append(4210095118)
        elif ('inserm' in aff_string.lower()) & ('U1028' in aff_string):
            affs.append(4210095118)

        # Laboratoire de Géologie de Lyon
        if any(word in aff_string for word in ['Laboratory of Geology of Lyon',
                                               'Laboratoire de Géologie de Lyon',
                                               'LGLTPE']):
            affs.append(4210155927)

        # Cancer Center of Lyon
        if any(word in aff_string for word in ['CNRS 5286', 'CNRS5286','CRCL']):
            affs.append(4210125048)
        elif ('inserm' in aff_string.lower()) & any(word in aff_string for word in ['U1052', 'U590']):
            affs.append(4210125048)

        # Camille Jordan Institute
        if any(word in aff_string for word in ['UMR 5208', 'UMR5208','CNRS5208','CNRS 5208',
                                               'Institut Camille Jordan']):
            affs.append(4210104796)

        # Institut des Nanotechnologies de Lyon
        if any(word in aff_string for word in ['UMR5270', 'UMR 5270','Nanotechnology Institute of Lyon']):
            affs.append(2800958632)
        elif re.search('\\bINL\\b', aff_string):
            affs.append(2800958632)

        # Pratique des Hautes Études
        if re.search('\\bEPHE\\b', aff_string):
            affs.append(159885104)

        # Biometry and Evolutionary Biology Laboratory
        if re.search('\\bLBBE\\b', aff_string):
            affs.append(4210135640)

        # Institut Lumière Matière
        if 'Light Matter Institute' in aff_string:
            affs.append(4210133140)

        # Laboratoire de Physique de l'ENS de Lyon
        if 'UMR 5672' in aff_string:
            affs.append(4210096929)

        # Hôpital de la Croix-Rousse
        if any(word in aff_string for word in ['Croix-Rousse University Hospital','Croix-Rousse Hospital']):
            affs.append(4210089315)

        # Hôpital Édouard-Herriot
        if any(word in aff_string for word in ['Herriot hospital','hôpital Edouard-Herriot',
                                               'University hospital of Lyon Edouard Herriot']):
            affs.append(4210123600)

        # Laboratoire de Mécanique des Fluides et d'Acoustique
        if any(word in aff_string for word in ['LMFA',"Laboratoire de Mécanique des Fluides et d'Acoustique",
                                               'Laboratory of Fluid Mechanics and Acoustic',
                                               'Fluid Mechanics and Acoustics Laboratory',
                                               'Fluid Mechanics and Acoustic Laboratory']):
            affs.append(4210149024)

        # Physiologie de la Reproduction et des Comportements
        if ("PRC" in aff_string) and any(word in aff_string for word in ["CIRE","INRAE","Nouzilly"]):
            affs.append(4210116130)
        elif 'Physiologie de la reproduction et des comportements' in aff_string:
            affs.append(4210116130)

        # Claude Bernard University Lyon 1
        if any(word in aff_string for word in ['Lyon 1','UCBL','Claude Bernard','Lyon I','UCB Lyon','Lyon1',
                                               'Interuniversity Laboratory of Human Movement Biology','LIBM',
                                               'Observatoire de Lyon','Claude-Bernard–Lyon-I',
                                               'Université Claude-Bernard','Claude-Bernard université',
                                               'Claude-Bernard university','Lyon-1 University',
                                               'Claude-Bernard University','Claude-Bernard-Lyon university',
                                               'Lyon-I University','Université de Lyon, LIP','LBMC',
                                               'LIP Lyon','IBCP','University of Lyon-1']):
            affs.append(100532134)
        elif any(word in aff_string for word in ['UMR CNRS 5574','UMR5574','UMR 5574','CNRS 5574','CNRS5574',
                                                 'UMR 5259','UMR5259','UMR CNRS 5259','UMR1033','UMR 1033',
                                                 'UMR5305','UMR 5305','UMR 5005','UMR5005',
                                                 'UMR5286','UMR 5086','UMR5086','CNRS 5005','CNRS5005',
                                                 'INSERM U1210','UMR5308','UMR CNRS 5023','UMR 5023',
                                                 'UMR5023','UMR 5308','UMR 5239','UMR5239']):
            affs.append(100532134)
        elif ('inserm' in aff_string.lower()) & any(word in aff_string for word in ['U1060']):
            affs.append(100532134)
        elif ('Faculté de Médecine Lyon Sud' in aff_string) & ('Pierre Bénite' in aff_string):
            affs.append(100532134)
        elif re.search('\\bCIRI\\b', aff_string):
            affs.append(100532134)
        elif any(word in aff_string for word in ['Université de Lyon','University of Lyon','Universite de Lyon',
                                                 'Univ Lyon','Univ. Lyon','Université Lyon',
                                                 'University Lyon','Univ.Lyon','University de Lyon',
                                                 'Univ. de Lyon','Univ-Lyon','Universite´ Lyon',
                                                 'Univ de Lyon',"Universit’e de Lyon"]):
            if 'Villeurbanne' in aff_string:
                affs.append(100532134)
            elif 'LIP Laboratory' in aff_string:
                affs.append(100532134)
            else:
                affs.append(100532134)

        # Tours University and Orleans University
        if 'Poisson' in aff_string:
            if any(word in aff_string for word in ['Institut Dennis Poisson','Fédération Denis-Poisson',
                                                   'Institut Denis Poisson']):
                affs.append(4387156285)


        # University of Orléans
        if 'Orléans' in aff_string:
            if any(word in aff_string for word in ["Université d’Orléans","Univ. Orléans",
                                                   "Univ Orléans","University Orléans"]):
                affs.append(12449238)
            if any(word in aff_string for word in ['Tours University','Université de Tours','Univ. Tours',
                                                   'U. de Tours','Universite de Tours','Université Tours',
                                                   'université de Tours','universitaire de Tours',
                                                   'Univ. de Tours','Université F. Rabelais',
                                                   'University François Rabelais','Univ Tours',
                                                   'University François-Rabelais','U. Tours',
                                                   'University Tours']):
                affs.append(110017253)

        # Tours
        if any(word in aff_string for word in ['Rabelais','Tours','TOURS']):
            # Tours University (François Rabelais University)
            if any(word in aff_string for word in ['Tours University','Université de Tours','Univ. Tours',
                                                   'U. de Tours','Universite de Tours','Université Tours',
                                                   'université de Tours','universitaire de Tours',
                                                   'Univ. de Tours','Université F. Rabelais',
                                                   'University François Rabelais','Univ Tours',
                                                   'University François-Rabelais','U. Tours','University Tours']):
                affs.append(110017253)

            # CHRU de Tours
            if any(word in aff_string for word in ['CHU de Tours','CHRU de Tours','Hôpital Bretonneau',
                                                     'CHRU Bretonneau','CHU Tours','CHU Bretonneau',
                                                     'CHU Bretonneau','CHRU, hopital Bretonneau',
                                                     'Trousseau Hospital','Clocheville Hospital',
                                                     'hôpital Clocheville','Trousseau University Hospital',
                                                     'Bretonneau Hospital', 'CHU de Bretonneau',
                                                     'Regional Hospital University Centre of Tours',
                                                     'University Hospital Bretonneau','CHU Trousseau',
                                                     'Bretonneau University Hospital','CHRU de Bretonneau',
                                                     'university hospital of Tours','CRHU de Tours',
                                                     'University Hospital of Tours','Hôpital Trousseau',
                                                     'CHU de Trousseau','hôpital Trousseau',
                                                     'CHU Clocheville','CHRU Clocheville',
                                                     'Clocheville University Hospital',
                                                     'University Hospital Regional Center Tours',
                                                     'Hôpital Clocheville','Hopital Bretonneau',
                                                     'Hospital Trousseau','CHRU tours',
                                                     'University Hospital Center, Tours',
                                                     'University Hospital Centre Tours',
                                                     'University Hospital Tours']):
                affs.append(4210095477)
            elif ('CHU' in aff_string) & ('Tours' in aff_string):
                affs.append(4210095477)


            # Nantes and Tours together
            if any(word in aff_string for word in ['Universities of Nantes and Tours',
                                               'Nantes and Tours Universities',
                                               'Universités de Nantes et Tours',
                                               'University of Nantes and Tours']):
                affs.append(110017253)
                affs.append(97188460)

    # University of Hong Kong
    if any(word in aff_string for word in ['UHK', 'University of Hong Kong','HKI','HKU',
                                           'HKMU','Hong Kong','HongKong']):
        # Chinese University of Hong Kong, Shenzhen
        if re.search('\\bCUHKSZ\\b', aff_string):
            affs.append(4210116924)
        elif any(word in aff_string for word in ['Chinese University of Hong Kong, Shenzhen']):
            affs.append(4210116924)

        # Education University of Hong Kong
        if re.search('\\bEUHK\\b', aff_string):
            affs.append(4210086892)
        elif re.search('\\bHKIEd\\b', aff_string):
            affs.append(4210086892)
        elif re.search('\\bEdUHK\\b', aff_string):
            affs.append(4210086892)
        elif any(word in aff_string for word in ['Education University of Hong Kong',
                                                 'Education University of HongKong',
                                                 'Hong Kong Education University',
                                                 'Education University of Hongkong',
                                                 'Education University of Hong of Kong',
                                                 'Hong Kong Institute of Education',
                                                 'Educational University of Hong Kong']):
            affs.append(4210086892)

        # Open University of Hong Kong
        if re.search('\\bHKMU\\b', aff_string):
            affs.append(8679417)
        elif any(word in aff_string for word in ['Hong Kong Open University','Open University of Hong Kong']):
            affs.append(8679417)

        # City University of Hong Kong
        if re.search('\\bCityU\\b', aff_string) and ('Hong Kong' in aff_string):
            affs.append(168719708)
        elif 'City University of Hong Kong' in aff_string:
            if 'Shenzhen Research Institute' in aff_string:
                affs.append(4210105229)
            else:
                affs.append(168719708)

        # Hong Kong Baptist University
        if 'Hong Kong Baptist University' in aff_string:
            affs.append(141568987)

        # Chinese University of Hong Kong
        if 'Chinese University of Hong Kong' in aff_string:
            if 'Shenzhen' in aff_string:
                affs.append(4210116924)
            else:
                affs.append(177725633)

        # Hang Seng University of Hong Kong
        if 'Hang Seng University of Hong Kong' in aff_string:
            affs.append(47605537)

        # University of Hong Kong
        if not any(inst in affs for
                   inst in [4210116924,4210086892,4210105229,4210141719,47605537,177725633,168719708,8679417]):
            if any(word in aff_string for word in ['UHK', 'University of Hong Kong','HKU',
                                                   'Hong Kong University']):
                if 'Shenzhen Hospital' in aff_string:
                    affs.append(4210141719)
                else:
                    affs.append(889458895)

    # Poznan University of Life Sciences
    if any(word in aff_string for word in ['University of Life','Poznan University Life Sci',
                                           'Poznan Univ of Life Sciences','Poznan Life Sciences University',
                                           'Poznan University of life Sciences',
                                           'Poznan Univeristy of Life Science',
                                           'Univ. of Life Sciences',
                                           'Poznan Univ. of Life Sci','Poznan Life Science University']):
        if 'Poznan University of Life Science' in aff_string:
            affs.append(55783418)

        if any(word in aff_string for word in ['Poznan','Poland','Lublin','Warsaw']):
            if any(word in aff_string for word in ['Poznan','Poznań','Poznañ',
                                                   'Institute of Food Technology of Plant Origin']):
                affs.append(55783418)
            elif 'Lublin' in aff_string:
                affs.append(158552681)
            elif 'Warsaw' in aff_string:
                affs.append(170230895)

        if any(word in aff_string for word in ['Estonia','Tartu']):
            affs.append(19409027)

        if any(word in aff_string for word in ['Latvia','Jelgava']):
            affs.append(116152951)

        if any(word in aff_string for word in ['Mongolia','Ulaanbaatar']):
            affs.append(190774190)

        if any(word in aff_string for word in ['Norway','Norwegian']):
            affs.append(54108979)

        if any(word in aff_string for word in ['Czech','Prague']):
            affs.append(205984670)

    # Ohio University
    if any(word in aff_string.lower() for word in ['ohio univ']):
        if 'Athens' in aff_string:
            affs.append(4210106879)
        elif any(word in aff_string for word in ['Ironton','Ohio University Southern']):
            affs.append(2801499443)
        elif 'Lancaster' in aff_string:
            affs.append(22759111)
        elif 'Chillicothe' in aff_string:
            affs.append(182441304)
        else:
            affs.append(4210106879)

    # Université Henri Poincaré
    if 'henri poincar' in aff_string.lower():
        if any(word in aff_string for word in ['Université Henri Poincar','Universite Henri Poincar',
                                               'Univ. Henri Poincar', "Universite ´Henri Poincar"]):
            affs.append(90183372)

    if 'nancy' in aff_string.lower():
        if re.search('\\bUHP\\b', aff_string):
            affs.append(90183372)
        elif re.search('\\bINPL\\b', aff_string):
            affs.append(90183372)
        elif any(word in aff_string.lower() for word in ['université nancy 1','univ. nancy 1','universite nancy 1',
                                                 'université de nancy 1','universite de nancy 1',
                                                 'univ nancy 1','université nancy 2','univ. nancy 2',
                                                 'universite nancy 2','univ nancy 2','université de nancy 2',
                                                 'universite de nancy 2','nancy 1 univ','nancy 2 univ'
                                                 'institut national polytechnique de lorraine']):
            affs.append(90183372)
        elif re.search('\\bENSG\\b', aff_string):
            affs.append(90183372)



    # Sağlık Bilimleri Üniversitesi
    if 'Tur' in aff_string:
        if 'Turkey' in aff_string:
            if any(word in aff_string for word in ['University of Health Sciences','Sağlık Bilimleri Üniversitesi',
                                                   'SBÜ','Unıversity of Health Sciences','Dr. A.Y Ankara Oncology Training and Research Hospital']):
                affs.append(4210128276)
        elif 'Turquía' in aff_string:
            if 'Universidad de Ciencias de la Salud' in aff_string:
                affs.append(4210128276)

    # Guiyang Medical University
    if 'China' in aff_string:
        if any(word in aff_string for word in ['Guiyang Medical University','Guizhou Medical University']):
            affs.append(149137203)

    # National Kaohsiung First University of Science and Technology
    if any(word in aff_string for word in ['National Kaohsiung First University of Science and Technology',
                                           'NKUST']):
        affs.append(109289231)

    # Monterrey Institute of Technology and Higher Education
    if 'Monterrey' in aff_string:
        if any(word in aff_string for word in ['Tecnológico de Monterrey','Tecnologico de Monterrey',
                                            'Monterrey Institute of Technology and Higher Education',
                                            'Instituto Tecnológico y de Estudios Superiores de Monterrey',
                                            'Instituto Tecnologico y de Estudios Superiores de Monterrey',
                                            'ITESM','Tec de Monterrey']):
            affs.append(98461037)

    # North China University of Water Conservancy and Electric Power
    if 'China' in aff_string:
        if any(word in aff_string for word in ['North China University of Water Conservancy and Electric Power',
                                            'North China Univ. of Water Conservancy and Electric Power',
                                            'North China Univ. of WC&EP','NC UWCEP',
                                            'North China Univ. of Water Conservancy & EP','North China UWCEP',
                                            'NC University of Water Conservancy and Electric Power','Tec de Monterrey']):
            affs.append(198645480)

    # State University of Campinas
    if 'Campinas' in aff_string:
        if any(word in aff_string for word in ['State University of Campinas','Universidade Estadual de Campinas',
                                            'UNICAMP']):
            affs.append(181391015)


    # Goethe University Frankfurt
    if 'Frankfurt' in aff_string:
        if any(word in aff_string for word in ['Institut für Geowissenschaften','Geothe-University',
                                               'University of Frankfurt','Center for Financial Studies',
                                               'Goethe University','University Frankfurt',
                                               'Goethe-Universität','Universidad Goethe',
                                               'Goethe Universität','Institut für Kernphysik',
                                               'Goethe-University','Universität Frankfurt',
                                               'Institut Für Geowissenschaften','Goethe university',
                                               'Institute for Theoretical Physics',
                                               'Institute for Atmospheric and Environmental Sciences',
                                               'University Clinic of Frankfurt','Frankfurt Medical School']):
            affs.append(114090438)
        elif 'Frankfurt University' in aff_string:
            if ~any(word in aff_string for word in ['Applied Sciences','Music and Performing Arts']):
                affs.append(114090438)

        if 'University Cancer Center' in aff_string:
            affs.append(114090438)
            affs.append(4210132578)
    elif ('Goethe' in aff_string) and ('Frankfurt'not in aff_string):
        if any(word in aff_string for word in ['Institut für Geowissenschaften','Geothe-University',
                                               'Center for Financial Studies',
                                               'Goethe University',
                                               'Goethe-Universität','Universidad Goethe',
                                               'Goethe Universität','Institut für Kernphysik',
                                               'Goethe-University','Institut Für Geowissenschaften',
                                               'Goethe university','Institute for Theoretical Physics',
                                               'Institute for Atmospheric and Environmental Sciences']):
            affs.append(114090438)

    # Technical University of Graz (need to remove University of Graz)
    if 'Graz' in aff_string:
        if any(word in aff_string for word in ['TU Graz','Technical University of Graz',
                                               'Technical University Graz','Graz Technical University',
                                               'University of Technology','TUGraz','TU-Graz']):
            affs.append(4092182)
    elif 'Austria' in aff_string:
        if re.search('\\bTUG\\b', aff_string):
            affs.append(4092182)

    # Universität Hamburg
    if 'Hamburg' in aff_string:
        if any(word in aff_string for word in ['University Medical Center Hamburg',
                                               'University Hospital Hamburg','Center for Free-Electron Laser Science',
                                               'CFEL','Center for Free Electron Laser Science']):
            affs.append(159176309)

    # Universidade Nova de Lisboa
    if 'Portugal' in aff_string:
        if any(word in aff_string for word in ['NOVA School of Science and Technology', 'FCT NOVA',
                                               'New University of Lisbon','NOVA Medical School',
                                               'Universidade Nova de Lisboa','Uninova']):
            affs.append(83558840)


    # Heidelberg University
    if any(word in aff_string for word in ['Mannheim','Heidelberg','Heidelburg']):
        if any(word in aff_string for word in ['Ruprecht-Karls-Universität','University Heidelberg',
                                               'University of Heidelberg','Rupprecht-Karls-Universität',
                                               'Universität Heidelberg','Heidelberg Univ',
                                               'Heidelburg University','Institute for Anatomy and Cell Biology',
                                               'Interdisciplinary Center for Scientific Computing',
                                               'Ruprecht-Karls University','German Cancer Research Center',
                                               'DKFZ','Ruprecht-Karls-University','Universität#N# Heidelberg',
                                               'Ruprecht-Karis-University','Universtät Heidelberg',
                                               'Institute of Environmental Physics','Univ. Heidelberg',
                                               'University of Heidel','Kirchhoff-Institute for Physics',
                                               'Universitätsbibliothek Heidelberg','Ruprecht Karls University',
                                               'Kirchhoff-Institute for Physics','Universitätsbibliothek Heidelberg',
                                               'UniversitätHeidelberg','University of Mannheim-Heidelberg',
                                               'Institute for Pharmacy and Molecular Biotechnology',
                                               'Institute of Human Genetics','Institut für Umweltphysik',
                                               'Center for Pediatric and Adolescent Medicine',
                                               'Center for Pediatric & Adolescent Medicine','Univ of Heidelberg',
                                               'Kirchhoff-Institut für Physik','Universitet Heidelberg',
                                               'Universität#N#                    Heidelberg',
                                               'Universität                     Heidelberg',
                                               'Institute of Pharmacy and Molecular Biotechnology']):
            affs.append(223822909)

        if any(word in aff_string for word in ['UniversitatsKlinikum Heidelberg','University-Hospital Mannheim',
                                               'UniversitätsKlinikum Heidelberg','University of Medicine',
                                               'Mannheim University Hospital',"University Children’s Hospital",
                                               'Center for Pediatrics and Adolescent Medicine',
                                               'University Hospital Mannheim','Heidelberg School of Medicine',
                                               'University Hospital Manheim','University Medical Center',
                                               'Universitätsmedizin Mannheim','University Medicine Mannheim',
                                               'University Hospital','Universitätsklinik',
                                               'University Clinic Heidelberg',"Universtity Children's Hospital",
                                               "University Children's Hospital",'University Clinic',
                                               'Medical Faculty Heidelberg','Universitaetsklinik Heidelberg',
                                               'Universitäts Klinikum Heidelberg','Medical Faculty Mannheim',
                                               'University Medical Centre Heidelberg','Univ Med Cntr',
                                               'Universitaetsklinkum Heidelberg','Medical Faculty',
                                               'Ruprecht-Karls-Universitat Hospital','University Medicine',
                                               'University Medicine of Mannheim','Universitäts-Klinikum',
                                               'Heidelberg Medical School','Medizinische Fakultät Heidelberg',
                                               'Heidelberg Institute of Global Health','Universitätsklinkum']):
            affs.append(2802164966)
            affs.append(223822909)

    elif 'Germany' in aff_string:
        if any(word in aff_string for word in ['Ruprecht-Karls-Universität','University Heidelberg',
                                               'University of Heidelberg','Rupprecht-Karls-Universität',
                                               'Universität Heidelberg','Heidelberg Univ',
                                               'Ruprecht-Karls University','Ruprecht-Karls-University',
                                               'Ruprecht-Karis-University','Universtät Heidelberg']):
            affs.append(223822909)

    # Indian Institute of Technology Dhanbad
    if 'dhanbad' in aff_string.lower():
        if 'Indian Institute of Technology' in aff_string:
            affs.append(189109744)
        elif 'ISM' in aff_string:
            affs.append(189109744)
        elif 'Indian School of Mines' in aff_string:
            affs.append(189109744)

    if 'India' in aff_string:
        if re.search('\\bISM\\b', aff_string):
            affs.append(189109744)

    # Indian Institute of Technology Hyderabad
    if 'Hyderabad' in aff_string:
        if re.search('\\bIIIT\\b', aff_string):
            affs.append(65181880)
        elif any(word in aff_string for word in ['Indian Institute of Technology','IIT Hyderabad']):
            affs.append(65181880)

    # Hubei University
    if 'Hubei University' in aff_string:
        if 'Hubei University of China' in aff_string:
            affs.append(75900474)
        elif 'Hubei University of' not in aff_string:
            affs.append(75900474)

    # Hongik University
    if 'Hongik University' in aff_string:
        affs.append(94588446)

    # Henan Polytechnic University
    if 'Henan' in aff_string:
        if any(word in aff_string for word in ['Henan Polytechnic','Henan Quality Polytechnic']):
            affs.append(4210166499)

    # Henan Normal University
    if 'Henan Normal Univ' in aff_string:
        affs.append(75955062)

    # Heinrich Heine University Düsseldorf (take out Hochschule Düsseldorf University of Applied Sciences)
    if any(word in aff_string for word in ['Düsseldorf','Duesseldorf','Dusseldorf',
                                           'Du¨sseldorf','D€usseldorf']):
        if any(word in aff_string for word in ['Medicine University Düsseldorf',
                                               'Medicine University Duesseldorf',
                                               'Medicine University Dusseldorf',
                                               'Universität Düsseldorf','HHU',
                                               'University Dusseldorf','University Duesseldorf',
                                               'University Düsseldorf','Center for Health and Society',
                                               'Düsseldorf University','Dusseldorf University',
                                               'Duesseldorf University','Heinrich Heine',
                                               'Heinrich-Heine','Univ Düsseldorf']):
            affs.append(44260953)
        elif any(word in aff_string for word in ['University of Düsseldorf',
                                                 'University of Duesseldorf',
                                                 'University of Dusseldorf',
                                                 'University of Du¨sseldorf',
                                                 'University of D€usseldorf']):
            if not any(word in aff_string for word in ['Hochschule Düsseldorf University',
                                                   'Düsseldorf University of Applied Sciences',
                                                   'Hochschule Duesseldorf University',
                                                   'Duesseldorf University of Applied Sciences',
                                                   'Hochschule Dusseldorf University',
                                                   'Dusseldorf University of Applied Sciences',
                                                   'Hochschule Du¨sseldorf University',
                                                   'Du¨sseldorf University of Applied Sciences',
                                                   'Hochschule D€usseldorf University',
                                                   'D€usseldorf University of Applied Sciences']):
                affs.append(44260953)



        # Düsseldorf University Hospital
        if any(word in aff_string for word in ['University Hospital',"University Children's Hospital",
                                               'Universitätsklinik', 'University Medical Center',
                                               'Universitätsfrauenklinik','University Clinic',
                                               'University Medical Centre','Universitäts-Frauenklinik',
                                               'Uniklinik']):
            affs.append(4210089242)
            affs.append(44260953)

    elif 'Germany' in aff_string:
        # German Center for Diabetes Research
        if any(word in aff_string for word in ['German Centre for Diabetes Research',
                                               'German Center for Diabetes Research']):
            affs.append(4210152419)
            affs.append(44260953)


    # Indiana University – Purdue University (remove Indiana University)
    if any(word in aff_string for word in ['IUPU','Indiana Univ', 'Purdue Univ','Bloomington']):
        if any(word in aff_string for word in ['Indiana University – Purdue University Columbus','IUPUC']):
            affs.append(59900826)

        if any(word in aff_string for word in ['Indiana University – Purdue University Fort Wayne', 'IUPUFW']):
            affs.append(162817326)

        if any(word in aff_string
               for word in ['IUPUI','Indiana University School of Medicine','Indiana University, Indianapolis',
                            'Indiana University Medical School','Indiana University School Of Medicine',
                            'Indiana University, Medical School','Robert H. McKinney School of Law',
                            'Indiana University, School of Medicine','Indiana University School of Nursing',
                            'Richard M. Fairbanks','Indiana Univ. School of Medicine',
                            'Indiana University Indianapolis','Indiana University; Indianapolis',
                            'Indiana University of School of Medicine','Indiana University School of Dentistry',
                            'Kelley School of Business']):
            if 'Indianapolis' in aff_string:
                affs.append(55769427)
        elif all(word in aff_string for word in ['Indiana University','Purdue University','Indianapolis']):
            affs.append(55769427)
        elif ('Indiana Univ' in aff_string) and ('Indianapolis' in aff_string):
            affs.append(55769427)
        elif ('Purdue Univ' in aff_string) and ('Indianapolis' in aff_string):
            affs.append(55769427)

        if (any(word in aff_string for word in ['Indiana Univ','University of Indiana','Indiana university']) and
            any(word in aff_string for word in ['Bloomington','Bloomingston'])):
            affs.append(4210119109)

    if 'Purdue' in aff_string:
        if any(word in aff_string for word in ['Purdue School of Science',
                                               'Purdue School of Engineering and Technology']):
            affs.append(55769427)

    if 'Indianapolis' in aff_string:
        if any(word in aff_string for word in ['VA Medical Center', 'Roudebush VA']):
            affs.append(4210110049)
            affs.append(55769427)

        if any(word in aff_string for word in ['Simon Cancer Center','Simon Cancer Ctr',
                                               'Simon Comprehensive Cancer Center',
                                               'Indiana University of Medicine']):
            affs.append(1283055418)
            affs.append(55769427)

        if any(word in aff_string
               for word in ['IU School of Medicine','Indian University School of Medicine',
                            'Indiana School of Medicine','Indiana  University School of Medicine',
                            'Ryan White Center for Infectious Diseases and Global Health',
                            'Indiana, University School of Medicine','IUSD','Riley Heart Research Center',
                            'Krannert Institute of Cardiology','IUSM','Herman B Wells Center']):
            affs.append(55769427)

    # Inner Mongolia
    if 'Inner Mongolia' in aff_string:
        # Inner Mongolia Agriculture University
        if any(word in aff_string for word in ['Inner Mongolia Agricultural Univ',
                                               'Inner Mongolia Agriculture Univ']):
            affs.append(120379545)

        # Inner Mongolia University
        if 'Inner Mongolia Univ' in aff_string:
            if not any(word in aff_string for word in ['Inner Mongolia University for',
                                                       'Inner Mongolia University of']):
                affs.append(2722730)

    # Istanbul Technical University
    if (re.search(r'\b(ITÜ|ITU|I\.T\.Ü)\b', aff_string)
        or 'Istanbul Tech. Uni' in aff_string
        or 'Istanbul Technical University' in aff_string):
        affs.append(48912391)

    # Islamic Azad University (Need to remove the other one)
    if 'Islamic Azad University' in aff_string:
        if any(word in aff_string.lower()
               for word in ['sciences and researches branch',
                            'science and research branch',
                            'sciences and research branch']):
            affs.append(155419210)

    # More French institutions
    if any(word in aff_string for word in ['Paris','France','Bordeaux','Toulouse','Grenoble','Rouen','Lyon',
                                           'Nantes','Montpellier','Sorbonne','Rennes','Caen','Normandie']):
        # INSERM
        if any(word in aff_string for word in ['INSERM','Inserm']):
            affs.append(154526488)

        # CNRS
        if 'CNRS' in aff_string:
            affs.append(1294671590)

        # CEA (CEA-Leti, CEA LITEN/Liten, CEA Gramat, CEA Valduc, CEA DAM Île-de-France/DIF, CEA Grenoble,
        #      CEA Cadarache, CEA LIST/List, CEA Marcoule, CEA Saclay, CEA Ripault, CEA Fontenay-aux-Roses)
        if 'CEA' in aff_string:
            if re.search('\\bCEA\\b', aff_string):
                affs.append(2738703131)
                if re.search('\\bLeti\\b', aff_string):
                    affs.append(4210150049)
                elif any(word in aff_string for word in ['LITEN','Liten']):
                    affs.append(3019244752)
                elif re.search('\\bGramat\\b', aff_string):
                    affs.append(4210094417)
                elif re.search('\\bValduc\\b', aff_string):
                    affs.append(2799888343)
                elif any(word in aff_string for word in ['DAM Île-de-France','DAM DIF','DAM-DIF','DIF']):
                    affs.append(4210101455)
                elif re.search('\\bGrenoble\\b', aff_string):
                    affs.append(3020098449)
                elif re.search('\\bCadarache\\b', aff_string):
                    affs.append(4210110641)
                elif any(word in aff_string for word in ['LIST','List']):
                    affs.append(4210085861)
                elif re.search('\\bMarcoule\\b', aff_string):
                    affs.append(4210143636)
                elif re.search('\\bSaclay\\b', aff_string):
                    affs.append(4210128565)
                elif re.search('\\bRipault\\b', aff_string):
                    affs.append(4210115841)
                elif re.search('\\bFontenay\\b', aff_string):
                    affs.append(4210097138)
            elif 'CEALETI' in aff_string:
                    affs.append(4210150049)

        # INRA/INRAE
        if 'INRA' in aff_string:
            if re.search('\\bINRAE\\b', aff_string):
                affs.append(4210088668)

            # INRA (not in OpenAlex)

        # INSA
        if 'INSA' in aff_string:
            # INSA Rouen
            if any(word in aff_string for word in ['INSA Rouen', 'INSA de Rouen']):
                affs.append(88814501)

            # INSA Rennes
            if any(word in aff_string for word in ['INSA Rennes', 'INSA de Rennes']):
                affs.append(28221208)

            # INSA Strasbourg
            if any(word in aff_string for word in ['INSA Strasbourg', 'INSA de Strasbourg']):
                affs.append(2801509770)

            # INSA CVL
            if any(word in aff_string for word in ['INSA CVL', 'INSA de CVL', 'INSA Centre Val de Loire','INSACVL']):
                affs.append(4210143826)

            # INSA Lyon
            if any(word in aff_string for word in ['INSA Lyon', 'INSA de Lyon']):
                affs.append(48430043)

            # INSA Toulouse
            if any(word in aff_string for word in ['INSA Toulouse', 'INSA de Toulouse','INSAT']):
                affs.append(196454796)

        # Institut de Recherche en Santé, Environnement et Travail
        if any(word in aff_string for word in ['Irset','IRSET']):
            affs.append(4210108239)

        # Centre Hospitalier Universitaire de Rennes
        if 'Rennes' in aff_string:
            if any(word in aff_string for word in ['University Hospital of Rennes',
                                                   'University of Rennes Hospital',
                                                   'Hospitalier Universitaire de Rennes']):
                affs.append(4210155724)

            elif any(word in aff_string for word in ['Univ Rennes', 'Univ. Rennes','Univ de Rennes',
                                                     'Univ. de Rennes']):
                affs.append(56067802)

        # Biology and Genetics of Plant-Pathogen Interactions
        if re.search('\\bBGPI\\b', aff_string):
            affs.append(4210087514)

        # Artois University
        if 'Artois' in aff_string:
            if any(word in aff_string for word in ['Univ. Artois', 'Univ Artois']):
                affs.append(44563897)

        # Centre d'Écologie Fonctionnelle et Évolutive
        if re.search('\\bCEFE\\b', aff_string):
            affs.append(4210089824)

        # Laboratory of Catalysis and Solid State Chemistry
        if re.search('\\bUCCS\\b', aff_string):
            affs.append(4210141930)

        # La Timone University Hospital
        if any(word in aff_string for word in ['Timone','Timône']):
            if any(word in aff_string for word in ['La Timone university hospital',
                                                   'Hôpital de la Timone',
                                                   'Timône University Hospital']):
                affs.append(4210162909)

        # Institut de Physique du Globe de Strasbourg (remove Paris version)
        if 'Institut de Physique du Globe' in aff_string:
            if 'Strasbourg' in aff_string:
                affs.append(68947357)

        # Hôpital Cochin
        if 'Cochin Hospital' in aff_string:
            affs.append(4210092774)

        # Université d'Avignon
        if "Université d'Avignon" in aff_string:
            affs.append(198415970)

        # LRI
        if re.search('\\bLRI\\b', aff_string):
            affs.append(4210144804)

        # BIAM ###### check one time
        if re.search('\\bBIAM\\b', aff_string):
            affs.append(4210152302)

        # iEES
        if re.search('\\biEES\\b', aff_string):
            affs.append(4210134846)

        # CNAM ####### check (could be wrong)
        if re.search('\\bCNAM\\b', aff_string):
            affs.append(124158823)

        # IBMM
        if re.search('\\bIBMM\\b', aff_string):
            affs.append(4210145258)

        # CIRAD/Cirad
        if re.search('\\bCIRAD\\b', aff_string):
            affs.append(131077856)
        elif re.search('\\bCirad\\b', aff_string):
            affs.append(131077856)

        # PBS
        if re.search('\\bPBS\\b', aff_string):
            affs.append(4210140452)

        # LaMCoS
        if re.search('\\bLaMCoS\\b', aff_string):
            affs.append(203339264)

        # LIS
        if re.search('\\bLIS\\b', aff_string):
            affs.append(4210114274)

        # CESP
        if re.search('\\bCESP\\b', aff_string):
            affs.append(4210103698)

        # ENSCL
        if re.search('\\bENSCL\\b', aff_string):
            affs.append(137614889)

        # GEPI
        if re.search('\\bGEPI\\b', aff_string):
            affs.append(4210103454)

        # IRFU
        if re.search('\\bIRFU\\b', aff_string):
            affs.append(4210165232)

        # LCC
        if re.search('\\bLCC\\b', aff_string):
            affs.append(4210119060)

        # GAEL
        if re.search('\\bGAEL\\b', aff_string):
            affs.append(4210091947)

#         if 'CHU Montpellier' in aff_string:
#                 affs.append(#######) down below

        #################################################################

        # Institute of Electronics, Microelectronics and Nanotechnology
        if re.search('\\bIEMN\\b', aff_string):
            affs.append(4210123471)

        # Toulouse Institute of Computer Science Research
        if all(word in aff_string for word in ['Toulouse','IRIT']):
            affs.append(4210119061)

        # Lille
        if 'Lille' in aff_string:
            # CIC
            #if re.search('\\bCIC\\b', aff_string):
            #    affs.append()

            # University of Lille
            if any(word in aff_string for word in ['Univ Lille','Univ. Lille']):
                affs.append(2279609970)

            # CHU de Lille
            if any(word in aff_string for word in ['CHU Lille','CHU de Lille']):
                affs.append(3018718406)

            # École Centrale de Lille
            if 'Centrale Lille' in aff_string:
                affs.append(7454413)

        # Polytechnic University of Hauts-de-France
        if 'Polytechnique Hauts-de-France' in aff_string:
            affs.append(70348806)

        # Caen
        if 'Caen' in aff_string:
            # CHU de Caen
            if any(word in aff_string for word in ['CHU de Caen','CHU Caen']):
                affs.append(4210114068)
                affs.append(98702875)
            # Université de Caen Normandie
            elif any(word in aff_string for word in ['UNICAEN','Unicaen','Univ. de Caen']):
                affs.append(98702875)

            # University of Rouen
            if 'UNIROUEN' in aff_string:
                affs.append(62396329)

        # Normandie
        if 'Normandie' in aff_string:
            # Normandie Université
            if any(word in aff_string for word in ['Normandie Univ', 'Univ Normandie']):
                affs.append(4210105918)

        # LEASP
        if any(word in aff_string for word in ['LEASP','Leasp']):
            affs.append(4210127234)

        # Institut de Recherche pour le Développement
        if 'IRD' in aff_string:
            if re.search('\\bIRD\\b', aff_string):
                affs.append(4210166444)

            if re.search('\\bIRDL\\b', aff_string):
                affs.append(4210126368)

        # École des Mines d'Alès
        if 'IMT Mines Ales' in aff_string:
            affs.append(4210127738)

        # Institut des Sciences de l'Evolution de Montpellier
        if re.search('\\bISEM\\b', aff_string):
            affs.append(4210105943)

        # Institut National de Recherches Archéologiques Préventives
        if re.search('\\bINRAP\\b', aff_string):
            affs.append(4210155116)

        # École Normale Supérieure de Lyon
        if 'ENS de Lyon' in aff_string:
            affs.append(113428412)

        # European Institute for Marine Studies
        if re.search('\\bIUEM\\b', aff_string):
            affs.append(4210157108)

        # Institute for the Separation Chemistry in Marcoule
        if re.search('\\bICSM\\b', aff_string):
            affs.append(4210147247)

        # Hôpital Bichat-Claude-Bernard
        if 'Hôpital Bichat' in aff_string:
            affs.append(4210145170)

        # Laboratory for Ocean Physics and Satellite Remote Sensing
        if re.search('\\bLOPS\\b', aff_string):
            affs.append(4210134272)

        # French Research Institute for Exploitation of the Sea
        if any(word in aff_string for word in ['Ifremer','IFREMER']):
            affs.append(154202486)

        # Digestive Health Research Institute
        if re.search('\\bIRSD\\b', aff_string):
            affs.append(4210122796)

        # Institut Nanosciences et Cryogénie
        if re.search('\\bINAC\\b', aff_string):
            affs.append(4210124948)

        # Astrophysique, Instrumentation et Modélisation
        if re.search('\\bAIM\\b', aff_string):
            affs.append(4210086977)

        # Centre de Recherches sur les Fonctionnements et Dysfonctionnements Psychologiques
        if 'CRFDP' in aff_string:
            affs.append(4210136405)

        # LATMOS/IPSL (Institut Pierre-Simon Laplace)
        if 'IPSL' in aff_string:
            if 'LATMOS' in aff_string:
                affs.append(4210114102)

        # PSL
        if 'PSL' in aff_string:
            # PSL Research University
            if 'PSL Universit' in aff_string:
                affs.append(2746051580)

            # École Normale Supérieure - PSL
            if any(word in aff_string for word in ['École Normale Supérieure']):
                affs.append(29607241)
            elif re.search('\\bENS\\b', aff_string):
                affs.append(29607241)

        # University of Montpellier
        if 'Montpellier' in aff_string:
            if any(word in aff_string for word in ['Univ Montpellier','Univ. Montpellier',
                                                   'Université Montpellier','Montpellier Univ']):
                affs.append(19894307)

            if 'Institut Agro' in aff_string:
                affs.append(4210136436)

            if 'MISTEA' in aff_string:
                affs.append(4210117045)

#             if 'CHU Montpellier' in aff_string:
#                 affs.append(#######)

        # CHRU de Strasbourg
        if all(word in aff_string for word in ['CHRU','Strasbourg']):
            affs.append(4210145324)

        # Assistance Publique – Hôpitaux de Paris
        if re.search('\\bAP-HP\\b', aff_string):
            affs.append(4210097159)

        # Grenoble Alpes University
        if 'Grenoble' in aff_string:
            if any(word in aff_string for word in ['Univ. Grenoble Alp','University Grenoble Alp',
                                                'University of Grenoble Alp','Univ. Grenoble-Alp',
                                                'université Grenoble Alpe',
                                                'University of Grenoble-Alp','Université de Grenoble',
                                                'Universitaire de Grenobl','Grenoble Alps University',
                                                'Univ Grenoble Alp','Grenoble 1','Univ.Grenoble Alp',
                                                'University of Grenoble','University Grenoble-Alp',
                                                'University Of Grenoble','Univ Grenoble-Alp',
                                                'Grenoble University','Univ. Grenoble Alp',
                                                'Universit Grenoble Alp','Uni. Grenoble Alp',
                                                'Univeristy of Grenoble Alp','Université of Grenoble Alp',
                                                'Univ. Grenoble Alp','Univ. GrenobleAlp',
                                                'Grenoble Electrical Engineering Laboratory',
                                                'G2Elab','Univ, Grenoble Alp','Universitï¿½ Grenoble Alp',
                                                'Université Grenoble#N#Alp','Univ. de Grenoble Alp',
                                                'Univ. Grenoble Alp','université Grenoble-Alp',
                                                'Grenoble Alpes Univ','Univ. Grenoble Grenoble Alp',
                                                'Universit de Grenoble-Alp','Univ.\xa0Grenoble Alp',
                                                'Univ-Grenoble Alp','Université#R#Grenoble#R#Alp',
                                                'université de Grenoble-Alp','U. Grenoble Alp',
                                                'Université Grenoble Alp']):
                affs.append(899635006)
            elif re.search('\\bUGA\\b', aff_string):
                affs.append(899635006)
            elif re.search('\\bBIG\\b', aff_string):
                affs.append(899635006)

            if re.search('\\bIBS\\b', aff_string):
                affs.append(4210152516)
            elif 'Institut de Biologie Structurale' in aff_string:
                affs.append(4210152516)

            if re.search('\\bIAB\\b', aff_string):
                affs.append(4210160510)
            if re.search('\\bLIG\\b', aff_string):
                affs.append(4210104430)
            if re.search('\\bLJK\\b', aff_string):
                affs.append(4210149092)

            # Grenoble Institute of Technology
            if any(word in aff_string for word in ['Minatec','MINATEC']):
                affs.append(106785703)

            # Inria Grenoble - Rhône-Alpes research centre
            if 'Inria' in aff_string:
                affs.append(4210101348)

            # Laboratoire d'Écologie Alpine
            if re.search('\\bLECA\\b', aff_string):
                affs.append(4210137965)

            # Joseph Fourier University
            if re.search('\\bUJF\\b', aff_string):
                affs.append(177483745)
            elif any(word in aff_string for word in ['University J. Fourier','Université J. Fourier',
                                                    'J. Fourier University']):
                affs.append(177483745)

            # Science et Ingénierie des Matériaux et Procédés
            if re.search('\\bSIMaP\\b', aff_string):
                affs.append(4210094574)

            # Grenoble Images Parole Signal Automatique
            if re.search('\\bGipsa\\b', aff_string):
                affs.append(4210124956)

            # Institute of Environmental Geosciences
            if re.search('\\bIGE\\b', aff_string):
                affs.append(4210121220)

            # Laboratoire Jean Kuntzmann
            if re.search('\\bLJK\\b', aff_string):
                affs.append(4210149092)

            # Centre Hospitalier Universitaire de Grenoble
            if re.search('\\bCHU\\b', aff_string):
                affs.append(2800555055)
            elif any(word in aff_string for word in ['CHRU de Grenoble','Grenoble University Hospital',
                                                    'University Hospital of Grenoble',
                                                    'University Hospital Grenoble',
                                                    'Grenoble-Alps University Hospital']):
                affs.append(2800555055)
                affs.append(899635006)

        elif 'Toulouse' in aff_string:
            if re.search('\\bUGA\\b', aff_string):
                affs.append(899635006)

            # Paul Sabatier University
            if re.search('\\bUPS\\b', aff_string):
                affs.append(134560555)
            elif any(word in aff_string for word in ['Paul Sabatier','Toulouse III','Paul-Sabatier','Toulouse-III',
                                                     'Inserm U1027','INSERM U1027', 'UMR1295','UMR 1295']):
                affs.append(134560555)
            elif re.search('\\bIRIT\\b', aff_string):
                affs.append(134560555)
                affs.append(3131550300)
                affs.append(4210152422)
                affs.append(4210160189)

            # École Nationale Vétérinaire de Toulouse
            if re.search('\\bENVT\\b', aff_string):
                affs.append(176063091)

        # University of Western Brittany
        if any(word in aff_string for word in ['Bretagne-Occidentale','Bretagne Occidentale','Lab-STICC',
                                               'Laboratoire des Sciences et Techniques de l’Information, de la Communication et de la Connaissance','Laboratoire STICC',
                                               'Lab STICC','Institut de Recherche Dupuy de Lôme','Univ Brest',
                                               'Laboratoire de Microbiologie des Environnements Extrêmes',
                                               'University of Brest','Univ. Brest','Université de Brest']):
            affs.append(161929037)
        elif re.search('\\bUBO\\b', aff_string):
            affs.append(161929037)
        elif re.search('\\bLM2E\\b', aff_string):
            affs.append(161929037)

        if re.search('\\bSTICC\\b', aff_string):
            affs.append(4210123702)
        if re.search('\\bIRDL\\b', aff_string):
            affs.append(4210126368)

        if any(word in aff_string for word in ['CHRU de Brest','CHRU Brest','Cavale-Blanche']):
            affs.append(4210132604)


        # Laboratoire d'études spatiales et d'instrumentation en astrophysique (LESIA)
        if any(word in aff_string for word in ["Laboratoire d'études spatiales et d'instrumentation en astrophysique",
                                               "LESIA"]):
            affs.append(4210120578)


        # Versailles Saint-Quentin-en-Yvelines University
        if any(word in aff_string for word in ['UVSQ','Versailles Saint-Quentin-en-Yvelines',
                                               'Versailles Saint Quentin en Yvelines',
                                               'Versailles SaintQuentin-en-Yvelines']):
            affs.append(195731000)

        # Université Gustave Eiffel
        if any(word in aff_string for word in ['Gustave Eiffel','Gustave-Eiffel','ISterre',
                                               'Paris-Est Marne-la-Vallée','ISTerre',
                                               'Paris Est Marne la Vallée']):
            affs.append(4210154111)
        elif re.search('\\bUPEM\\b', aff_string):
            affs.append(4210154111)
        elif re.search('\\bUMRAE\\b', aff_string):
            affs.append(4210154111)
        elif re.search('\\bESIEE\\b', aff_string):
            affs.append(4210154111)
#             affs.append(####)

        # Toulouse Institute of Technology
        if any(word in aff_string for word in ['Toulouse Institute of Technology',
                                               'National Polytechnic Institute of Toulouse',
                                               'Institut National Polytechnique de Toulouse',
                                               'École Nationale Supérieure Agronomique de Toulouse',
                                               'École Nationale Supérieure d’Électrotechnique, d’Électronique, d’Informatique, d’Hydraulique et des Télécommunications',
                                               'École Nationale Supérieure des Ingénieurs en Arts Chimiques et Technologiques',"École Nationale d'Ingénieurs de Tarbes",
                                               'École Nationale de la Météorologie',"École d'ingénieurs de Purpan",
                                               "École Nationale Vétérinaire de Toulouse",'ENSIACET','ENIT',
                                               'INP ENM','ENSAT','ENSEEIHT']):
            affs.append(205747304)
        elif re.search('\\bINPT\\b', aff_string):
            affs.append(205747304)

        # University of Rouen Normandy (Université de Rouen Normandie
        if 'Rouen' in aff_string:
            if any(word in aff_string for word in ['Rouen Normandy','Rouen Normandie','Universitaire de Rouen',
                                                'Université de Rouen','INSA Rouen','CHU Rouen','CHU de Rouen',
                                                'Rouen University','UNIROUEN']):
                affs.append(62396329)
            elif ('boulevard Gambetta' in aff_string) and ('CHU' in aff_string) and ('Rouen' in aff_string):
                affs.append(62396329)

        # Institut Polytechnique de Paris
        if any(word in aff_string for word in ['Telecom SudParis','Télécom SudParis','Telecom SudParís',
                                               'TELECOM SudParis','Telecom-SudParis','Telecom Sudparis',
                                               'Télécom Sud Paris','Telecom SudParis','IPParis']):
            affs.append(4210145102)
        elif re.search('\\bIPP\\b', aff_string):
            if any(word in aff_string.lower() for word in ['palaiseau','paris']):
                affs.append(4210145102)

        # Université Paris Saclay
        if any(word in aff_string for word in ['Paris-Saclay Univ', 'Université Paris Saclay',
                                               'Université Paris-Saclay','Universit Paris Saclay',
                                               'Paris Saclay Univ','University of Paris-Saclay',
                                               'Universit Paris-Saclay','Univ. Paris-Saclay']):
            affs.append(277688954)

        if any(word in aff_string for word in ['Ecole polytechnique','École polytech','Ecole Polytech']):
            if 'Computer Science Laboratory' not in aff_string:
                if any(word in aff_string.lower() for word in ['palaiseau','paris']):
                    affs.append(142476485)

        if any(word in aff_string for word in ['Univ. Paris-Sud','Université Paris Sud',
                                               'Université Paris-Sud', 'University of Paris-Sud',
                                               'University of Paris Sud','Paris-Sud Univ','Paris Sud Univ']):
            affs.append(102197404)


        if re.search('\\bLTCI\\b', aff_string):
            affs.append(4210165912)

        if re.search('\\bLULI\\b', aff_string):
            affs.append(4210087526)

        if re.search('\\bENSAI\\b', aff_string):
            affs.append(84009706)

        if re.search('\\bIRMAR\\b', aff_string):
            affs.append(4210161663)

        if re.search('\\bEPOC\\b', aff_string):
            affs.append(4210099840)
        elif ('Epoc' in aff_string) and ('Pessac' in aff_string):
            affs.append(4210099840)
        elif 'EpOC Lab' in aff_string:
            affs.append(4210099840)

        # University of Bordeaux
        if 'Bordeaux' in aff_string:
            if any(word in aff_string for word in ['University of Bordeaux','Univ. Bordeaux','Univ Bordeaux',
                                                   'Université de Bordeaux']):
                affs.append(15057530)

        # Institut Polytechnique de Bordeaux
        if re.search('\\bIMS\\b', aff_string):
            affs.append(4210160189)
        elif any(word in aff_string for word in ['Bordeaux INP','Bordeaux-IPB']):
            affs.append(4210160189)
        elif re.search('\\bIPB\\b', aff_string):
            affs.append(4210160189)
        elif re.search('\\bI2M\\b', aff_string):
            affs.append(4210160189)
        elif any(word in aff_string for word in ['UMR CNRS 5218','UMR 5218',
                                                 'CNRS 5218','UMR5218','UMR5248','UMR 5248']):
            affs.append(4210160189)
        elif 'Bordeaux' in aff_string:
            if any(word in aff_string for word in ['Microbiologie Fondamentale et Pathogénicité',
                                                   'Institute of Mathematics',
                                                   'Bordeaux Institut National Polytechnique',
                                                   'Institut Polytechnique Bordeaux']):
                affs.append(4210160189)

        elif 'ICMCB' in aff_string:
            affs.append(15057530)

        if re.search('\\bISM\\b', aff_string):
            affs.append(4210086194)
        elif 'Institute of Molecular Science' in aff_string:
            affs.append(4210086194)

        if re.search('\\bLCPO\\b', aff_string):
            affs.append(4210144122)

        if re.search('\\bIMB\\b', aff_string):
            affs.append(4210166017)
        elif any(word in aff_string for word in ['UMR5251','UMR 5251']):
            affs.append(4210166017)

        if any(word in aff_string for word in ['LABRI','LaBRI']):
            affs.append(4210142254)
        elif re.search('\\bLabri\\b', aff_string):
            affs.append(4210142254)

        if re.search('\\bIECB\\b', aff_string):
            affs.append(4210144489)

        # Sorbonne University (Paris-Sorbonne University + Pierre-and-Marie-Curie University)
        if 'Sorbonne' in aff_string:
            if any(word in aff_string for word in ['Sorbonne Univ','Sorbonne-University','Université Sorbonne',
                                                    'Université-Sorbonne','Sorbonne-Université','Paris-Sorbonne University',
                                                    'Sorbonne Université','Sorbonne université',
                                                    'Paris Sorbonne University','Pierre-and-Marie-Curie University',
                                                    'Pierre et Marie Curie','Pitié-Salpêtrière','Pitié Salpêtrière']):
                if not any(word in aff_string for word in ['Université-Sorbonne-Paris','Panthéon-Sorbonne University',
                                                        'Panthéon-Sorbonne University','Université Sorbonne-Nouvelle',
                                                        'Université Sorbonne Nouvelle','New Sorbonne University']):
                    if 'Abu Dhabi' not in aff_string:
                        affs.append(39804081)

        if 'Paris' in aff_string:
            # University of Paris 1 Panthéon-Sorbonne
            if (re.search('\\bParis I\\b', aff_string)) or (re.search('\\bParis 1\\b', aff_string)):
                affs.append(51101395)
            elif re.search('\\bCRED\\b', aff_string):
                affs.append(51101395)
            elif any(word in aff_string for word in ['Panthéon-Sorbonne','Panthéon Sorbonne','Pantheon-Sorbonne',
                                                    'Pantheon Sorbonne','Centre de Recherche en Économie et Droit']):
                affs.append(51101395)

            # Paris-Panthéon-Assas University (Paris II Panthéon-Assas)
            if (re.search('\\bParis II\\b', aff_string)) or (re.search('\\bParis 2\\b', aff_string)):
                affs.append(117841876)
            elif any(word in aff_string for word in ['Paris-Panthéon-Assas','Paris Panthéon Assas','Paris-Pantheon-Assas',
                                                    'Paris Pantheon Assas']):
                affs.append(117841876)

            # Université Sorbonne Nouvelle (New Sorbonne University)
            if (re.search('\\bParis III\\b', aff_string)) or (re.search('\\bParis 3\\b', aff_string)):
                affs.append(182627622)
            elif any(word in aff_string for word in ['Sorbonne Nouvelle','Sorbonne-Nouvelle']):
                affs.append(182627622)

            # Sorbonne University (Paris-Sorbonne University + Pierre-and-Marie-Curie University)
            if (re.search('\\bParis IV\\b', aff_string)) or (re.search('\\bParis 4\\b', aff_string)):
                affs.append(39804081)
            elif (re.search('\\bParis VI\\b', aff_string)) or (re.search('\\bParis 6\\b', aff_string)):
                affs.append(39804081)
            elif re.search('\\bUPMC\\b', aff_string):
                affs.append(39804081)

            # Paris Cité University (Paris Descartes University + Paris Diderot University)
            if (re.search('\\bParis V\\b', aff_string)) or (re.search('\\bParis 5\\b', aff_string)):
                affs.append(204730241)
            elif (re.search('\\bParis VII\\b', aff_string)) or (re.search('\\bParis 7\\b', aff_string)):
                affs.append(204730241)
            elif any(word in aff_string for word in ['Paris Cité','Paris Diderot','Paris Descartes','René Descartes',
                                                    'Paris-Cité','Paris-Diderot','Paris-Descartes','Denis Diderot']):
                affs.append(204730241)

            # Paris 8 University Vincennes-Saint-Denis (University of Vincennes)
            if (re.search('\\bParis VIII\\b', aff_string)) or (re.search('\\bParis 8\\b', aff_string)):
                affs.append(48825208)
            elif any(word in aff_string for word in ['Vincennes-Saint-Denis','Vincennes Saint Denis']):
                affs.append(48825208)

            # Paris Dauphine University - PSL (grande école of PSL University)
            if (re.search('\\bParis IX\\b', aff_string)) or (re.search('\\bParis 9\\b', aff_string)):
                affs.append(56435720)

            # Paris Nanterre University (Université Paris Ouest)
            if (re.search('\\bParis X\\b', aff_string)) or (re.search('\\bParis 10\\b', aff_string)):
                affs.append(40434647)

            # Paris-Saclay University (Université Paris-Sud)
            if (re.search('\\bParis XI\\b', aff_string)) or (re.search('\\bParis 11\\b', aff_string)):
                affs.append(277688954)

            # Paris-East Créteil University (Université Paris-Est)
            if (re.search('\\bParis XII\\b', aff_string)) or (re.search('\\bParis 12\\b', aff_string)):
                affs.append(197681013)
            elif re.search('\\bUPEC\\b', aff_string):
                affs.append(197681013)
            elif any(word in aff_string for word in ['Paris Est Créteil','Paris-Est Créteil','Paris Est Creteil',
                                                     'Paris-Est Creteil']):
                affs.append(197681013)

            # Sorbonne Paris North University (Université Paris Nord)
            if (re.search('\\bParis XIII\\b', aff_string)) or (re.search('\\bParis 13\\b', aff_string)):
                affs.append(4210091279)
            elif any(word in aff_string for word in ['Sorbonne Paris North','Sorbonne Paris Nord',
                                                    'Université Sorbonne-Paris-Nord',
                                                    'Sorbonne North Paris University']):
                affs.append(4210091279)

    # CUNY Graduate Center
    if any(word in aff_string for word in ['New York','USA','CUNY']) or re.search('\\bNY\\b', aff_string):
        if (re.search('\\bCUNY\\b', aff_string)) or ('City University of New York'):
            if any(word in aff_string for word in ['Graduate Center','Graduate School and University Center',
                                                   'The Graduate School','Advanced Science Research Center']):
                affs.append(121847817)
            elif re.search('\\bASRC\\b', aff_string):
                affs.append(121847817)


    # Tunis El Manar University
    if any(word in aff_string for word in ['Tunis','Tunisia']):
        if any(word in aff_string for word in ['Tunis El Manar']):
            affs.append(63596082)
        elif re.search('\\bUTM\\b', aff_string):
            affs.append(63596082)

    # Louisiana State University Health Sciences Center New Orleans (need to remove 81020160)
    if any(word in aff_string for word in ['LSU','Louisiana']) or re.search('\\bLA\\b', aff_string):
        if 'Health Science' in aff_string:
            if any(word in aff_string for word in ['LSU Health Science','(LSU) Health Science',
                                                   'Louisiana State University Health Science']):
                if 'New Orleans' in aff_string:
                    affs.append(75420490)

    # University of Trieste
    if 'Italy' in aff_string:
        if any(word in aff_string for word in ['INFN, Sezione di Trieste','UniTS','INFN Sezione di Trieste',
                                               'INFN Trieste','Università degli Studi di Trieste']):
            affs.append(142444530)

    # University of Tennessee at Knoxville
    if 'Knoxville' in aff_string:
        if 'University of Tennessee' in aff_string:
            if 'University of Tennessee System' not in aff_string:
                affs.append(75027704)
        elif re.search('\\bUT\\b', aff_string):
            affs.append(75027704)

    # China University of Geosciences, Wuhan (need to remove 3016766249)
    if 'China University of Geosciences' in aff_string:
        if 'Wuhan' in aff_string:
            affs.append(3124059619)

    # Texas Tech University (remove 4210088475)
    if 'Texas' in aff_string:
        if 'Texas Tech University' in aff_string:
            if 'Texas Tech University System' not in aff_string:
                affs.append(12315562)

    # Shandong Academy of Medical Science
    if 'Shandong' in aff_string:
        if any(word in aff_string for word in ['Shandong Academy of Medical Science',
                                               'Shandong First Medical University']):
            if 'Affiliated Hospital of Shandong Academy of Medical Sciences' not in aff_string:
                affs.append(4210163399)

    # Anhui University of Science and Technology
    if 'Anhui' in aff_string:
        if any(word in aff_string for word in ['Anhui Science and Technology Univ']):
            affs.append(184681353)

    # Army Medical University
    if 'Army' in aff_string:
        if any(word in aff_string for word in ['Third Military Medical Univ','Army Medical Univ']):
            affs.append(151075929)
        elif 'China' in aff_string:
            if 'Army Medical Center' in aff_string:
                affs.append(151075929)
    elif 'Military' in aff_string:
        if 'Third Military Medical Univ' in aff_string:
            affs.append(151075929)

    # Banaras Hindu University
    if 'Varanasi' in aff_string:
        if re.search('\\bBHU\\b', aff_string):
            affs.append(91357014)

    # Cadi Ayyad University
    if 'Cadi Ayyad' in aff_string:
        if any(word in aff_string for word in ['Université Cadi Ayyad']):
            affs.append(119856527)

    if 'China' in aff_string:
        # Central South University
        if 'Central South Univ' in aff_string:
            affs.append(139660479)

        # China Academy of Chinese Medical Sciences
        if 'China Academy of Chinese Medical Science' in aff_string:
            affs.append(4210141683)

        # China University of Geosciences
        if any(word in aff_string for word in ['Chinese University of Geoscience',
                                               'China University of Geoscience']):
            if 'Wuhan' in aff_string:
                affs.append(3124059619)

            if 'Beijing' in aff_string:
                affs.append(3125743391)

        # Chongqing Medical University
        if 'Chongqing Medical Univ' in aff_string:
            affs.append(87780372)

        # Civil Aviation University of China
        if 'Civil Aviation University of China' in aff_string:
            affs.append(28813325)

        # Civil Aviation Flight University of China
        if 'Civil Aviation Flight University of China' in aff_string:
            affs.append(58995867)

        # Dalian Medical University
        if 'Dalian Medical Univ' in aff_string:
            affs.append(191996457)

        # Fujian Medical University
        if 'Fujian Medical Univ' in aff_string:
            affs.append(129708740)

        # Guangzhou Medical University
        if 'Guangzhou Medical Univ' in aff_string:
            affs.append(92039509)

        # Harbin Medical University
        if 'Harbin Medical Univ' in aff_string:
            affs.append(156144747)

        # Hebei Medical University
        if 'Hebei Medical Univ' in aff_string:
            affs.append(111381250)

        # Jichi Medical University
        if 'Jichi' in aff_string:
            if any(word in aff_string for word in ['Jichi Medical Univ','Jichi-Medical Univ']):
                affs.append(146500386)

        # Kunming Medical University
        if 'Kunming' in aff_string:
            if any(word in aff_string for word in ['Kunming Medical Univ', 'Kunming Medical College']):
                affs.append(26080491)

        # University of South China
        if 'University of South China' in aff_string:
            affs.append(91935597)

        # Shandong Medical College
        if 'Shandong Medical College' in aff_string:
            affs.append(4210163399)

        # Shanghai University of TCM
        if 'Shanghai University of TCM' in aff_string:
            affs.append(4210098460)

    # Czech Technical University
    if 'Czech' in aff_string:
        if 'Czech Technical Univ' in aff_string:
            if any(word in aff_string for word in ['Prague','Praha']):
                affs.append(44504214)

    # Georgetown University
    if 'Georgetown' in aff_string:
        if 'Georgetown Univ' in aff_string:
            if any(word in aff_string for word in ['Doha','Qatar']):
                pass
            else:
                affs.append(184565670)

    # Goethe University Frankfurt
    if 'Frankfurt' in aff_string:
        if any(word in aff_string for word in ['Universitätsklinikum Frankfurt',
                                               'Universitatsklinikum Frankfurt',
                                               'University Hospital Frankfurt',
                                               'Goethe University Hospital',
                                               'Goethe University Frankfurt']):
            affs.append(114090438)

    # Ludwig-Maximilians-Universität München
    if any(word in aff_string for word in ['Munich','München']):
        if all(word in aff_string for word in ['Ludwig', 'Maximilian']):
            if 'Univ' in aff_string:
                affs.append(8204097)
        elif re.search('\\bLMU\\b', aff_string):
            affs.append(8204097)

    # University of Macau
    if 'Macau' in aff_string:
        if any(word in aff_string for word in ['Macau University', 'University of Macau','Univ. of Macau','Universidade de Macau']):
            if not any(word in aff_string for word in ['City University of Macau','Macau University of Science','City Univ. of Macau']):
                affs.append(204512498)

    # Near East University
    if 'Near East University' in aff_string:
        if any(word in aff_string for word in ['Turkey','Nicosia']):
            affs.append(69050122)
    elif 'Turkey' in aff_string:
        if any(word in aff_string for word in ['Yakin Dogu University','Yakın Doğu Üniversitesi','Yakın Doğu University']):
            affs.append(69050122)

    # National Yang Ming Chiao Tung University
    if 'Yang' in aff_string:
        if any(word in aff_string for word in ['National Yang-Ming University', 'National Yang Ming University',
                                               'Yang-Ming Medical University','National Yang‐Ming University']):
            affs.append(148366613)
        elif 'Taiwan' in aff_string:
            if 'Taipei' in aff_string:
                if 'Ming University' in aff_string:
                    affs.append(148366613)
                elif 'Yang-Ming' in aff_string:
                    if 'University' in aff_string:
                        affs.append(148366613)

    # University of Colorado Denver
    if 'Denver' in aff_string:
        if any(word in aff_string for word in ['University of Colorado at Denver']):
            affs.append(921990950)
        elif any(word in aff_string for word in ['UC Denver/Anschutz', 'University of Colorado Denver/Anschutz']):
            affs.append(921990950)
            affs.append(51713134)


    # Sağlık Bilimleri Üniversitesi
    if any(word in aff_string for word in ['Istanbul','İstanbul','Ankara','Turkey','Türkiye']):
        if any(word in aff_string for word in ['Health Science University','Health Sciences University','University of Health Science']):
            affs.append(4210128276)

    if 'Russia' in aff_string:
        # Moscow Engineering Physics Institute
        if any(word in aff_string for word in ['Moscow Physical Engineering Institute',
                                               'Moscow Engineering Physics Insitute',
                                               'National Research Nuclear University', 'MEPhI']):
            affs.append(887846188)

        # Moscow Institute of Physics and Technology
        if any(word in aff_string for word in ['Moscow Institute of Physics and Technology',
                                               'Moscow institute of physics and technology',
                                               'Moscow Institute for Physics and Technology',
                                               'Moscow Institute of Physics and Technologies',
                                               'Moscow Institute of physics and technology',
                                               'Moscow Institute for Physics and Technology',
                                               'Moscow Institute of Physisc and Technologies',
                                               'Moscow Institute of Technology and Physics']):
            affs.append(153845743)
        elif 'Moscow' in aff_string:
            if 'Institute of Physics and Technology' in aff_string:
                affs.append(153845743)

    # The University of Texas Health Science Center at San Antonio
    if 'San Antonio' in aff_string:
        if any(word in aff_string for word in ['University of Texas Health']):
            affs.append(165951966)

    if 'Hawai' in aff_string:
        # University of Hawaiʻi at Mānoa
        if any(word in aff_string for word in ['Mānoa','Manoa','Monoa']):
            if any(word in aff_string for word in ['University of Hawai']):
                affs.append(117965899)
        elif "University of Hawai'i at Mā noa" in aff_string:
            affs.append(117965899)

    if 'Germany' in aff_string:
        if any(word in aff_string for word in ['Luebeck','Lübeck']):
            if any(word in aff_string for word in ['University of Luebeck','University of Lübeck']):
                affs.append(9341345)

    # University of Split
    if 'Sveučilišta u Splitu' in aff_string:
        affs.append(92251255)

    # University of Eastern Piedmont Amadeo Avogadro
    if 'Italy' in aff_string:
        if 'Azienda Ospedaliera' in aff_string:
            if any(word in aff_string for word in ['Maggiore della Carità','Maggiore della Carita']):
                affs.append(123338534)
                affs.append(4210119436)
        elif 'Maggiore Della Carità Hospital' in aff_string:
            affs.append(123338534)
            affs.append(4210119436)
        elif any(word in aff_string for word in ['University of East Piedmont','Eastern Piedmont University',
                                                 'Università del Piemonte Orientale']):
            if any(word in aff_string for word in ['Alessandria','Novara','Vercelli','Torino','Turin','Maggiore della Carità']):
                affs.append(123338534)

    # Palacky University
    if 'Olomouc' in aff_string:
        if any(word in aff_string for word in ['Palacký University','Palacky University']):
            affs.append(70703428)

    # National Research University Higher School of Economics
    if 'higher school of economics' in aff_string.lower():
        affs.append(118501908)

    # National Research Tomsk State University
    if 'Tomsk State University' in aff_string:
        if 'Tomsk State University of' not in aff_string:
            affs.append(196355604)

    # University of Georgia
    if 'University of Georgia' in aff_string:
        if any(word in aff_string for word in ['GA','USA','United States','Athens','Griffin','Atlanta','Augusta']):
            affs.append(165733156)

    # University of Colorado
    if 'University of Colorado, Aurora' in aff_string:
        affs.append(51713134)

    # Baylor University
    if 'Baylor University' in aff_string:
        if 'Baylor University Medical' not in aff_string:
            affs.append(157394403)

    # Boğaziçi University
    if 'BoĿaziçi University' in aff_string:
        affs.append(4405392)

    # Xuzhou Medical College
    if 'Xuzhou' in aff_string:
        if any(word in aff_string for word in ['Xuzhou Medicine University','Xuzhou Stomatology Hospital',
                                               'Xuzhou Medicinal University','Xuzhou Medical University']):
            affs.append(177388780)

    # University of Reims Champagne-Ardenne
    if 'Reims' in aff_string:
        if 'Maison Blanche' in aff_string.replace("-", " "):
            if any(word in aff_string for word in ['CHU', 'Hospital','Hôpital','hôpital','hospital']):
                affs.append(96226040)
                affs.append(4210105796)
        elif any(word in aff_string for word in ['CHRU de Reims','CHU de Reims','CHU Reims', 'CHRU Reims',
                                                 'Reims University Hospital','University Hospital of Reims']):
            affs.append(96226040)
            affs.append(4210105796)
        elif any(word in aff_string for word in ['Université de Reims','Univ of Reims','université de Reims']):
            affs.append(96226040)

    # Soochow University
    if 'Soochow Univ' in aff_string:
        if any(word in aff_string for word in ['China','Suzhou', 'Jiangsu','Changshu','CHINA','china']):
            affs.append(3923682)

    # TMU/Ryerson
    if re.search('\\bX University\\b', aff_string):
        if any(word in aff_string for word in ['Canada','Toronto','Ontario']):
            affs.append(530967)

    # Universidad Central de Chile
    if 'Chile' in aff_string:
        if any(word in aff_string for word in ['Universidad Central de Chile','Central University of Chile']):
            affs.append(4210156023)
        elif re.search('\\bUCEN\\b', aff_string):
            affs.append(4210156023)

    # University of Hong Kong (and others)
    if 'University of Hong Kong' in aff_string:
        if 'Chinese University of Hong Kong' in aff_string:
            if 'Shenzhen' in aff_string:
                affs.append(4210116924)
            else:
                affs.append(177725633)
        elif 'Education University of Hong Kong' in aff_string:
            affs.append(4210086892)
        elif 'City University of Hong Kong' in aff_string:
            affs.append(168719708)
        elif 'Open University of Hong Kong' in aff_string:
            affs.append(8679417)
        else:
            affs.append(889458895)

    # Krembil Research Institute
    if 'Krembil Research Institute' in aff_string:
        affs.append(4388446386)

    # For Canada institutions
    if any(word in aff_string for word in ['Canada','Quebec','Québec','Montréal','Montreal','Trois-Rivières',
                                           'Trois-Rivieres','Chicoutimi','Rimouski','Outaouais','Abitibi']):

        u_quebec_strings = ['Université du Québec','University of Quebec','University of Québec','Quebec University',
                             'Universite du Quebec','Univ Quebec','Univ. Quebec','Univ Québec','Univ. Québec',
                             'Quebec Univ.','Québec Univ.','Univ. of Quebec','Univ. of Québec','U de Québec',
                             'U de Quebec','Univ. du Quebec','Univ. du Québec','Université du Quebec']

        # UQ
        if 'UQ' in aff_string:
            if re.search('\\bUQTR\\b', aff_string):
                affs.append(63341726)
            elif re.search('\\bUQÀM\\b', aff_string):
                affs.append(159129438)
            elif re.search('\\bUQAM\\b', aff_string):
                affs.append(159129438)
            elif re.search('\\bUQÀC\\b', aff_string):
                affs.append(104914703)
            elif re.search('\\bUQAC\\b', aff_string):
                affs.append(104914703)
            elif re.search('\\bUQÀR\\b', aff_string):
                affs.append(182451676)
            elif re.search('\\bUQAR\\b', aff_string):
                affs.append(182451676)
            elif re.search('\\bUQÀT\\b', aff_string):
                affs.append(190270569)
            elif re.search('\\bUQAT\\b', aff_string):
                affs.append(190270569)
            elif re.search('\\bUQO\\b', aff_string):
                affs.append(33217400)

        # University of Quebec at Montréal
        if any(word in aff_string for word in ['Montréal','Montreal']):
            if any(word in aff_string for word in u_quebec_strings):
                affs.append(159129438)

        # University of Quebec at Trois-Rivières
        if any(word in aff_string for word in ['Trois-Rivières','Trois-Rivieres','Trois Rivieres',
                                               'Trois Rivières','Three-Rivers', 'Three Rivers','Trois‐Rivieres']):
            if any(word in aff_string for word in u_quebec_strings):
                affs.append(63341726)

        # University of Quebec at Chicoutimi
        if any(word in aff_string for word in ['Chicoutimi']):
            if any(word in aff_string for word in u_quebec_strings):
                affs.append(104914703)

        # University of Quebec at Rimouski
        if any(word in aff_string for word in ['Rimouski']):
            if any(word in aff_string for word in u_quebec_strings):
                affs.append(182451676)

        # University of Quebec at Outaouais
        if any(word in aff_string for word in ['Outaouais','Hull']):
            if any(word in aff_string for word in u_quebec_strings):
                affs.append(33217400)

        # University of Quebec at Abitibi-Témiscamingue
        if any(word in aff_string for word in ['Abitibi-Témiscamingue','Abitibi Témiscamingue',
                                               'Abitibi Temiscamingue','Abitibi-Temiscamingue']):
            if any(word in aff_string for word in u_quebec_strings):
                affs.append(190270569)

        # TÉLUQ University
        if any(word in aff_string for word in ['Téluq','TÉLUQ','TELUQ']):
            affs.append(200745827)

        # Institut National de la Recherche Scientifique
        if re.search('\\bINRS\\b', aff_string):
            affs.append(39481719)
        elif re.search('\\bInrs\\b', aff_string):
            affs.append(39481719)
        elif any(word in aff_string
                for word in ['Institut National de la Recherche Scientifique', 'Institut Armand-Frappier',
                             'I.N.R.S.','Institut Armand Frappier','Armand-Frappier Institute',
                             'Institute Armand-Frappier','Institute Armand Frappier','Armand Frappier Institute',
                             'institute Armand‐Frappier','institute Armand Frappier']):
            affs.append(39481719)

        # Ecole national d'administration publique
        if re.search('\\bENAP\\b', aff_string):
            affs.append(31571312)
        elif any(word in aff_string
                for word in ["Ecole national d'administration publique","École Nationale d'Administration Publique",
                             "Ecole Nationale d'Administration Publique"]):
            affs.append(31571312)

        # Ecole de technologie superieure
        if re.search('\\bETS\\b', aff_string):
            affs.append(9736820)
        elif any(word in aff_string
                for word in ["Ecole de technologie superieure","École de Technologie Supérieure",
                             "Ecole de Technologie Superieure"]):
            affs.append(9736820)

    # University of Maryland
    if 'Maryland' in aff_string:
        if any(word in aff_string for word in ['Univ. of Maryland','Univ of Maryland','University of Maryland']):
            if 'Baltimore' in aff_string:
                if 'Baltimore County' in aff_string:
                    affs.append(79272384)
                else:
                    affs.append(126744593)
            elif any(word in aff_string for word in ['School of Medicine']):
                affs.append(126744593)
            elif any(word in aff_string for word in ['Princess Anne','Eastern Shore']):
                affs.append(22407884)
            elif 'College Park' in aff_string:
                affs.append(66946132)

    # Universität Hamburg
    if 'Hamburg' in aff_string:
        if any(word in aff_string for word in ['University of Hamburg','Univ of Hamburg','Univ. of Hamburg',
                                               'Hamburg Univ']):
            affs.append(159176309)
        elif any(word in aff_string for word in ['University Medical Center',
                                                 'University Medical Centre',
                                                 'University Hospital Eppendorf',
                                                 'University Heart Cent',
                                                 'Universitätsklinikum Eppendorf']):
            affs.append(159176309)
            affs.append(4210108711)
        elif 'Hamburg-Eppendorf' in aff_string:
            if 'Universit' in aff_string:
                affs.append(159176309)
                affs.append(4210108711)
        elif 'University Clinic' in aff_string:
            if 'Eppendorf' in aff_string:
                affs.append(159176309)
                affs.append(4210108711)
        elif re.search('\\bUKE\\b', aff_string):
            affs.append(159176309)
            affs.append(4210108711)

    # Commenius University Bratislava
    if any(word in aff_string for word in ['Comenious University','Commenius University',
                                           'Comenius University']):
        if any(word in aff_string for word in ['Slovakia','Bratislava']):
            affs.append(74788687)

    # Polytechnic University of Bari
    if 'Bari' in aff_string:
        if any(word in aff_string for word in ['Technical University of Bari','Technical Univ. of Bari',
                                            'University and Politecnico of Bari',
                                            'Technical University Politecnico di Bari',
                                            'Polytechnic University of Bari',
                                            'Politecnico di Bari',
                                            'Polytechnic of Bari',
                                            'Univ. and Politecnico of Bari','Polytechinic University of Bari']):
            affs.append(68618741)

     # University of Science and Technology of Hanoi
    if 'Hanoi' in aff_string:
        if any(word in aff_string for word in ['University of Science and Technology of Hanoi',
                                               'Hanoi University of Science and Technology']):
            affs.append(94518387)

        if any(word in aff_string for word in ['Graduate University of Science and Technology','VAST']):
            affs.append(70349855)
            if 94518387 in affs:
                if any(word in aff_string for word in ['University of Science and Technology of Hanoi',
                                                       'Hanoi University of Science and Technology']):
                    pass
                else:
                    affs.remove(94518387)

    elif 'Vietnam' in aff_string:
        if re.search('\\bUSTH\\b', aff_string):
            affs.append(94518387)

    # Sri Sivasubramaniya Nadar (SSN) College of Engineering
    if any(word in aff_string for word in ['India','Chennai','INDIA']):
        if any(word in aff_string for word in ['SSN College of Engineering',
                                               'Sri Sivasubramaniya Nadar College of Engineering',
                                               'SSN Engineering College',
                                               'Sri Sivasubramania Nadar College of Engineering',
                                               'SSN Research Centre',
                                               'Sri Sivasubramaniya Nadar (SSN) College of Engineering',
                                               'Sri Sivasubramanyia Nadar College of Engineering',
                                               'SSN college of Engineering',
                                               'Sri Sivasubrmaniya Nadar (SSN) College of Engineering',
                                               'SSN College OF Engineering',
                                               'Sri SivasubramaniyaNadar College of Engineering',
                                               'Sri Sivasubramaniya Nadar College of Engineering',
                                               'SSN Collage of Engineering',
                                               'Sri SivaSubramaniya Nadar College of Engineering',
                                               'Sri SivasubarmaniaNadar College of Engineering']):
            affs.append(916357946)
    elif 'Sri Sivasubramaniya Nadar College of Engineering' in aff_string:
        affs.append(916357946)

    # National Kaohsiung University of Science and Technology
    if 'Kaohsiung' in aff_string:
        if any(word in aff_string.lower() for word in ['kaohsiung university of sci',
                                                       'kaohsiung univ. of science and tech',
                                                       'kaohsiung university sciences and tech']):
            affs.append(4387154394)

    # National Research University Higher School of Economics
    if 'HSE' in aff_string:
        if 'Russia' in aff_string:
            affs.append(118501908)
        elif any(word in aff_string for word in ['HSE University','NRU HSE', 'National Research University HSE']):
            affs.append(118501908)

    # Jinzhou Medical University
    if 'jinzhou' in aff_string.lower():
        if any(word in aff_string.lower() for word in ['jinzhou medical univ', 'jinzhou medicical univ',
                                                       'jinzhou medicinal univ']):
            affs.append(85430964)

    # Christian Medical College
    if 'christian medical college' in aff_string.lower():
        affs.append(172917736)

    # AIIMS (All India Institute of Medical Sciences)
    if 'AIIMS' in aff_string:
        # All India Institute of Medical Sciences Bhubaneswar
        if 'Bhubaneswar' in aff_string:
            affs.append(4210117092)

        # All India Institute of Medical Sciences Bhopal
        elif 'Bhopal' in aff_string:
            affs.append(4210106490)

        # All India Institute of Medical Sciences, Nagpur
        elif 'Nagpur' in aff_string:
            affs.append(4401200305)

        # All India Institute of Medical Sciences Guwahati
        elif 'Guwahati' in aff_string:
            affs.append(4387153078)

        # All India Institute of Medical Sciences Rishikesh
        elif 'Rishikesh' in aff_string:
            affs.append(4387152206)

        # All India Institute of Medical Sciences Raipur
        elif 'Raipur' in aff_string:
            affs.append(129734738)

        # All India Institute of Medical Sciences Jodhpur
        elif 'Jodhpur' in aff_string:
            affs.append(216021267)

        # All India Institute of Medical Sciences, Deoghar
        elif 'Deoghar' in aff_string:
            affs.append(4396570500)

        # All India Institute of Medical Sciences, New Delhi
        else:
            affs.append(63739035)

    # Anna University
    if 'Anna University' in aff_string:
        if 'India' in aff_string:
            if 'Coimbatore' in aff_string:
                affs.append(4400600945)
            else:
                affs.append(33585257)
        elif 'Coimbatore' in aff_string:
            affs.append(4400600945)

    # Instituto Politécnico Nacional
    if 'IPN' in aff_string:
        if any(word in aff_string.lower() for word in ['mexico', 'méxico']):
            if 'CINVESTAV' in aff_string:
                affs.append(68368234)
            else:
                affs.append(59361560)
        if any(word in aff_string for word in ['OAXACA','Cinvestav','ESIME', 'CINVESTAV',
                                               'CIIDIR','CDMX','CICIMAR','CIDIIR','Escuela Superior de Medicina',
                                               'Estudios Avanzados','Actividades Académicas','Ciencias Marinas']):
            if 'cinvestav' in aff_string.lower():
                affs.append(68368234)
            else:
                affs.append(59361560)

    # Army Engineering University
    if 'Army Engineering University' in aff_string:
        if any(word in aff_string for word in ['Shijiazhuang','China']):
            affs.append(4210163363)

    # University of North Texas
    if 'University of North Texas' in aff_string:
        if 'Denton' in aff_string:
            affs.append(123534392)
        elif 'Health Science Center' in aff_string:
            affs.append(165139151)
            affs.append(123534392)
        elif 'Fort Worth' in aff_string:
            affs.append(165139151)
            affs.append(123534392)
        elif 'Dallas' in aff_string:
            affs.append(87573096)

    # University of Veterinary Medicine Hannover, Foundation
    if 'Hannover' in aff_string:
        if 'University of Veterinary Medicine' in aff_string:
            affs.append(189991)

    # Upstate Medical University Hospital
    if 'Syracuse' in aff_string:
        if 'Upstate Medical University' in aff_string:
            affs.append(20388574)
        elif 'Upstate College of Medicine' in aff_string:
            affs.append(20388574)

    # University of Cukurova
    if 'University of Cukurova' in aff_string:
        if 'Turkey' in aff_string:
            affs.append(55931168)

    # Panjab University
    if 'Panjab University' in aff_string:
        affs.append(51452335)

    # Punjab University
    if 'Punjab University' in aff_string:
        affs.append(172780181)

    # Universidad de Los Andes
    if 'Andes' in aff_string:
        if any(word in aff_string for word in ['Universidad de Los Andes','Universidad de los Andes',
                                               'University Los Andes']):
            if any(word in aff_string for word in ['Bogota', 'Colombia','Bogotá']):
                affs.append(162096671)

    # Tomsk Polytechnic University
    if 'Tomsk Polytechnic University' in aff_string:
        affs.append(196355604)

    # University College London
    if 'London' in aff_string:
        if 'University College London' in aff_string:
            affs.append(45129253)
        elif re.search('\\bUCL\\b', aff_string):
            affs.append(45129253)

    # Radboud University
    if re.search('\\bradboud university\\b', aff_string.lower()):
        if 'radboud university medical cent' in aff_string.lower():
            affs.append(145872427)
            affs.append(2802934949)
        else:
            affs.append(145872427)
    elif 'Netherlands' in aff_string:
        if (re.search('\\bUMC\\b', aff_string)) or ('University Medical Cent' in aff_string):
            if any(word in aff_string for word in ['Radboud','Nijmegen']):
                affs.append(145872427)
                affs.append(2802934949)

    # Zhejiang Agriculture and Forestry University
    if 'Zhejiang Agriculture and Forestry University' in aff_string:
        affs.append(1284762954)
    elif 'Zhejiang Agricultural and Forestry University' in aff_string:
        affs.append(1284762954)

    # Technical University Dortmund
    if 'Dortmund' in aff_string:
        if any(word in aff_string for word in ['Technical University Dortmund','TU Dortmund',
                                               'Dortmund University of Technology','Technische Universitaet Dortmund',
                                               'Technical University of Dortmund']):
            affs.append(200332995)

    # University of Illinois, Chicago
    if 'Chicago' in aff_string:
        if any(word in aff_string for word in ['University of Illinois']):
            if 'Rockford' in aff_string:
                pass
            else:
                affs.append(39422238)
        elif re.search('\\bUIC\\b', aff_string):
            affs.append(39422238)

    # Southern Methodist University
    if 'Dallas' in aff_string:
        if re.search('\\bSMU\\b', aff_string):
            affs.append(178169726)

    # Universitat Politècnica de Catalunya
    if 'Barcelona' in aff_string:
        if any(word in aff_string for word in ['Barcelona Supercomputing Cent', 'BarcelonaTech',
                                               'BarcelonaTECH','Barcelona Supercomputer Cent']):
            affs.append(9617848)
        elif re.search('\\bUPC\\b', aff_string):
            affs.append(9617848)
        elif re.search('\\bBSC\\b', aff_string):
            affs.append(9617848)
        elif re.search('\\bCIMNE\\b', aff_string):
            affs.append(9617848)
    elif 'Spain' in aff_string:
        if any(word in aff_string for word in ['Technical University of Catalonia','Universidad Politécnica De Cataluña',
                                               'Universidade Politécnica de Cataluña','Polytechnic University of Catalonia']):
            affs.append(9617848)

    # Johns Hopkins University
    if 'Baltimore' in aff_string:
        if re.search('\\bJHU\\b', aff_string):
            affs.append(145311948)
        elif 'Johns Hopkins' in aff_string:
            if any(word in aff_string for word in ['Bloomberg School of Public Health','School of Medicine',
                                                   'Medical School','Medical Institution']):
                affs.append(145311948)
    elif 'Johns Hopkins University' in aff_string:
        if any(word in aff_string for word in ['Applied Physics Laboratory','APL','Makerere','Nanjing','Bologna']):
            pass
        else:
            affs.append(145311948)

    # Johannes Gutenberg University Mainz
    if 'Mainz' in aff_string:
        if any(word in aff_string for word in ['Johannes Gutenberg University','Mainz University Hospital',
                                               'Universitätsmedizin Mainz','University Hospital Mainz',
                                               'University Medical Center Mainz','Universitaetsmedizin Mainz',
                                               'Johannes Gutenberg-Universität','University of Mainz',
                                               'University Medicine Mainz','Universitätsklinik Mainz',
                                               'University Medical Cent','Universitätsklinikum Mainz',
                                               'Universitätsmedizin, Mainz','University Hospital of Mainz',
                                               'UNIVERSITÄTSMEDIZIN Mainz','Helmholtz Institute']):
            affs.append(197323543)

    # Udulağ University
    if 'Bursa' in aff_string:
        if any(word in aff_string for word in ['Udulağ University','Uludag University']):
            affs.append(131835042)

    # Yokohama City University
    if 'Yokohama City University' in aff_string:
        affs.append(89630735)

    # Tampere University
    if 'Tampere' in aff_string:
        if any(word in aff_string for word in ['Tampere University of Technology',
                                               'Tampere university of technology','University of Tampere',
                                               'Tampere Univ. of Technology','Tampere University Hospital']):
            affs.append(4210133110)

    # National Technical University of Athens
    if any(word in aff_string for word in ['Athens','Greece']):
        if re.search('\\bNTUA\\b', aff_string):
            affs.append(174458059)

    # University of Trento
    if 'Trento' in aff_string:
        if any(word in aff_string for word in ['Università di Trento','University of Trento']):
            affs.append(193223587)

    # University of Siegen
    if 'Siegen' in aff_string:
        if any(word in aff_string for word in ['Siegen University','University Siegen','Universitat Siegen',
                                               'Universität Siegen','Universitaet Siegen']):
            affs.append(206895457)

    # University of Ulster
    if 'Ulster' in aff_string:
        if any(word in aff_string for word in ['Ulster Univ','University of Ulster','University Jordanstown']):
            affs.append(138801177)

    # Tunis El Manar University
    if 'Tunis' in aff_string:
        if all(word in aff_string for word in ['Univer','Tunis','Manar']):
            if any(word in aff_string for word in ['el','El']):
                affs.append(63596082)
        elif 'universi' in aff_string:
            if any(word in aff_string for word in ['El Manar','El-Manar','El Manar',
                                                   'el Manar','el-Manar','el Manar',
                                                   'El manar','El-manar','El manar',
                                                   'el manar','el-manar','el manar']):
                affs.append(63596082)
        elif any(word in aff_string for word in ['Université de Tunis EL Manar','université Tunis Manar',
                                                 'Université Tunis EL Manar',
                                                 'University of Tunis EL Manar','University of Tunis Manar']):
            affs.append(63596082)

    # Génomique fonctionnelle métabolique (epi) et mécanismes moléculaires impliqués
    if re.search('\bGI3M\b', aff_string):
        affs.append(4387156116)

    # Acteurs, Ressources et Territoires dans le Développement
    if re.search('\bART-Dev\b', aff_string):
        affs.append(4210097770)
    elif re.search('\bARTDev\b', aff_string):
        affs.append(4210097770)

    # AERIS/ICARE Data and Services Center
    if re.search('\bICARE\b', aff_string):
        affs.append(4387155909)

    # Agropolymerpolymer Engineering and Emerging Technologies
    if re.search('\bIATE\b', aff_string):
        affs.append(4210088087)

    # Aliments Bioprocédés Toxicologie Environnements
    if re.search('\bABTE\b', aff_string):
        affs.append(4387156208)

    # AMURE - Centre de droit et d'économie de la mer
    if re.search('\bAMURE\b', aff_string):
        affs.append(4387156023)

    # Analyses Littéraires et Histoire de la Langue
    if re.search('\bALITHILA\b', aff_string):
        affs.append(4387152964)

    # Anthropology and History of the Ancient World
    if re.search('\bANHIMA\b', aff_string):
        affs.append(4210140785)

    # Astrophysique Relativiste, Théories, Expériences, Métrologie, Instrumentation, Signaux
    if re.search('\bARTEMIS\b', aff_string):
        affs.append(4210124832)

    # Bases, Corpus, Langage
    if re.search('\bBCL\b', aff_string):
        affs.append(4210139825)

    # Biochemistry and Plant Molecular Physiology
    if re.search('\bBPMP\b', aff_string):
        affs.append(4210165050)

    # Brain and Cognition Research Center
    if re.search('\bCERCO\b', aff_string):
        affs.append(4210145991)

    # CALMIP
    if re.search('\bCALMIP\b', aff_string):
        affs.append(4387153662)

    # CANTHER - Hétérogénéité, Plasticité et Résistance aux Thérapies des Cancers
    if re.search('\bCANTHER\b', aff_string):
        affs.append(4387152542)

    # Center for Research in Medicine, Science, Health, Mental Health and Society
    if re.search('\bcermes3\b', aff_string):
        affs.append(4210132422)

    # Center for Social Studies on African, American and Asian Worlds
    if re.search('\bCESSMA\b', aff_string):
        affs.append(4210137420)

    # Centre Atlantique de Philosophie
    if re.search('\bCAPHI\b', aff_string):
        affs.append(4387152714)

    # Centre d'Etude des Arts Contemporains
    if re.search('\bCEAC\b', aff_string):
        affs.append(4387154796)

    # Centre d'Étude et de Recherche Travail Organisation Pouvoir
    if re.search('\bCERTOP\b', aff_string):
        affs.append(4210130108)

    # Centre d'Etudes des Maladies Infectieuses et Pharmacologie Anti-Infectieuse
    if re.search('\bCEMIPAI\b', aff_string):
        affs.append(4387156441)

    # Centre d'Etudes en Civilisations, Langues et Littératures Etrangères
    if re.search('\bCECILLE\b', aff_string):
        affs.append(4210123514)

    # Centre d'Études et de Recherches sur le Développement International
    if re.search('\bCERDI\b', aff_string):
        affs.append(4387153954)

    # Centre d'Études Spatiales de la Biosphère
    if re.search('\bCESBIO\b', aff_string):
        affs.append(4210100083)

    # Centre de Compétences NanoSciences Ile-de-France
    if re.search("\bC'Nano IdF\b", aff_string):
        affs.append(4210114404)

    # Centre de la Méditerranée Moderne et Contemporaine
    if re.search('\bCMMC\b', aff_string):
        affs.append(4210126072)

    # Centre de Linguistique Inter-langues, de Lexicologie, de Linguistique Anglaise et de Corpus-Atelier de Recherche sur la Parole
    if re.search('\bCLILLAC-ARP\b', aff_string):
        affs.append(4210113454)

    # Centre de Recherche "Individus, Epreuves, Sociétés"
    if re.search('\bCeRIES\b', aff_string):
        affs.append(4387154329)

    # Centre de Recherche en Informatique, Signal et Automatique de Lille
    if re.search('\bCRIStAL\b', aff_string):
        affs.append(4387153239)

    # Centre de recherche sur l’éducation, les apprentissages et la didactique
    if re.search('\bCREAD\b', aff_string):
        affs.append(4387155070)

    # Centre de recherche sur les civilisations de l'Asie orientale
    if re.search('\bCRCAO\b', aff_string):
        affs.append(4387154843)

    # Centre de Recherche sur les Liens Sociaux
    if re.search('\bCERLIS\b', aff_string):
        affs.append(4210110823)

    # Centre de Recherches Sociologiques sur le Droit et les Institutions Pénales
    if re.search('\bCESDIP\b', aff_string):
        affs.append(4210109827)

    # Centre de Recherches sur les Fonctionnements et Dysfonctionnements Psychologiques
    if re.search('\bCRFDP\b', aff_string):
        affs.append(4210136405)

    # Centre for Research in Epidemiology and Population Health
    if re.search('\bCESP\b', aff_string):
        affs.append(4210103698)

    # Centre Hospitalier Universitaire de Nice
    if re.search('\bCHU de Nice\b', aff_string):
        affs.append(3018988418)

    # Centre Hospitalier Universitaire de Nice
    if re.search('\bCHU Nice\b', aff_string):
        affs.append(3018988418)

    # Centre Interuniversitaire de Recherche en Education de Lille
    if re.search('\bCIREL\b', aff_string):
        affs.append(4387155238)

    # Centre Lillois d'Etudes et de Recherches Sociologiques et Economiques
    if re.search('\bCLERSÉ\b', aff_string):
        affs.append(4210095576)
    elif re.search('\bCLERSE\b', aff_string):
        affs.append(4210095576)

    # Centre Méditerranéen de l’Environnement et de la Biodiversité
    if re.search('\bCeMEB\b', aff_string):
        affs.append(4387154032)

    # Centre Méditerranéen de Médecine Moléculaire
    if re.search('\bC3M\b', aff_string):
        affs.append(4210118704)

    # Centre National de Création Musicale
    if re.search('\bCNCM\b', aff_string):
        affs.append(4387155025)

    # Clermont Research Management
    if re.search('\bCleRMa\b', aff_string):
        affs.append(4210094644)

    # Cognition Behaviour Technology
    if re.search('\bCoBTeK\b', aff_string):
        affs.append(4210144177)

    # Complexe de Recherche Interprofessionnel en Aérothermochimie
    if re.search('\bCORIA\b', aff_string):
        affs.append(4210104963)

    # Cultures et Environnements. Préhistoire, Antiquité, Moyen Âge
    if re.search('\bCEPAM\b', aff_string):
        affs.append(4210141055)

    # Digestive Health Research Institute
    if re.search('\bIRSD\b', aff_string):
        affs.append(4210122796)

    # Diversité, adaptation, développement des plantes
    if re.search('\bDIADE\b', aff_string):
        affs.append(4387156163)

    # Diversity Adaptation plant Development
    if re.search('\bDIADE\b', aff_string):
        affs.append(4210117822)

    # Diversity, Genomes and Insects-Microorganisms Interactions
    if re.search('\bDGIMI\b', aff_string):
        affs.append(4210131987)

    # Dynamique Musculaire et Métabolisme
    if re.search('\bDMEM\b', aff_string):
        affs.append(4387153819)

    # Ecology and Conservation Science for Sustainable Seas
    if re.search('\bECOMERS\b', aff_string):
        affs.append(4387156187)
    elif re.search('\bECOSEAS\b', aff_string):
        affs.append(4387156187)

    # Ecosystèmes, Biodiversité, Evolution
    if re.search('\bECOBIO\b', aff_string):
        affs.append(4210087209)

    # Empenn
    if re.search('\bVISAGES\b', aff_string):
        affs.append(4387152452)

    # Épidémiologie Clinique, Évaluation Économique Appliquées aux Populations Vulnérables
    if re.search('\bECEVE\b', aff_string):
        affs.append(4210110362)

    # Ethologie animale et humaine
    if re.search('\bETHoS\b', aff_string):
        affs.append(4387154707)

    # Étude des Structures, des Processus d’Adaptation et des Changements de l’Espace
    if re.search('\bESPACE\b', aff_string):
        affs.append(4387156460)

    # European Institute for Marine Studies
    if re.search('\bIUEM\b', aff_string):
        affs.append(4210157108)

    # Évolution, Génomes, Comportement, Écologie
    if re.search('\bLEGS\b', aff_string):
        affs.append(4210100071)
    elif re.search('\bEGCE\b', aff_string):
        affs.append(4210100071)

    # Expression Génétique Microbienne
    if re.search('\bEGM\b', aff_string):
        affs.append(4210165307)

    # Facteurs de risque et déterminants moléculaires des maladies liées au vieillissement
    if re.search('\bRID-AGE\b', aff_string):
        affs.append(4387154478)

    # Fish Physiology and Genomics Institute
    if re.search('\bLPGP\b', aff_string):
        affs.append(4210141078)

    # Fonctions Optiques pour les Technologies de l’information
    if re.search('\bFOTON\b', aff_string):
        affs.append(4210138837)

    # Galaxies, Etoiles, Physique et Instrumentation
    if re.search('\bGEPI\b', aff_string):
        affs.append(4210103454)

    # Genetics, Diversity and Ecophysiology of Cereals
    if re.search('\bGDEC\b', aff_string):
        affs.append(4210138126)

    # Genetics, Functional Genomics and Biotechnology
    if re.search('\bGGB\b', aff_string):
        affs.append(4210165461)

    # Genetique Reproduction and Developpement
    if re.search('\bGReD\b', aff_string):
        affs.append(4210163188)

    # Group of Study of Condensed Matter
    if re.search('\bGEMaC\b', aff_string):
        affs.append(4210110683)

    # Groupe d'Etudes et de Recherche Interdisciplinaire en Information et Communication
    if re.search('\bGERIICO\b', aff_string):
        affs.append(4387156027)

    # Groupe de Recherche en Droit, Économie, Gestion
    if re.search('\bGREDEG\b', aff_string):
        affs.append(4210096615)

    # Groupe de Recherche sur les formes Injectables et les Technologies Associées
    if re.search('\bGRITA\b', aff_string):
        affs.append(4387154967)

    # Handicap Neuromusculaire Physiopathologie, Biothérapie et Pharmacologie Appliquées
    if re.search('\bEND-ICAP\b', aff_string):
        affs.append(4210129617)

    # Histoire, Archéologie et Littérature des Mondes Anciens
    if re.search('\bHALMA\b', aff_string):
        affs.append(4387155028)

    # Identités et Différenciation de l'Environnement des Espaces et des Sociétés
    if re.search('\bIDEES\b', aff_string):
        affs.append(4210155900)

    # Imagerie Moléculaire et Stratégies Théranostiques
    if re.search('\bIMoST\b', aff_string):
        affs.append(4387155818)

    # IMPact de l'Environnement Chimique sur la Santé humaine
    if re.search('\bIMPECS\b', aff_string):
        affs.append(4387154702)

    # Infection, Antimicrobials, Modelling, Evolution
    if re.search('\bIAME\b', aff_string):
        affs.append(4387156044)

    # Innovations Thérapeutiques en Hémostase
    if re.search('\bIThEM\b', aff_string):
        affs.append(4387152685)

    # Institut Charles Gerhardt
    if re.search('\bICGM\b', aff_string):
        affs.append(4210115639)

    # Institut d'Histoire des Représentations et des Idées dans les Modernités
    if re.search('\bIHRIM\b', aff_string):
        affs.append(4210120985)

    # Institut de Génétique Moléculaire de Montpellier
    if re.search('\bIGMM\b', aff_string):
        affs.append(4210114166)

    # Institut de Mathématique et de Modélisation de Montpellier
    if re.search('\bI3M\b', aff_string):
        affs.append(4210142014)

    # Institut de Mathématiques de Jussieu
    if re.search('\bIMJ-PRG\b', aff_string):
        affs.append(3017942884)

    # Institut de Mécanique Céleste et de Calcul des Éphémérides
    if re.search('\bIMCCE\b', aff_string):
        affs.append(54006703)

    # Institut de Pharmacologie Moléculaire et Cellulaire
    if re.search('\bIPMC\b', aff_string):
        affs.append(4210160500)

    # Institut de Physique de Nice
    if re.search('\bINPHYNI\b', aff_string):
        affs.append(4210149294)

    # Institut de Recherche bio-Médicale et d'Epidémiologie du Sport
    if re.search('\bIRMES\b', aff_string):
        affs.append(4387155426)

    # Institut de Recherche Dupuy de Lôme
    if re.search('\bIRDL\b', aff_string):
        affs.append(4210126368)

    # Institut de Recherche en Infectiologie de Montpellier
    if re.search('\bIRIM\b', aff_string):
        affs.append(4210140640)

    # Institut de Recherche en Informatique et Systèmes Aléatoires
    if re.search('\bIRISA\b', aff_string):
        affs.append(2802519937)

    # Institut de Recherche en Santé, Environnement et Travail
    if re.search('\bIRSET\b', aff_string):
        affs.append(4210108239)

    # Institut de recherche mathématique de Rennes
    if re.search('\bIRMAR\b', aff_string):
        affs.append(4210161663)

    # Institut de Recherche sur les Composants logiciels et matériels pour l'Information et la Communication Avancée
    if re.search('\bIRCICA\b', aff_string):
        affs.append(4387153055)

    # Institut de Recherches Historiques du Septentrion
    if re.search('\bIRHiS\b', aff_string):
        affs.append(4210115252)

    # Institut des Biomolécules Max Mousseron
    if re.search('\bIBMM\b', aff_string):
        affs.append(4210145258)

    # Institut des Sciences Chimiques de Rennes
    if re.search('\bISCR\b', aff_string):
        affs.append(4210090783)

    # Institut des Sciences de l'Evolution de Montpellier
    if re.search('\bISEM\b', aff_string):
        affs.append(4210105943)

    # Institut des Sciences des Plantes de Paris Saclay
    if re.search('\bIPS2\b', aff_string):
        affs.append(4210090571)

    # Institut du droit public et de la science politique
    if re.search('\bIDPSP\b', aff_string):
        affs.append(4387154572)

    # Institut Lavoisier de Versailles
    if re.search('\bILV\b', aff_string):
        affs.append(4210165330)

    # Institut Necker Enfants Malades
    if re.search('\bINEM\b', aff_string):
        affs.append(4210086369)

    # Institute for Genetics, Environment and Plant Protection
    if re.search('\bIGEPP\b', aff_string):
        affs.append(4210141755)

    # Institute for Regenerative Medicine & Biotherapy
    if re.search('\bIRMB\b', aff_string):
        affs.append(4210095750)

    # Institute for the Separation Chemistry in Marcoule
    if re.search('\bICSM\b', aff_string):
        affs.append(4210147247)

    # Institute of Cancer Research of Montpellier
    if re.search('\bIRCM\b', aff_string):
        affs.append(4210140335)

    # Institute of Chemistry of Clermont-Ferrand
    if re.search('\bICCF\b', aff_string):
        affs.append(4210133183)

    # Institute of Electronics and Telecommunications of Rennes
    if re.search('\bIETR\b', aff_string):
        affs.append(4210100151)

    # Institute of Electronics, Microelectronics and Nanotechnology
    if re.search('\bIEMN\b', aff_string):
        affs.append(4210123471)

    # Institute of Fluid Mechanics of Toulouse
    if re.search('\bIMFT\b', aff_string):
        affs.append(4210110935)

    # Institute of Genetics and Development of Rennes
    if re.search('\bIGDR\b', aff_string):
        affs.append(4210127029)

    # Institute of Molecular Chemistry Reims
    if re.search('\bICMR\b', aff_string):
        affs.append(4210131305)

    # Institute of Pharmacology and Structural Biology
    if re.search('\bIPBS\b', aff_string):
        affs.append(4210099749)

    # Institute of Psychiatry and Neuroscience of Paris
    if re.search('\bIPNP\b', aff_string):
        affs.append(4210130152)

    # Institute of Research on Cancer and Aging in Nice
    if re.search('\bIRCAN\b', aff_string):
        affs.append(4210119200)

    # Integrative Neuroscience and Cognition Center
    if re.search('\bINCC\b', aff_string):
        affs.append(4387154659)

    # Integrative Physics and Physiology of Fruit and Forest Trees
    if re.search('\bPIAF\b', aff_string):
        affs.append(4210133230)

    # Interactions Hôtes-Pathogènes-Environnements
    if re.search('\bIHPE\b', aff_string):
        affs.append(4387154330)

    # Interfaces Traitements Organisation et Dynamique des Systèmes
    if re.search('\bITODYS\b', aff_string):
        affs.append(4210124942)

    # Laboratoire Chimie Electrochimie Moléculaires et Chimie Analytique
    if re.search('\bCEMCA\b', aff_string):
        affs.append(4210118725)

    # Laboratoire d'Anthropologie et de Psychologie Cognitives et Sociales
    if re.search('\bLAPCOS\b', aff_string):
        affs.append(4210114701)

    # Laboratoire d'Automatique, Génie Informatique et Signal
    if re.search('\bLAGIS\b', aff_string):
        affs.append(4210143894)

    # Laboratoire d'Écophysiologie Moléculaire des Plantes sous Stress Environnementaux
    if re.search('\bLEPSE\b', aff_string):
        affs.append(4210131186)

    # Laboratoire d'Électronique, Antennes et Télécommunications
    if re.search('\bLEAT\b', aff_string):
        affs.append(4210095736)

    # Laboratoire d'Électrotechnique et d'Électronique de Puissance de Lille
    if re.search('\bL2EP\b', aff_string):
        affs.append(4210102085)

    # Laboratoire d'études et de recherche en sociologie
    if re.search('\bLABERS\b', aff_string):
        affs.append(4387152360)

    # Laboratoire d'Études et de Recherches Appliquées en Sciences Sociales
    if re.search('\bLERASS\b', aff_string):
        affs.append(4387154506)

    # Laboratoire d'Informatique, Signaux et Systèmes de Sophia Antipolis
    if re.search('\bI3S\b', aff_string):
        affs.append(4210106479)

    # Laboratoire d’Économie et de Gestion de l'Ouest
    if re.search('\bLEGO\b', aff_string):
        affs.append(4387154291)

    # Laboratoire d’Étude et de Recherche sur l’Économie, les Politiques et les Systèmes Sociaux
    if re.search('\bLEREPS\b', aff_string):
        affs.append(4210155567)

    # Laboratoire d’Études en Géophysique et Océanographie Spatiales
    if re.search('\bLEGOS\b', aff_string):
        affs.append(4210112630)

    # Osteo-Articular Bioengineering and Bioimaging
    if re.search('\bB2OA\b', aff_string):
        affs.append(4210137432)
    elif re.search('\bB3OA\b', aff_string):
        affs.append(4210137432)

    # Laboratoire de Chimie et Biochimie Pharmacologiques et Toxicologiques
    if re.search('\bLCBPT\b', aff_string):
        affs.append(4210117082)

    # Laboratoire de Chimie et Physique Quantiques
    if re.search('\bLCPQ\b', aff_string):
        affs.append(4210144120)

    # Laboratoire de Génétique & Evolution des Populations Végétales
    if re.search('\bGEPV\b', aff_string):
        affs.append(4210104410)

    # Laboratoire de génie civil et génie mécanique
    if re.search('\bLGCGM\b', aff_string):
        affs.append(4387155956)

    # Laboratoire de Génie Civil et Géo Environnement
    if re.search('\bLGCgE\b', aff_string):
        affs.append(4387153130)

    # Laboratoire de Géographie Physique et Environnementale
    if re.search('\bGEOLAB\b', aff_string):
        affs.append(4210092141)

    # Laboratoire de Mathématiques de Bretagne Atlantique
    if re.search('\bLMBA\b', aff_string):
        affs.append(4210119023)

    # Laboratoire de Mathématiques Raphaël Salem
    if re.search('\bLMRS\b', aff_string):
        affs.append(4210105181)

    # Laboratoire de Mécanique des Fluides de Lille - Kampé de Fériet
    if re.search('\bLCFC\b', aff_string):
        affs.append(4210123886)

    # Laboratoire de Mécanique et Génie Civil
    if re.search('\bLMGC\b', aff_string):
        affs.append(4210115072)

    # Laboratoire de Mécanique, Multiphysique, Multiéchelle
    if re.search('\bLaMCUBE\b', aff_string):
        affs.append(4387155011)

    # Laboratoire de Microbiologie et Génétique Moléculaires
    if re.search('\bLMGM\b', aff_string):
        affs.append(4210149702)

    # Laboratoire de PhysioMédecine Moléculaire
    if re.search('\bLP2M\b', aff_string):
        affs.append(4210091024)

    # Laboratoire de Physique des Lasers, Atomes et Molécules
    if re.search('\bPhLAM\b', aff_string):
        affs.append(4210160651)

    # Laboratoire de Physique Nucléaire et de Hautes Énergies
    if re.search('\bLPNHE\b', aff_string):
        affs.append(4210105151)

    # Laboratoire de Probabilités, Statistique et Modélisation
    if re.search('\bLPSM\b', aff_string):
        affs.append(4387155306)

    # Laboratoire de Psychologie : Cognition, Comportement, Communication
    if re.search('\bLP3C\b', aff_string):
        affs.append(4210132724)

    # Laboratoire de Psychologie Sociale et Cognitive
    if re.search('\bLAPSCO\b', aff_string):
        affs.append(4210116526)

    # Laboratoire de recherche en droit
    if re.search('\bLab-LEX\b', aff_string):
        affs.append(4387153921)

    # Laboratoire de Recherche en Sciences Végétales
    if re.search('\bLRSV\b', aff_string):
        affs.append(4210140000)

    # Laboratoire de Recherche sur les Cultures Anglophones
    if re.search('\bLARCA\b', aff_string):
        affs.append(4387155640)

    # Laboratoire de Sécurité des Procédés Chimiques
    if re.search('\bLSPC\b', aff_string):
        affs.append(4387153716)

    # Laboratoire de Spectrochimie Infrarouge et Raman
    if re.search('\bLASIR\b', aff_string):
        affs.append(4210107855)

    # Laboratoire des 2 Infinis Toulouse
    if re.search('\bL2IT\b', aff_string):
        affs.append(4387153973)

    # Laboratoire des Interactions Moléculaires et Réactivité Chimique et Photochimique
    if re.search('\bIMRCP\b', aff_string):
        affs.append(4210101257)

    # Laboratoire des Interactions Plantes Micro-Organismes
    if re.search('\bLIPM\b', aff_string):
        affs.append(4210115873)

    # Laboratoire des Sciences de l'Environnement Marin
    if re.search('\bLEMAR\b', aff_string):
        affs.append(4210162872)

    # Laboratoire des Sciences du Climat et de l'Environnement
    if re.search('\bLCSE\b', aff_string):
        affs.append(4210124937)

    # Laboratoire des Sciences et Techniques de l’Information de la Communication et de la Connaissance
    if re.search('\bLAB-STICC\b', aff_string):
        affs.append(4210123702)

    # Laboratoire Dynamiques Sociales et Recomposition des Espaces
    if re.search('\bLADYSS\b', aff_string):
        affs.append(4210141654)

    # Laboratoire Génie et Matériaux Textiles
    if re.search('\bGEMTEX\b', aff_string):
        affs.append(4210132107)

    # Laboratoire Hétérochimie Fondamentale et Appliquée
    if re.search('\bLHFA\b', aff_string):
        affs.append(4210135875)

    # Laboratoire interdisciplinaire de recherche en didactique, éducation et formation
    if re.search('\bLIRDEF\b', aff_string):
        affs.append(4387152446)

    # Laboratoire Interdisciplinaire des Énergies de Demain
    if re.search('\bLIED\b', aff_string):
        affs.append(4210094488)

    # Laboratoire Jacques-Louis Lions
    if re.search('\bLJLL\b', aff_string):
        affs.append(4210158291)

    # Laboratoire Magmas et Volcans
    if re.search('\bLMV\b', aff_string):
        affs.append(4210125915)

    # Laboratoire Microorganismes Génome et Environnement
    if re.search('\bLMGE\b', aff_string):
        affs.append(4210122170)

    # Laboratoire Motricité Humaine Éducation Sport Santé
    if re.search('\bLAMHESS\b', aff_string):
        affs.append(4210137748)

    # French National High Magnetic Field Laboratory
    if re.search('\bLNCMI\b', aff_string):
        affs.append(3170133708)

    # Laboratoire Traitement du Signal et de l'Image
    if re.search('\bLTSI\b', aff_string):
        affs.append(4210105651)

    # Laboratoire Univers et Particules de Montpellier
    if re.search('\bLUPM\b', aff_string):
        affs.append(4210095986)

    # Laboratory for Ocean Physics and Satellite Remote Sensing
    if re.search('\bLOPS\b', aff_string):
        affs.append(4210134272)

    # Laboratory for the Psychology of Child Development and Education
    if re.search('\bLaPsyDÉ\b', aff_string):
        affs.append(4210111983)
    elif re.search('\bLaPsyDE\b', aff_string):
        affs.append(4210111983)

    # Laboratory for Vascular Translational Science
    if re.search('\bLVTS\b', aff_string):
        affs.append(4210134185)

    # Laboratory of Computing, Modelling and Optimization of the Systems
    if re.search('\bLIMOS\b', aff_string):
        affs.append(4210099416)

    # Laboratory of Molecular Anthropology and Image Synthesis
    if re.search('\bAMIS\b', aff_string):
        affs.append(4210159772)

    # Laboratory of Physical and Chemical Biology of Membrane Proteins
    if re.search('\bLBPCPM\b', aff_string):
        affs.append(4210128656)

    # Laboratory of Space Studies and Instrumentation in Astrophysics
    if re.search('\bLESIA\b', aff_string):
        affs.append(4210120578)

    # Laboratory Universe and Theories
    if re.search('\bLUTH\b', aff_string):
        affs.append(4210089183)

    # LACTH - Laboratoire d'Architecture Conception Territoire Histoire Matérialité
    if re.search('\bLACTH\b', aff_string):
        affs.append(4387155060)

    # Lille Center for European Research on Administration, Politics and Society
    if re.search('\bCERAPS\b', aff_string):
        affs.append(4210144087)

    # Lille Inflammation Research International Center
    if re.search('\bLIRIC\b', aff_string):
        affs.append(4210128436)

    # Lille Neurosciences & Cognition
    if re.search('\bLilNCog\b', aff_string):
        affs.append(4387155073)

    # Lille School of Management Research Center
    if re.search('\bLSMRC\b', aff_string):
        affs.append(4387154787)

    # Lille University Management
    if re.search('\bLUMEN\b', aff_string):
        affs.append(4387156340)

    # Maison de la Simulation
    if re.search('\bMdlS\b', aff_string):
        affs.append(4210125654)

    # Maison Européenne des Sciences de l'Homme et de la Société
    if re.search('\bMESHS\b', aff_string):
        affs.append(4210093229)

    # Maladies Infectieuses et Vecteurs: Écologie, Génétique, Évolution et Contrôle
    if re.search('\bMIVEGEC\b', aff_string):
        affs.append(4210087127)

    # Marine Biodiversity Exploitation and Conservation
    if re.search('\bMARBEC\b', aff_string):
        affs.append(4210149887)

    # Marrow Adiposity & Bone Lab
    if re.search('\bMABLAB\b', aff_string):
        affs.append(4387154812)

    # Mathématiques Appliquées à Paris 5
    if re.search('\bMAP5\b', aff_string):
        affs.append(4387154995)

    # Matrice Extracellulaire et Dynamique Cellulaire MEDyC
    if re.search('\bMEDyC\b', aff_string):
        affs.append(4387155780)

    # Mécanismes moléculaires dans les démences neurodégénératives
    if re.search('\bMMDN\b', aff_string):
        affs.append(4387152481)

    # Médicaments et Molécules pour Agir sur les Systèmes Vivants
    if re.search('\bM2SV\b', aff_string):
        affs.append(4387154141)

    # Mère et Enfant en Milieu Tropical
    if re.search('\bMERIT\b', aff_string):
        affs.append(4210093064)

    # Microbe, Intestine, Inflammation and Host Susceptibility
    if re.search('\bM2iSH\b', aff_string):
        affs.append(4210123714)

    # Microbiologie Environnement Digestif Santé
    if re.search('\bMEDIS\b', aff_string):
        affs.append(4210133202)

    # Microenvironment and B-cells: Immunopathology, Cell, Differentiation and Cancer
    if re.search('\bMOBIDIC\b', aff_string):
        affs.append(4387154398)

    # Miniaturisation pour la Synthèse, l'Analyse et la Protéomique
    if re.search('\bMSAP\b', aff_string):
        affs.append(4210123347)

    # Molecular and Atmospheric Spectrometry Group
    if re.search('\bGSMA\b', aff_string):
        affs.append(4210130789)

    # Montpellier Laboratory of Informatics, Robotics and Microelectronics
    if re.search('\bLIRMM\b', aff_string):
        affs.append(4210101743)

    # Movement, Sport and health Sciences Laboratory
    if re.search('\bM2S\b', aff_string):
        affs.append(4210160484)

    # Normandie Innovation Marché Entreprise Consommation
    if re.search('\bNIMEC\b', aff_string):
        affs.append(4387153363)

    # Nutrition, métabolismes et cancer
    if re.search('\bNUMECAN\b', aff_string):
        affs.append(4387156410)

    # Observatoire de Physique du Globe de Clermont-Ferrand
    if re.search('\bOPGC\b', aff_string):
        affs.append(4210111874)

    # Observatoire des Sciences de l'Univers de Rennes
    if re.search('\bOSUR\b', aff_string):
        affs.append(4387156395)

    # Observatoire des Sciences de l'Univers OREME
    if re.search('\bOREME\b', aff_string):
        affs.append(4387155600)

    # Optimisation Thérapeutique en Neuropsychopharmacologie
    if re.search('\bVARIAPSY\b', aff_string):
        affs.append(4387154652)

    # Organic and Analytical Chemistry Laboratory
    if re.search('\bCOBRA\b', aff_string):
        affs.append(4210152404)

    # Paris Cardiovascular Research Center
    if re.search('\bPARCC\b', aff_string):
        affs.append(4210131199)

    # Pathologies Pulmonaires et Plasticité Cellulaire
    if re.search('\bP3CELL\b', aff_string):
        affs.append(4387154171)

    # Pharmacochimie et Pharmacologie pour le Développement
    if re.search('\bPHARMA-DEV\b', aff_string):
        affs.append(4210140430)

    # PhysicoChimie des Processus de Combustion et de l'Atmosphère
    if re.search('\bPC2A\b', aff_string):
        affs.append(4210139807)

    # Physiology & Experimental Medicine of the Heart and Muscles
    if re.search('\bPHYMEDEXP\b', aff_string):
        affs.append(4210086516)

    # Physique et Mécanique des Milieux Hétérogènes
    if re.search('\bPMMH\b', aff_string):
        affs.append(4210133938)

    # Plateformes Lilloises en Biologie et Santé
    if re.search('\bPLBS\b', aff_string):
        affs.append(4387153745)

    # Pôle de Recherche pour l'Organisation et la Diffusion de l'Information Géographique
    if re.search('\bPRODIG\b', aff_string):
        affs.append(4210087727)

    # Population and Development Center
    if re.search('\bCEPED\b', aff_string):
        affs.append(4210091642)

    # Protéomique, Réponse Inflammatoire et Spectrométrie de Masse
    if re.search('\bPRISM\b', aff_string):
        affs.append(4210164350)

    # Psychologie : Interactions, Temps, Emotions, Cognition
    if re.search('\bPSITEC\b', aff_string):
        affs.append(4387154736)

    # Recherches Translationnelles sur le VIH et les Maladies Infectieuses
    if re.search('\bTransVIHMI\b', aff_string):
        affs.append(4387153971)

    # Research Institute in Astrophysics and Planetology
    if re.search('\bIRAP\b', aff_string):
        affs.append(4210165452)

    # Research Institute on the Foundations of Computer Science
    if re.search('\bIRIF\b', aff_string):
        affs.append(4210117673)

    # Réseau interdisciplinaire pour l’aménagement, l’observation et la cohésion des territoires européens
    if re.search('\bRIATE\b', aff_string):
        affs.append(4387153667)

    # Sciences Cognitives et Sciences Affectives
    if re.search('\bSCALAB\b', aff_string):
        affs.append(4210166223)

    # Sciences, Philosophie, Histoire
    if re.search('\bSPHERE\b', aff_string):
        affs.append(4210150713)

    # South European Center for Political Studies
    if re.search('\bCEPEL\b', aff_string):
        affs.append(4210126751)

    # SPPIN - Saints-Pères Paris Institute for Neurosciences
    if re.search('\bSPPIN\b', aff_string):
        affs.append(4387154016)

    # Stabilité Génétique, Cellules Souches et Radiations
    if re.search('\bSGCSR\b', aff_string):
        affs.append(4387152861)

    # STIC Research Centre
    if re.search('\bCReSTIC\b', aff_string):
        affs.append(4210100943)

    # Stress Environnementaux et Biosurveillance des Milieux Aquatiques
    if re.search('\bSEBIO\b', aff_string):
        affs.append(4210142992)

    # Systèmes avancés de délivrance de principes actifs
    if re.search('\bADDS\b', aff_string):
        affs.append(4387155654)

    # Territoires, Villes, Environnement & Société
    if re.search('\bTVES\b', aff_string):
        affs.append(4210154078)

    # Toxalim Research Centre in Food Toxicology
    if re.search('\bTOXALIM\b', aff_string):
        affs.append(4210100066)

    # Transporteurs, Imagerie et Radiothérapie en Oncologie - Mécanismes Biologiques des Altérations du Tissu Osseux
    if re.search('\bTIRO-MATO\b', aff_string):
        affs.append(4387152173)

    # Unité de Glycobiologie Structurale et Fonctionnelle
    if re.search('\bUGSF\b', aff_string):
        affs.append(4210118174)

    # Unité de Recherche en Biomatériaux Innovant et Interfaces
    if re.search('\bURB2i\b', aff_string):
        affs.append(4387155632)

    # Unite de recherche migrations et sociétés
    if re.search('\bURMIS\b', aff_string):
        affs.append(4387154975)

    # Unité de Recherche Pluridisciplinaire Sport, Santé, Société
    if re.search('\bURePSSS\b', aff_string):
        affs.append(4210136412)

    # Unité de Taphonomie Médico-Légale
    if re.search('\bUTML&A\b', aff_string):
        affs.append(4387153599)

    # Unité de Technologies Chimiques et Biologiques pour la Santé
    if re.search('\bUTCBS\b', aff_string):
        affs.append(4210139070)

    # Virologie et Immunologie Moléculaires
    if re.search('\bVIM\b', aff_string):
        affs.append(4210114484)

    # Western Institute of Law and Europe
    if re.search('\bIODE\b', aff_string):
        affs.append(4210128017)

    if 'Aignan' in aff_string:
        # Polymères, Biopolymères, Surfaces
        if re.search('\bPBS\b', aff_string):
            affs.append(4210140452)

    if 'Aubière' in aff_string:
        # Laboratoire de Météorologie Physique
        if re.search('\bLAMP\b', aff_string):
            affs.append(4210133081)

    if 'Boulogne' in aff_string:
        # Laboratoire Vision Action Cognition
        if re.search('\bVAC\b', aff_string):
            affs.append(4210135853)

        # Memory and Cognition Laboratory
        if re.search('\bLMC\b', aff_string):
            affs.append(4210124483)

    if 'Brest' in aff_string:
        # Centre de recherche bretonne et celtique
        if re.search('\bCRBC\b', aff_string):
            affs.append(4387155747)

    if 'Caen' in aff_string:
        # Laboratoire Morphodynamique Continentale et Côtière
        if re.search('\bM2C\b', aff_string):
            affs.append(4387154573)

    if 'Castanet' in aff_string:
        # Laboratoire d'Excellence TULIP
        if re.search('\bTULIP\b', aff_string):
            affs.append(4387153282)

    if 'Cochin Pasteur' in aff_string:
        # Centre d'Investigation Clinique de Vaccinologie Cochin-Pasteur
        if re.search('\bCIC\b', aff_string):
            affs.append(4387156468)

    if 'Créteil' in aff_string:
        # Laboratoire Interuniversitaire des Systèmes Atmosphériques
        if re.search('\bLISA\b', aff_string):
            affs.append(4210135273)

    if 'Foix' in aff_string:
        # Station d’Écologie Théorique et Expérimentale
        if re.search('\bSETE\b', aff_string):
            affs.append(4210162824)

    if 'Gif-sur-Yvette' in aff_string:
        # Astrophysique, Instrumentation et Modélisation
        if re.search('\bAIM\b', aff_string):
            affs.append(4210086977)

    if 'Guyancourt' in aff_string:
        # Soutenabilité et Résilence
        if re.search('\bSOURCE\b', aff_string):
            affs.append(4387155194)

    if 'Lille' in aff_string:
        # Center for Infection and Immunity of Lille
        if re.search('\bCIIL\b', aff_string):
            affs.append(4210098529)

        # Centre d'Histoire Judiciaire
        if re.search('\bCHJ\b', aff_string):
            affs.append(4387154580)

        # Centre d'Investigation Clinique - Innovation Technologique de Lille
        if re.search('\bCIC\b', aff_string):
            affs.append(4387154933)

        # Centre de Recherche Droits et Perspectives du droit
        if re.search('\bCRDP\b', aff_string):
            affs.append(4387153790)

        # Evaluation des technologies de santé et des pratiques médicales
        if re.search('\bMETRICS\b', aff_string):
            affs.append(4387155751)

        # Institut de Biologie de Lille
        if re.search('\bIBL\b', aff_string):
            affs.append(4210163910)

        # Institute for Translational Research in Inflammation
        if re.search('\bINFINITE\b', aff_string):
            affs.append(4387152267)

        # Laboratory of Catalysis and Solid State Chemistry
        if re.search('\bUCCS\b', aff_string):
            affs.append(4210141930)

        # Lille Économie Management
        if re.search('\bLEM\b', aff_string):
            affs.append(4210159017)

        # Recherche translationnelle sur le diabète
        if re.search('\bTRD\b', aff_string):
            affs.append(4387154483)

        # Unité de Mécanique de Lille - Joseph Boussinesq
        if re.search('\bUML\b', aff_string):
            affs.append(4387155272)

    if 'Montpellier' in aff_string:
        # Botany and Modelling of Plant Architecture and Vegetation
        if re.search('\bAMAP\b', aff_string):
            affs.append(4210121611)

        # Center for Environmental Economics - Montpellier
        if re.search('\bCEE-M\b', aff_string):
            affs.append(4387156380)
        elif re.search('\bCEEM\b', aff_string):
            affs.append(4387156380)

        # Centre d'Écologie Fonctionnelle et Évolutive
        if re.search('\bCEFE\b', aff_string):
            affs.append(4210089824)

        # Centre de Biochimie Structurale
        if re.search('\bCBS\b', aff_string):
            affs.append(4210100279)

        # Centre for Biochemical and Macromolecular Research
        if re.search('\bCRBM\b', aff_string):
            affs.append(4210113016)

        # Institut d'Électronique et des Systèmes
        if re.search('\bIES\b', aff_string):
            affs.append(4210134800)

        # Institut de Génomique Fonctionnelle
        if re.search('\bIGF\b', aff_string):
            affs.append(4210156758)

        # Institut Européen des Membranes
        if re.search('\bIEM\b', aff_string):
            affs.append(4210159287)

        # Institute of Human Genetics
        if re.search('\bIGH\b', aff_string):
            affs.append(4210163339)

        # Laboratoire Charles Coulomb
        if re.search('\bL2C\b', aff_string):
            affs.append(4210128986)

        # Laboratoire des Symbioses Tropicales et Méditerranéennes
        if re.search('\bLSTM\b', aff_string):
            affs.append(4210165061)

        # Laboratory HydroSciences Montpellier
        if re.search('\bHSM\b', aff_string):
            affs.append(3019667749)

        # Sciences pour L’Œnologie
        if re.search('\bSPO\b', aff_string):
            affs.append(4210147867)

    if 'Nantes' in aff_string:
        # Centre François Viète
        if re.search('\bCFV\b', aff_string):
            affs.append(4387153064)

    if 'Nice' in aff_string:
        # Institut de Chimie de Nice
        if re.search('\bICN\b', aff_string):
            affs.append(4210086528)

        # Institute of Biology Valrose
        if re.search('\bIBV\b', aff_string):
            affs.append(4210117840)

        # Maison des Sciences de l'Homme et de la Société Sud-Est
        if re.search('\bMSHS\b', aff_string):
            affs.append(4387153586)

        # Observatoire de la Côte d’Azur
        if re.search('\bOCA\b', aff_string):
            affs.append(4210126779)

    if 'Nîmes' in aff_string:
        # Bacterial Virulence and Chronic Infections
        if re.search('\bVBIC\b', aff_string):
            affs.append(4387154649)
        elif re.search('\bVBMI\b', aff_string):
            affs.append(4387154649)

    if 'Paris' in aff_string:
        # Astroparticle and Cosmology Laboratory
        if re.search('\bAPC\b', aff_string):
            affs.append(2802090066)

        # Centre de Recherche des Cordeliers
        if re.search('\bCRC\b', aff_string):
            affs.append(4210092322)

        # Epigenetics and Cell Fate
        if re.search('\bEDC\b', aff_string):
            affs.append(4210131858)

        # HIPI - Human Immunology, Pathophysiology and Immunotherapy
        if re.search('\bHIPI\b', aff_string):
            affs.append(4387154437)

        # Histoire des Théories Linguistiques
        if re.search('\bHTL\b', aff_string):
            affs.append(4387155844)

        # Institut Droit et Santé
        if re.search('\bIDS\b', aff_string):
            affs.append(4387152995)

        # Institut Jacques Monod
        if re.search('\bIJM\b', aff_string):
            affs.append(4210113761)

        # Institute of Ecology and Environmental Sciences Paris
        if re.search('\bIEES\b', aff_string):
            affs.append(4210134846)

        # International College of Territorial Sciences
        if re.search('\bCIST\b', aff_string):
            affs.append(4210088804)

        # Laboratoire d'Electrochimie Moléculaire
        if re.search('\bLEM\b', aff_string):
            affs.append(4210131283)

        # Laboratoire de Linguistique Formelle
        if re.search('\bLLF\b', aff_string):
            affs.append(4210114212)

        # Laboratoire ICT
        if re.search('\bICT\b', aff_string):
            affs.append(3018794593)

        # Laboratoire Matière et Systèmes Complexes
        if re.search('\bMSC\b', aff_string):
            affs.append(4210127056)

        # Laboratory Materials and Quantum Phenomena
        if re.search('\bMPQ\b', aff_string):
            affs.append(4210133036)

        # Laboratory of Theoretical Biochemistry
        if re.search('\bLBT\b', aff_string):
            affs.append(4210094297)

        # Laboratory Preuves, Programmes et Systèmes
        if re.search('\bPPS\b', aff_string):
            affs.append(4210139011)

        # Pathologie et Virologie Moléculaire
        if re.search('\bPVM\b', aff_string):
            affs.append(4210086835)

        # The Centre for Studies on China, Korea and Japan
        if re.search('\bCCJ\b', aff_string):
            affs.append(4210125567)

        # Unit of Functional and Adaptive Biology
        if re.search('\bBFA\b', aff_string):
            affs.append(4210137329)

    if 'Plouzané' in aff_string:
        # Geo-Ocean
        if re.search('\bLGO\b', aff_string):
            affs.append(4387153566)

    if 'Reims' in aff_string:
        # Fractionnation of AgroResources and Environment
        if re.search('\bFARE\b', aff_string):
            affs.append(4210086276)

        # Laboratoire de Mathématiques de Reims
        if re.search('\bLMR\b', aff_string):
            affs.append(4387156098)

    if 'Rennes' in aff_string:
        # Centre de droit des affaires
        if re.search('\bCDA\b', aff_string):
            affs.append(4387152641)

        # Centre de Recherche en Économie et Management
        if re.search('\bCREM\b', aff_string):
            affs.append(4210088544)

        # CIC Rennes
        if re.search('\bCIC\b', aff_string):
            affs.append(4210116274)

        # Institut de Physique de Rennes
        if re.search('\bIPR\b', aff_string):
            affs.append(4210109443)

        # Oncogenesis Stress Signaling
        if re.search('\bOSS\b', aff_string):
            affs.append(4210090689)

    if 'Rouen' in aff_string:
        # Nutrition, Inflammation et axe Microbiote-Intestin-Cerveau
        if re.search('\bADEN\b', aff_string):
            affs.append(4387154241)

    if 'Seine-Port' in aff_string:
        # Groupe de Physique des Matériaux
        if re.search('\bGPM\b', aff_string):
            affs.append(4210130800)

    if 'Tarbes' in aff_string:
        # Télescope Bernard Lyot
        if re.search('\bTBL\b', aff_string):
            affs.append(4387154879)

    if 'Toulouse' in aff_string:
        # Cancer Research Center of Toulouse
        if re.search('\bCRCT\b', aff_string):
            affs.append(4210087620)

        # Centre d’Investigation Clinique 1436
        if re.search('\bCIC\b', aff_string):
            affs.append(4387156050)

        # Géosciences Environnement Toulouse
        if re.search('\bGET\b', aff_string):
            affs.append(4210164289)

        # Institut Clément Ader
        if re.search('\bICA\b', aff_string):
            affs.append(4210130254)

        # Laboratoire de Génie Chimique
        if re.search('\bLGC\b', aff_string):
            affs.append(4210087602)

        # Laboratoire Epidémiologie et Analyses en Santé Publique : Risques, Maladies Chroniques et Handicaps
        if re.search('\bLEASP\b', aff_string):
            affs.append(4210127234)

        # Laboratory Evolution and Biological Diversity
        if re.search('\bEDB\b', aff_string):
            affs.append(4210122570)

        # Research Centre on Animal Cognition
        if re.search('\bCRCA\b', aff_string):
            affs.append(4210098684)

        # RESTORE
        if re.search('\bRESTORE\b', aff_string):
            affs.append(4387152741)

        # Toulouse Mathematics Institute
        if re.search('\bIMT\b', aff_string):
            affs.append(84500057)

    if 'Valbonne' in aff_string:
        # Institut Sophia Agrobiotech
        if re.search('\bISA\b', aff_string):
            affs.append(4210153141)

    if 'Versailles' in aff_string:
        # Laboratoire de Mathématiques de Versailles
        if re.search('\bLMV\b', aff_string):
            affs.append(4387152735)

    if 'Villeneuve' in aff_string:
        # Laboratoire d'Optique Atmosphérique
        if re.search('\bLOA\b', aff_string):
            affs.append(4210123210)

        # Laboratoire Paul Painlevé
        if re.search('\bLPP\b', aff_string):
            affs.append(4210145948)

        # Savoirs, Textes, Langage
        if re.search('\bSTL\b', aff_string):
            affs.append(4210087136)

        # Unité Matériaux et Transformations
        if re.search('\bUMET\b', aff_string):
            affs.append(4210143235)

    if 'Wimereux' in aff_string:
        # Laboratoire d’Océanologie et de Géosciences
        if re.search('\bLOG\b', aff_string):
            affs.append(4210161208)

    return list(set(affs))

#################################################################################################################################
#################################################################################################################################

def chinese_medical_university_affs(aff_string, current_affs, aff_id_1, one_off_affs, affs_to_add, 
                                    strings_for_affs):
    
    all_affs_to_search = [aff_id_1]+one_off_affs+affs_to_add
    
    if any(aff_id in current_affs for aff_id in all_affs_to_search):
        current_affs.append(aff_id_1)
        for aff_id_check, aff_string_check in zip(affs_to_add, strings_for_affs):
            if aff_id_check in current_affs:
                for aff_id_check_2, aff_string_check_2 in zip(affs_to_add, strings_for_affs):
                    if aff_id_check_2 == aff_id_check:
                        pass
                    else:
                        if aff_string_check_2 in aff_string:
                            if aff_id_check in current_affs:
                                current_affs.remove(aff_id_check)
                            if aff_id_check_2 not in current_affs:
                                current_affs.append(aff_id_check_2)
    return current_affs


#################################################################################################################################
#################################################################################################################################

def process_current_affiliation_with_ids(current_affs, aff_string):
    """
    Refines a list of institution IDs matched to an affiliation string
    by applying post-processing rules based on known disambiguation patterns.

    Parameters
    ----------
    current_affs : List[int]
        A list of initially matched institution IDs. May contain ambiguous
        placeholder values like -1.

    aff_string : str
        The original raw affiliation string used to provide context for
        resolving ambiguities.

    Returns
    -------
    List[int]
        A de-duplicated list of refined institution IDs.

    Examples
    --------
    >>> process_current_affiliation_with_ids([-1, 4210086685], "AP-HP Sorbonne University")
    [4210086685, 39804081]

    >>> process_current_affiliation_with_ids([88155538], "Universidad Pública de Navarra")
    [175051016]
    """

    if (len(current_affs) > 1) & (-1 in current_affs):
        current_affs.remove(-1)

    # Sorbonne Hospitals
    if any(inst in current_affs for inst in [4210086685,4210166768,4210134887,4210153132,4210090185,
                                             4210102928,4210121705]):
        current_affs.append(39804081)

    # International Council for the Exploration of the Sea
    if 282179226 in current_affs:
        if 'denmark' not in aff_string.lower():
            if 'sea' not in aff_string.lower():
                current_affs.remove(282179226)

    # Universidad Publica de Navarra vs Universidad de Navarra
    if 88155538 in current_affs:
        if any(word in aff_string.lower() for word in ['universidad publica','universidad pública']):
            current_affs.remove(88155538)
            current_affs.append(175051016)

    if 175051016 in current_affs:
        if any(word in aff_string.lower() for word in ['universidad de navarra']):
            current_affs.remove(175051016)
            current_affs.append(88155538)

    # University of Mannheim
    if 177802217 in current_affs:
        if any(word in aff_string.lower() for word in ['university hospital mannheim','university-hospital mannheim',
                                                       'mannheim university hospital']):
            current_affs.remove(177802217)
            if any(word in aff_string.lower() for word in ['university of heidelberg','heidelberg university','faculty of clinical medicine mannheim',
                                                           'universität heidelberg','universitätsklinik','university clinic, mannheim']):
                current_affs.append(4210156450)
                current_affs.append(223822909)
            elif 'university hospital heidelberg' in aff_string.lower():
                current_affs.append(4210156450)
                current_affs.append(223822909)
            else:
                current_affs.append(4210156450)
        elif any(word in aff_string.lower() for word in ['university of heidelberg','heidelberg university','faculty of clinical medicine mannheim',
                                                         'universität heidelberg','universitätsklinik','university clinic, mannheim']):
            if any(word in aff_string.lower() for word in ['university of mannheim','mannheim university','universität mannheim']):
                current_affs.remove(177802217)
                current_affs.append(223822909)
        else:
            pass

    # Western Caspian University
    if 3130874397 in current_affs:
        if any(word in aff_string.lower() for word in ['caspian','baku']):
            pass
        else:
            current_affs.remove(3130874397)
            if 'Case Western' in aff_string:
                current_affs.append(58956616)
            elif any(word in aff_string.lower() for word in ['south western', 'central western','western university of health science']):
                pass
            else:
                current_affs.append(125749732)

    # Brown University
    if 27804330 in current_affs:
        if 196272386 in current_affs:
            if any(word in aff_string for word in ['Providence College', 'PC']):
                pass
            else:
                current_affs.remove(196272386)

    # Archéologie et Histoire Ancienne : Méditerranée – Europe
    if 4210089340 in current_affs:
        if any(word in aff_string.lower() for word in ['7044', 'histoire ancienne']):
            pass
        else:
            current_affs.remove(4210089340)

    # Institut Pascal
    if 169645620 in current_affs:
        if any(word in aff_string.lower() for word in ['institut blaise-pascal','institut blaise pascal','institut pascal']):
            pass
        elif any(word in aff_string.lower() for word in ['clermont','auvergne','blaise pascal','blaise-pascal']):
            current_affs.remove(169645620)
            current_affs.append(198244214)
        elif all(word in aff_string.lower() for word in ['blaise','pascal']):
            current_affs.remove(169645620)
            current_affs.append(198244214)
        else:
            pass
    
    # Tekniska Högskolans Studentkår
    if 4210147696 in current_affs:
        current_affs.remove(4210147696)
        current_affs.append(86987016)

    # Purdue University
    if 2801333002 in current_affs:
        current_affs.remove(2801333002)
        if 'Fort Wayne' in aff_string:
            current_affs.append(4210130184)
        elif 'Northwest' in aff_string:
            current_affs.append(117015748)
        elif 'Columbus' in aff_string:
            current_affs.append(59900826)
        else:
            current_affs.append(219193219)

    # Western University (Cambodia)
    if 4210116551 in current_affs:
        if 'cambodia' in aff_string.lower():
            pass
        elif 'KH' in aff_string:
            pass
        else:
            current_affs.remove(4210116551)
            if 'Lurie' in aff_string:
                current_affs.append(4390039227)
            elif 'northwestern university' in aff_string.lower():
                pass
            elif 'western university of sydney' in aff_string.lower():
                pass
            else:
                current_affs.append(125749732)

    # Western University (Canada)
    if 125749732 in current_affs:
        if 'caspian' in aff_string.lower():
            current_affs.remove(125749732)
            current_affs.append(3130874397)

    # UniLaSalle
    if 4210156868 in current_affs:
        if 'france' in aff_string.lower():
            pass
        elif any(word in aff_string.lower() for word in ['beauvais','beauvaisis','institut polytechnique']):
            pass
        else:
            current_affs.remove(4210156868)
            current_affs.append(4210102413)

    # Humana
    if 96048030 in current_affs:
        if 'humana' in aff_string.lower():
            if any(word in aff_string.lower() for word in ['louisville','kentucky','united states']):
                pass
            elif any(word in aff_string for word in ['KY', ' 40217', 'USA']):
                pass
            else:
                current_affs.remove(96048030)
        else:
            current_affs.remove(96048030)

    # Bocas del Toro Research Station
    if 4210115077 in current_affs:
        if 'Smith' in aff_string:
            pass
        else:
            current_affs.remove(4210115077)

    # American Institutes for Research
    if 1293631320 in current_affs:
        if 'ethical approval' in aff_string.lower():
            current_affs.remove(1293631320)

    # # Amsterdam University Medical Center
    # if 4210151833 in current_affs:
    #     if 865915315 in current_affs:
    #         current_affs.remove(865915315)

    # Anton Pannekoek Institute for Astronomy
    if 2898336195 in current_affs:
        if 'anton pannekoek' in aff_string.lower():
            current_affs.remove(2898336195)

    # Chemin de Polytechnique
    if 45683168 in current_affs:
        if 'chemin' in aff_string.lower():
            if any(word in aff_string for word in ['2940','2950','2017']):
                current_affs.remove(45683168)
    
    # Helen Hay Whitney Foundation
    if 262162183 in current_affs:
        if 'Helen' not in aff_string:
            current_affs.remove(262162183)
            current_affs.append(32971472)
    
    # Artificial Intelligence Research Institute
    if 4210131846 in current_affs:
        if 'airi' in aff_string.lower():
            current_affs.remove(4210131846)
            current_affs.append(4392021246)
        elif 'moscow' in aff_string.lower():
            current_affs.remove(4210131846)
            current_affs.append(4392021246)

    # Valleywise Health
    if 4210152461 in current_affs:
        if 'MIHS' not in aff_string:
            if 'health' not in aff_string.lower():
                if 'integrated' not in aff_string.lower():
                    current_affs.remove(4210152461)

    # California Academy of Sciences
    if 2803094215 in current_affs:
        if 'mathematics' in aff_string.lower():
            current_affs.remove(2803094215)

    # Manado State University
    if 3131162304 in current_affs:
        if 'UNIMA' not in aff_string:
            if 'negeri' not in aff_string.lower():
                if 'state' not in aff_string.lower():
                    current_affs.remove(3131162304)

    # Institut Català de Nanociència i Nanotecnologia
    if 4210093216 in current_affs:
        if re.search('\\bIN2UB\\b', aff_string):
            current_affs.append(4401200369)
            current_affs.remove(4210093216)
        elif any(word in aff_string.lower() for word in ["universitat de barcelona","university of barcelona"]):
            current_affs.append(4401200369)
            current_affs.remove(4210093216)

    # Department of Agriculture and Fisheries, Queensland Government
    if 2801244131 in current_affs:
        if 'Department of Agriculture and Fisheries, Queensland Government' in aff_string:
            current_affs.remove(2801244131)
            current_affs.append(4210164589)

    # St Xavier's in India
    if 906608882 in current_affs:
        if any(word in aff_string
               for word in ["St. Xavier's University", "St. Xaviers University",
                            "St Xaviers University", "St Xavier's University"]):
            if 'Kolkata' in aff_string:
                current_affs.append(4400573289)
                current_affs.remove(906608882)

    # Helsinki Art Museum
    if 4210102852 in current_affs:
        if 'elsingin yliopisto' in aff_string:
            current_affs.append(133731052)
            current_affs.remove(4210102852)

    # IIM
    if 150870154 in current_affs:
        if 'Bodhgaya' in current_affs:
            current_affs.append(4400600926)
            current_affs.remove(150870154)

    # Concordia University
    if 105925353 in current_affs:
        if any(word in aff_string.lower() for word in ["université concordia", "universite concordia",
                                                        "québec", "montréal", "quebec", "montreal",
                                                         "h3g ", "h3g1m8", "maisonneuve"]):
            current_affs.append(60158472)
            current_affs.remove(105925353)

    # University of Arizona
    if 4210122332 in current_affs:
        if any(word in aff_string.lower() for word in ['optical sciences center', 'college of optical sciences', 'wyant']):
            current_affs.remove(4210122332)
            current_affs.append(138006243)

    if 4210166658 in current_affs:
        if 'university of arizona' in aff_string.lower():
            current_affs.remove(4210166658)
            current_affs.append(138006243)

    # Bari fixes
    if 68618741 in current_affs:
        if any(word in aff_string for word in ['Technical University of Bari','Technical Univ. of Bari',
                                            'University and Politecnico of Bari',
                                            'Technical University Politecnico di Bari',
                                            'Polytechnic University of Bari',
                                            'Politecnico di Bari',
                                            'Polytechnic of Bari',
                                            'Univ. and Politecnico of Bari','Polytechinic University of Bari']):
            pass
        else:
            current_affs.remove(68618741)

    # Institut Polytechnique de Paris
    if 4210145102 in current_affs:
        if any(word in aff_string for word in ['Telecom SudParis','Télécom SudParis','Telecom SudParís',
                                               'TELECOM SudParis','Telecom-SudParis','Telecom Sudparis',
                                               'Télécom Sud Paris','Telecom SudParis','IPParis']):
            pass
        elif re.search('\\bIPP\\b', aff_string):
            if any(word in aff_string.lower() for word in ['palaiseau','paris']):
                pass
            else:
                current_affs.remove(4210145102)
        else:
            current_affs.remove(4210145102)

    # Adana Hospital
    if 4210094594 in current_affs:
        if any(word in aff_string.lower() for word in ['başkent','baskent','university hospital']):
            pass
        else:
            current_affs.remove(4210094594)

    # Amity Univ Noida
    if 191972202 in current_affs:
        if any(word in aff_string for word in ['Noida']):
            pass
        else:
            current_affs.remove(191972202)

    # Natl Inst Technol - Trichy
    if 122964287 in current_affs:
        if any(word in aff_string for word in ['National Engineering College']):
            if 'Kovilpatti' in aff_string:
                current_affs.remove(122964287)
        elif 'National College' in aff_string:
            if 'Tiruchirappalli' in aff_string:
                if not re.search('\\bNIT\\b', aff_string):
                    if 'National Institute' not in aff_string:
                        current_affs.remove(122964287)
        else:
            pass

    # Afyonkarahisar Health Sciences University and Kutahya Health Sciences University
    if 4210128276 in current_affs:
        if 'Afyonkarahisar Health Sciences University' in aff_string:
            current_affs.remove(4210128276)
            current_affs.append(4387154071)
        elif 'Kutahya Health Sciences University' in aff_string:
            current_affs.remove(4210128276)
            current_affs.append(4387156457)
        else:
            pass

    # Islamic Azad University
    if any(aff in current_affs for aff in [155419210, 110525433]):
        # Islamic Azad University Rasht Branch
        if 'Rasht' in aff_string:
            current_affs.append(4210098966)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Mahabad
        elif 'Mahabad' in aff_string:
            current_affs.append(4210101274)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Ahvaz Branch
        elif 'Ahvaz' in aff_string:
            current_affs.append(4210147666)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Zahedan Branch
        elif 'Zahedan' in aff_string:
            current_affs.append(4210103813)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Mashhad
        elif 'Mashhad' in aff_string:
            current_affs.append(183859904)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, UAE Branch
        elif 'UAE' in aff_string:
            current_affs.append(4210124555)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Naragh Branch
        elif 'Naragh' in aff_string:
            current_affs.append(4210124802)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Karaj
        elif 'Karaj' in aff_string:
            current_affs.append(204588832)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Falavarjan
        elif 'Falavarjan' in aff_string:
            current_affs.append(2802842351)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Shabestar
        elif 'Shabestar' in aff_string:
            current_affs.append(4210130889)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Qazvin Islamic Azad University
        elif 'Qazvin' in aff_string:
            current_affs.append(197220011)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Semnan
        elif 'Semnan' in aff_string:
            current_affs.append(4210104301)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Azadshahr Branch
        elif 'Azadshahr' in aff_string:
            current_affs.append(4210147006)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University North Tehran Branch
        elif 'North Tehran' in aff_string:
            current_affs.append(183067279)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Firoozkooh Branch
        elif 'Firoozkooh' in aff_string:
            current_affs.append(4210103782)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Ayatollah Amoli
        elif 'Ayatollah Amoli' in aff_string:
            current_affs.append(4210116557)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Sanandaj Branch
        elif 'Sanandaj' in aff_string:
            current_affs.append(2801503745)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Damghan Branch
        elif 'Damghan' in aff_string:
            current_affs.append(4210159640)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Dehaghan
        elif 'Dehaghan' in aff_string:
            current_affs.append(4210106588)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Qaemshahr Islamic Azad University
        elif 'Qaemshahr' in aff_string:
            current_affs.append(4210138903)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Islamshahr Branch
        elif 'Islamshahr' in aff_string:
            current_affs.append(4210159652)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Damavand
        elif 'Damavand' in aff_string:
            current_affs.append(4210163293)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Birjand
        elif 'Birjand' in aff_string:
            current_affs.append(4210122540)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Maybod
        elif 'Maybod' in aff_string:
            current_affs.append(4210145406)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Shoushtar Branch
        elif 'Shoushtar' in aff_string:
            current_affs.append(4210153432)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Kerman
        elif 'Kerman' in aff_string:
            current_affs.append(4210152146)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Larestan Branch
        elif 'Larestan' in aff_string:
            current_affs.append(4210145150)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Shirvan Branch
        elif 'Shirvan' in aff_string:
            current_affs.append(4210120980)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Pharmaceutical Sciences Branch
        elif 'Pharmaceutical Sciences' in aff_string:
            current_affs.append(4210152512)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Science and Research Branch
        elif 'Science and Research' in aff_string:
            current_affs.append(155419210)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Marvdasht
        elif 'Marvdasht' in aff_string:
            current_affs.append(4210090965)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Shahrekord
        elif 'Shahrekord' in aff_string:
            current_affs.append(4210121772)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Sari Branch
        elif 'Sari' in aff_string:
            current_affs.append(4210086811)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Buin-Zahra
        elif 'Buin-Zahra' in aff_string:
            current_affs.append(4210123837)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Firuzabad Branch
        elif 'Firuzabad' in aff_string:
            current_affs.append(4210095796)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Omidieh Branch
        elif 'Omidieh' in aff_string:
            current_affs.append(4210127412)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Jiroft Branch
        elif 'Jirt' in aff_string:
            current_affs.append(4210122456)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Gorgan
        elif 'Gorgan' in aff_string:
            current_affs.append(4210117791)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Yasuj
        elif 'Yasuj' in aff_string:
            current_affs.append(4210126091)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Tonekabon
        elif 'Tonekabon' in aff_string:
            current_affs.append(4210149299)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Nishapur
        elif 'Nishapur' in aff_string:
            current_affs.append(4210113978)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Shiraz
        elif 'Shiraz' in aff_string:
            current_affs.append(91138267)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Sirjan Branch
        elif 'Sirjan' in aff_string:
            current_affs.append(4210137408)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Nowshahr Branch
        elif 'Nowshahr' in aff_string:
            current_affs.append(4210118876)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Gachsaran
        elif 'Gachsaran' in aff_string:
            current_affs.append(4210093963)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Sepidan
        elif 'Sepidan' in aff_string:
            current_affs.append(4387152370)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Majlesi
        elif 'Majlesi' in aff_string:
            current_affs.append(261916979)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Central Tehran Branch
        elif 'Central Tehran' in aff_string:
            current_affs.append(41775361)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Masjed Soleyman
        elif 'Masjed Soleyman' in aff_string:
            current_affs.append(4210158817)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Malayer Branch
        elif 'Malayer' in aff_string:
            current_affs.append(4210131670)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Marand
        elif 'Marand' in aff_string:
            current_affs.append(4210151143)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Kashmar Branch
        elif 'Kashmar' in aff_string:
            current_affs.append(4210088567)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Tabriz
        elif 'Tabriz' in aff_string:
            current_affs.append(1293555014)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Kermanshah
        elif 'Kermanshah' in aff_string:
            current_affs.append(2801954088)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Dolatabad
        elif 'Dolatabad' in aff_string:
            current_affs.append(4210140966)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Dental Branch of Tehran
        elif 'Dental   Tehran' in aff_string:
            current_affs.append(2801553314)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Shahr-e-Qods Branch
        elif 'Shahr-e-Qods' in aff_string:
            current_affs.append(4210116438)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Bonab Branch
        elif 'Bonab' in aff_string:
            current_affs.append(4210156805)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Varamin
        elif 'Varamin' in aff_string:
            current_affs.append(4210109236)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Fasa Branch
        elif 'Fasa' in aff_string:
            current_affs.append(4210108082)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Khorramabad Branch
        elif 'Khorramabad' in aff_string:
            current_affs.append(4210129655)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Arsanjan
        elif 'Arsanjan' in aff_string:
            current_affs.append(4210146840)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Ardabil
        elif 'Ardabil' in aff_string:
            current_affs.append(4210161716)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Mobarakeh
        elif 'Mobarakeh' in aff_string:
            current_affs.append(4210103000)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Khomeynishahr
        elif any(word in aff_string for word in ['Khomeinishahr', 'Khomeynishahr']):
            current_affs.append(2799360687)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Mahshahr
        elif 'Mahshahr' in aff_string:
            current_affs.append(189748745)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Isfahan
        elif 'Isfahan' in aff_string:
            current_affs.append(2799282979)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Malard
        elif 'Malard' in aff_string:
            current_affs.append(3010108046)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Parand
        elif 'Parand' in aff_string:
            current_affs.append(2802594446)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Najafabad
        elif 'Najafabad' in aff_string:
            current_affs.append(9256017)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Lahijan Branch
        elif 'Lahijan' in aff_string:
            current_affs.append(33162209)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Takestan Islamic Azad University
        elif 'Takestan' in aff_string:
            current_affs.append(4210165322)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Chalous
        elif 'Chalous' in aff_string:
            current_affs.append(4210118335)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Bandar Abbas
        elif 'Bandar Abbas' in aff_string:
            current_affs.append(4210110333)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Roudehen Branch
        elif 'Roudehen' in aff_string:
            current_affs.append(1306291479)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Zanjan
        elif 'Zanjan' in aff_string:
            current_affs.append(131837588)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Izeh Islamic Azad University
        elif 'Izeh' in aff_string:
            current_affs.append(4210144160)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Quchan
        elif 'Quchan' in aff_string:
            current_affs.append(4210152523)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University South Tehran Branch
        elif 'South Tehran' in aff_string:
            current_affs.append(136830121)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Arak
        elif 'Arak' in aff_string:
            current_affs.append(2801771032)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Boroujerd Branch
        elif 'Boroujerd' in aff_string:
            current_affs.append(4210114866)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Tehran
        elif 'Tehran' in aff_string:
            current_affs.append(110525433)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Farahan
        elif 'Farahan' in aff_string:
            current_affs.append(2802465448)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Hamedan
        elif 'Hamedan' in aff_string:
            current_affs.append(4210089026)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Kashan
        elif 'Kashan' in aff_string:
            current_affs.append(2801582476)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Bostanabad
        elif 'Bostanabad' in aff_string:
            current_affs.append(4210155841)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Shahreza
        elif 'Shahreza' in aff_string:
            current_affs.append(4210094515)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Khoy Branch
        elif 'Khoy' in aff_string:
            current_affs.append(4210124016)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Medical Branch of Tehran
        elif 'Medical   Tehran' in aff_string:
            current_affs.append(2800744764)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Tafresh
        elif 'Tafresh' in aff_string:
            current_affs.append(4210120974)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Urmia
        elif 'Urmia' in aff_string:
            current_affs.append(4210163840)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Aliabad Katoul
        elif 'Aliabad Katoul' in aff_string:
            current_affs.append(4210111814)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Yazd
        elif 'Yazd' in aff_string:
            current_affs.append(4210159272)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Babol
        elif 'Babol' in aff_string:
            current_affs.append(4400573191)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Garmsar
        elif 'Garmsar' in aff_string:
            current_affs.append(115420810)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Bandar Anzali Branch
        elif 'Bandar Anzali' in aff_string:
            current_affs.append(4210126954)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Komijan Branch
        elif 'Komijan' in aff_string:
            current_affs.append(4210115839)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Sabzevar
        elif 'Sabzevar' in aff_string:
            current_affs.append(4210140773)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Shahr-e-Rey
        elif 'Shahr-e-Rey' in aff_string:
            current_affs.append(4210154044)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University of Ahar
        elif 'Ahar' in aff_string:
            current_affs.append(4210112351)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Saveh
        elif 'Saveh' in aff_string:
            current_affs.append(4210162396)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Kazeron
        elif 'Kazeron' in aff_string:
            current_affs.append(4210125694)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Janah
        elif 'Janah' in aff_string:
            current_affs.append(4210154612)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Dezful Branch
        elif 'Dezful' in aff_string:
            current_affs.append(4210155764)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Ashtian Branch
        elif 'Ashtian' in aff_string:
            current_affs.append(4210106052)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Bushehr Branch
        elif 'Bushehr' in aff_string:
            current_affs.append(4210132474)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Abhar Branch
        elif 'Abhar' in aff_string:
            current_affs.append(4210110389)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Qom Islamic Azad University
        elif 'Qom' in aff_string:
            current_affs.append(4210113955)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Shahrood
        elif 'Shahrood' in aff_string:
            current_affs.append(4210093808)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Jahrom Branch
        elif 'Jahrom' in aff_string:
            current_affs.append(4210108307)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Mehriz
        elif 'Mehriz' in aff_string:
            current_affs.append(4210096727)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Boukan
        elif 'Boukan' in aff_string:
            current_affs.append(4210091465)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Bojnourd Branch
        elif 'Bojnourd' in aff_string:
            current_affs.append(93979751)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Estahban Branch
        elif 'Estahban' in aff_string:
            current_affs.append(4210090247)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Langarud Branch
        elif 'Langarud' in aff_string:
            current_affs.append(4210139362)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University, Shahinshahr Branch
        elif 'Shahinshahr' in aff_string:
            current_affs.append(4210157053)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

        # Islamic Azad University Ilam Branch
        elif 'Ilam' in aff_string:
            current_affs.append(4210165053)
            if 110525433 in current_affs:
                current_affs.remove(110525433)
            if 155419210 in current_affs:
                current_affs.remove(155419210)

    # AIIMS (All India Institute of Medical Sciences)
    if 63739035 in current_affs:
        # All India Institute of Medical Sciences Bhubaneswar
        if 'Bhubaneswar' in aff_string:
            current_affs.append(4210117092)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Bhopal
        elif 'Bhopal' in aff_string:
            current_affs.append(4210106490)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences, Nagpur
        elif 'Nagpur' in aff_string:
            current_affs.append(4401200305)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Guwahati
        elif 'Guwahati' in aff_string:
            current_affs.append(4387153078)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Rishikesh
        elif 'Rishikesh' in aff_string:
            current_affs.append(4387152206)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Raipur
        elif 'Raipur' in aff_string:
            current_affs.append(129734738)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences Jodhpur
        elif 'Jodhpur' in aff_string:
            current_affs.append(216021267)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

        # All India Institute of Medical Sciences, Deoghar
        elif 'Deoghar' in aff_string:
            current_affs.append(4396570500)
            if 63739035 in current_affs:
                current_affs.remove(63739035)

    # Tianjin Medical University General Hospital
    if 5740404 in current_affs:
        if 'Tianjin Medical University General Hospital' in aff_string:
            current_affs.append(2802534033)
            current_affs.remove(5740404)

    # National Kaohsiung University of Science and Technology
    if 192168892 in current_affs:
        if any(word in aff_string.lower() for word in ['kaohsiung university of sci',
                                                       'kaohsiung univ. of science and tech',
                                                       'kaohsiung university sciences and tech']):
            current_affs.remove(192168892)
            current_affs.append(4387154394)

    # Institut Polytechnique de Paris
    if 4210145102 in current_affs:
      if any(word in aff_string for word in ['Telecom SudParis','Télécom SudParis','Telecom SudParís',
                                              'TELECOM SudParis','Telecom-SudParis','Telecom Sudparis',
                                              'Télécom Sud Paris','Telecom SudParis','IPParis',
                                              'Institut Polytechnique de Paris']):
          pass
      elif re.search('\\bIPP\\b', aff_string):
          if any(word in aff_string.lower() for word in ['palaiseau','paris']):
              pass
      elif re.search('\\bIP Paris\\b', aff_string):
          pass
      else:
          current_affs.remove(4210145102)

    # AGH University of Science and Technology
    if any(inst in current_affs for inst in [686019]):
        if 4210163816 in current_affs:
            current_affs.remove(4210163816)

    # Changchun University of Technology
    if any(inst in current_affs for inst in [4385474403]):
        if 49232843 in current_affs:
            current_affs.remove(49232843)
        if 106645853 in current_affs:
            current_affs.remove(106645853)

    # China Medical University
    if any(inst in current_affs for inst in [184693016,4210126829]):
        if 91656880 in current_affs:
            current_affs.remove(91656880)
        if 91807558 in current_affs:
            current_affs.remove(91807558)

    # China University of Petroleum East China
    if any(inst in current_affs for inst in [4210162190]):
        if 204553293 in current_affs:
            current_affs.remove(204553293)

    # Chinese Academy of Medical Sciences Peking Union Medical College
    if any(inst in current_affs for inst in [200296433]):
        if 4210119648 in current_affs:
            current_affs.remove(4210119648)
        if 4210141683 in current_affs:
            current_affs.remove(4210141683)
        if 4210147708 in current_affs:
            current_affs.remove(4210147708)
        if 4210092004 in current_affs:
            current_affs.remove(4210092004)
        if 4210141458 in current_affs:
            current_affs.remove(4210141458)

    # Claude Bernard University Lyon 1
    if any(inst in current_affs for inst in [100532134]):
        if 203339264 in current_affs:
            current_affs.remove(203339264)

    # Cornell University
    if any(inst in current_affs for inst in [205783295,4210152471]):
        if 145220665 in current_affs:
            current_affs.remove(145220665)
        if 92528248 in current_affs:
            current_affs.remove(92528248)

    # Czech Technical University in Prague
    if any(inst in current_affs for inst in [44504214]):
        if 4210100395 in current_affs:
            current_affs.remove(4210100395)

    # Education University of Hong Kong
    if any(inst in current_affs for inst in [4210086892]):
        if 16518940 in current_affs:
            current_affs.remove(16518940)
        if 200769079 in current_affs:
            current_affs.remove(200769079)
        if 177725633 in current_affs:
            current_affs.remove(177725633)
        if 168719708 in current_affs:
            current_affs.remove(168719708)
        if 8679417 in current_affs:
            current_affs.remove(8679417)

    # Ohio University
    if any(inst in current_affs for inst in [4210106879]):
        if 22759111 in current_affs:
            current_affs.remove(22759111)

    # Poznan University of Life Sciences
    if any(inst in current_affs for inst in [55783418]):
        if 158552681 in current_affs:
            current_affs.remove(158552681)

    # Graz University of Technology
    if any(inst in current_affs for inst in [4092182]):
        if 15766117 in current_affs:
            current_affs.remove(15766117)

    # Heinrich Heine University Düsseldorf
    if any(inst in current_affs for inst in [44260953]):
        if 75356249 in current_affs:
            current_affs.remove(75356249)

    # Henan Polytechnic University
    if any(inst in current_affs for inst in [4210166499]):
        if 4210115515 in current_affs:
            current_affs.remove(4210115515)

    # Hubei University
    if any(inst in current_affs for inst in [75900474]):
        if 4210099437 in current_affs:
            current_affs.remove(4210099437)
        if 4210154851 in current_affs:
            current_affs.remove(4210154851)

    # Indian Institute of Technology Dhanbad
    if any(inst in current_affs for inst in [189109744]):
        if 64295750 in current_affs:
            current_affs.remove(64295750)

    # Indian Institute of Technology Hyderabad
    if any(inst in current_affs for inst in [65181880]):
        if 64189192 in current_affs:
            current_affs.remove(64189192)

    # Indiana University Purdue University Indianapolis
    if any(inst in current_affs for inst in [55769427]):
        if 592451 in current_affs:
            current_affs.remove(592451)

    # Indiana University Bloomington
    if any(inst in current_affs for inst in [4210119109]):
        if 592451 in current_affs:
            current_affs.remove(592451)

    # Inner Mongolia Agricultural University
    if any(inst in current_affs for inst in [120379545]):
        if 190774190 in current_affs:
            current_affs.remove(190774190)

    # Inner Mongolia University
    if any(inst in current_affs for inst in [2722730]):
        if 55654194 in current_affs:
            current_affs.remove(55654194)

    # Islamic Azad University Science and Research Branch
    if any(inst in current_affs for inst in [155419210]):
        if 55654194 in current_affs:
            current_affs.remove(55654194)

    # China University of Geosciences, Wuhan (need to remove 3016766249)
    if any(inst in current_affs for inst in [3124059619]):
        if 3016766249 in current_affs:
            current_affs.remove(3016766249)

    # Texas Tech University (remove 4210088475)
    if any(inst in current_affs for inst in [12315562]):
        if 4210088475 in current_affs:
            current_affs.remove(4210088475)

    # Anhui Medical University
    if any(aff_id in current_affs for aff_id in [4210136596, 4210161469, 4210149412]):
        current_affs.append(197869895)

    # Central South University (first/4210159865, second/4210153856, third/4210156904)
    if any(aff_id in current_affs for aff_id in [4210159865, 4210153856, 4210156904, 139660479]):
        current_affs.append(139660479)
        if 4210159865 in current_affs:
            if 'Second Xiangya Hospital' in aff_string:
                current_affs.remove(4210159865)
                if 4210153856 not in current_affs:
                    current_affs.append(4210153856)
            if 'Third Xiangya Hospital' in aff_string:
                if 4210159865 in current_affs:
                    current_affs.remove(4210159865)
                if 4210156904 not in current_affs:
                    current_affs.append(4210156904)

        if 4210153856 in current_affs:
            if 'Second Xiangya Hospital' not in aff_string:
                current_affs.remove(4210153856)
                if 'Third Xiangya Hospital' not in aff_string:
                    if 'Xiangya Hospital' in aff_string:
                        if 4210159865 not in current_affs:
                            current_affs.append(4210159865)

            if 'Third Xiangya Hospital' in aff_string:
                if 4210153856 in current_affs:
                    current_affs.remove(4210153856)
                if 4210156904 not in current_affs:
                    current_affs.append(4210156904)

        if 4210156904 in current_affs:
            if 'Third Xiangya Hospital' not in aff_string:
                current_affs.remove(4210156904)
                if 'Second Xiangya Hospital' not in aff_string:
                    if 'Xiangya Hospital' in aff_string:
                        if 4210159865 not in current_affs:
                            current_affs.append(4210159865)

            if 'Second Xiangya Hospital' in aff_string:
                if 4210156904 in current_affs:
                    current_affs.remove(4210156904)
                if 4210153856 not in current_affs:
                    current_affs.append(4210153856)

    # China University of Geosciences
    if any(aff_id in current_affs for aff_id in [3124059619,3125743391]):
        if 'Wuhan' in aff_string:
            if 'Beijing' not in aff_string:
                if 3125743391 in current_affs:
                    current_affs.remove(3125743391)
                    current_affs.append(3124059619)

        if 'Beijing' in aff_string:
            if 'Wuhan' not in aff_string:
                if 3124059619 in current_affs:
                    current_affs.remove(3124059619)
                    current_affs.append(3125743391)

    # Chongqing Medical University
    if any(aff_id in current_affs for aff_id in [4210159428, 4210129459, 4210097509, 4210128042, 87780372]):
        current_affs.append(87780372)
        if 4210129459 in current_affs:
            if 'Second Affiliated Hospital' in aff_string:
                current_affs.remove(4210129459)
                if 4210097509 not in current_affs:
                    current_affs.append(4210097509)

        if 4210097509 in current_affs:
            if 'First Affiliated Hospital' in aff_string:
                current_affs.remove(4210097509)
                if 4210129459 not in current_affs:
                    current_affs.append(4210129459)

    # Civil Aviation Flight University of China
    if 28813325 in current_affs:
        if 'Civil Aviation Flight University of China' in aff_string:
            current_affs.append(58995867)
            current_affs.remove(28813325)

    # Civil Aviation University of China
    if 58995867 in current_affs:
        if 'Civil Aviation University of China' in aff_string:
            current_affs.append(28813325)
            current_affs.remove(58995867)

    # Dalian Medical University
    if any(aff_id in current_affs for aff_id in [4210097509, 4210140813, 191996457]):
        current_affs.append(191996457)
        if 4210140813 in current_affs:
            if 'Second Affiliated Hospital' in aff_string:
                current_affs.remove(4210140813)
                if 4210097509 not in current_affs:
                    current_affs.append(4210097509)

        if 4210100868 in current_affs:
            if 'First Affiliated Hospital' in aff_string:
                current_affs.remove(4210100868)
                if 4210140813 not in current_affs:
                    current_affs.append(4210140813)

    # Fujian Medical University
    if any(aff_id in current_affs for aff_id in [129708740, 4210121761, 4210134617]):
        current_affs.append(129708740)
        if 4210121761 in current_affs:
            if 'Second Affiliated Hospital' in aff_string:
                current_affs.remove(4210121761)
                if 4210134617 not in current_affs:
                    current_affs.append(4210134617)

        if 4210134617 in current_affs:
            if 'First Affiliated Hospital' in aff_string:
                current_affs.remove(4210134617)
                if 4210121761 not in current_affs:
                    current_affs.append(4210121761)

    # Goethe University Frankfurt
    if 4210132578 in current_affs:
        current_affs.append(114090438)

    # Guangzhou Medical University
    if any(aff_id in current_affs for aff_id in [92039509, 4210153921, 4210105982, 4210116575, 4210098361,
                                                 4210090868, 4210092091]):
        current_affs.append(92039509)
        if 4210098361 in current_affs:
            if ('Second Affiliated Hospital' in aff_string) or ('Third Affiliated Hospital' in aff_string):
                current_affs.remove(4210098361)
                if 'Second Affiliated Hospital' in aff_string:
                    if 4210090868 not in current_affs:
                        current_affs.append(4210090868)

                if 'Third Affiliated Hospital' in aff_string:
                    if 4210092091 not in current_affs:
                        current_affs.append(4210092091)

        if 4210090868 in current_affs:
            if ('First Affiliated Hospital' in aff_string) or ('Third Affiliated Hospital' in aff_string):
                current_affs.remove(4210090868)
                if 'First Affiliated Hospital' in aff_string:
                    if 4210098361 not in current_affs:
                        current_affs.append(4210098361)

                if 'Third Affiliated Hospital' in aff_string:
                    if 4210092091 not in current_affs:
                        current_affs.append(4210092091)

        if 4210092091 in current_affs:
            if ('First Affiliated Hospital' in aff_string) or ('Second Affiliated Hospital' in aff_string):
                current_affs.remove(4210092091)
                if 'First Affiliated Hospital' in aff_string:
                    if 4210098361 not in current_affs:
                        current_affs.append(4210098361)

                if 'Second Affiliated Hospital' in aff_string:
                    if 4210090868 not in current_affs:
                        current_affs.append(4210090868)

    # Harbin Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 156144747, [4210087423],
                                                   [4210156501, 4210132813, 4210103956, 4210122309],
                                                   ['First Affiliated Hospital','Second Affiliated Hospital',
                                                    'Third Affiliated Hospital','Fourth Affiliated Hospital'])

    # Hebei Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 111381250, [4210115020],
                                                   [4210099373, 4210088328, 4210106405],
                                                   ['Second Affiliated Hospital',
                                                    'Third Affiliated Hospital','Fourth Affiliated Hospital'])

    # Jichi Medical University
    if any(aff_id in current_affs for aff_id in [4210153031, 4210166222]):
        current_affs.append(146500386)


    # Jilin University
    if any(aff_id in current_affs for aff_id in [4210125137, 4210103885]):
        current_affs.append(194450716)

    # Keimyung University
    if 4210128080 in current_affs:
        current_affs.append(52010207)

    # Kunming Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 26080491, [],
                                                   [4210120169, 2799435780, 4210165315,4210146235],
                                                   ['First Affiliated Hospital','Second Affiliated Hospital',
                                                    'Sixth Affiliated Hospital',"Yan'an Hospital"])

    # Lanzhou University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 76214153, [],
                                                   [4210163492, 4210124531],
                                                   ['First Hospital','Second Hospital'])

    # Loma Linda University
    if 1293502524 in current_affs:
        current_affs.append(26347476)

    # Louisiana State University Health Sciences Center
    if 121820613 in current_affs:
        if any(word in aff_string for word in ['Health Sciences Center','School of Medicine']):
            if 'New Orleans' in aff_string:
                current_affs.append(75420490)
            elif 'Shreveport' in aff_string:
                current_affs.append(81020160)

    # University Mohammed V
    if any(word in aff_string for word in ['Mohammed V', 'Mohammed-V']):
        if 'VI' not in aff_string:
            if any(word in aff_string for word in ['Agdal', 'Rabat', 'Morocco']):
                current_affs.append(126477371)

    # Ningxia Medical University
    if 4210139449 in current_affs:
        current_affs.append(4210127460)

    # Second Military Medical University
    if any(aff_id in current_affs for aff_id in [4210137389, 4210115928, 4210151530]):
        current_affs.append(177933477)

    # Shandong Academy of Medical Science
    if any(aff_id in current_affs for aff_id in [4210162355, 4210100830,4210156461]):
        current_affs.append(4210163399)

    # Shanxi Medical University
    if any(aff_id in current_affs for aff_id in [4210125748, 4387154184, 4210160763, 4210133678]):
        current_affs.append(17721919)

    # Tianjin Medical University
    if any(aff_id in current_affs for aff_id in [4210133270, 2800200322, 4210145773,
                                                 4210088587, 4210095816]):
        current_affs.append(5740404)

    # Wenzhou Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 27781120,
                                                   [4210156545, 4210099263, 4210158774],
                                                   [4210086973, 2801769982, 4210099512],
                                                   ['Affiliated Eye Hospital','First Affiliated Hospital',
                                                    'Second Affiliated Hospital'])

    # Xinjiang Medical University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 154093214, [],
                                                   [2802734952, 4210165944, 4210102015, 2801820870,
                                                    4210151285],
                                                   ['First Affiliated Hospital', 'Second Affiliated Hospital',
                                                    'Third Affiliated Hospital', 'Fifth Affiliated Hospital',
                                                    'Sixth Affiliated Hospital'])

    # Xuzhou Medical College
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 177388780, [],
                                                   [4210106614, 4210140789],
                                                   ['First Affiliated Hospital', 'Second Affiliated Hospital'])

    # Nanchang University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 141649914, [],
                                                   [4210164024, 4210108480, 4210114086],
                                                   ['First Affiliated Hospital', 'Second Affiliated Hospital',
                                                    'Third Affiliated Hospital'])

    # Nantong University
    if any(aff_id in current_affs for aff_id in [4210086801, 4210085873, 4210112320, 4210122818, 4210119554]):
        current_affs.append(199305430)

    # Peking University
    if any(aff_id in current_affs for aff_id in [2802957242, 4210093964, 4210130930, 4210133846, 4210124809,
                                                 4210162420, 4210095659, 4210141942]):
        current_affs.append(20231570)

    # University of Macau
    if 111950717 in current_affs:
        if 'University of Macau' in aff_string:
            current_affs.remove(111950717)
            current_affs.append(204512498)

    # Toho University
    if any(aff_id in current_affs for aff_id in [4210125448, 4210093329, 4210095039]):
        current_affs.append(129634264)

    # Sun Yat-sen University
    current_affs = chinese_medical_university_affs(aff_string, current_affs, 157773358,
                                                   [4210128272, 4210129003, 4210119259, 4210097354,
                                                    4210146711, 4387155047],
                                                   [4210128921, 4210146956,
                                                    4210113039, 4210093460,
                                                    4210096354, 4387154481],
                                                   ['First Affiliated Hospital', 'Third Affiliated Hospital',
                                                    'Fifth Affiliated Hospital', 'Sixth Affiliated Hospital',
                                                    'Seventh Affiliated Hospital', 'Eighth Affiliated Hospital'])

    # Qingdao University
    if any(aff_id in current_affs for aff_id in [4210116869, 4210119987, 4210167271]):
        current_affs.append(108688024)

    # University of Lübeck
    if 4210112713 in current_affs:
        current_affs.append(9341345)

    # SUNY
    if 1327163397 in current_affs:
        if 'Albany' in aff_string:
            current_affs.remove(1327163397)
            current_affs.append(392282)

        elif 'Buffalo' in aff_string:
            current_affs.remove(1327163397)
            current_affs.append(63190737)

    # SUNY Upstate Medical University
    if any(aff_id in current_affs for aff_id in [4210089004, 4210114695, 4210106223]):
        current_affs.append(20388574)

    # Jiangsu University
    if any(aff_id in current_affs for aff_id in [4210111628, 4210110396, 4210124790, 4210104909,
                                                 4210139780, 4210146175]):
        current_affs.append(115592961)

    # University of Georgia
    if 4210126868 in current_affs:
        if 'Tbilisi' not in aff_string:
            current_affs.append(165733156)
            current_affs.remove(4210126868)

    # University of Eastern Piedmont Amadeo Avogadro
    if 4210119436 in current_affs:
        current_affs.append(123338534)

    # University of Colorado Denver
    if any(aff_id in current_affs for aff_id in [4210096275, 2801983979, 3019586173, 2802547023,
                                                 4210134151, 1288162130]):
        current_affs.append(921990950)

    # University of Tennessee at Knoxville
    if 2802076678 in current_affs:
        current_affs.append(75027704)

    # Xuzhou Medical College
    if any(aff_id in current_affs for aff_id in [4210106614, 4210140789, 4210124997, 4210143072]):
        current_affs.append(177388780)

    # UT Southwestern Medical Center
    if any(aff_id in current_affs for aff_id in [4210096815]):
        current_affs.append(867280407)

    # University of Reims Champagne-Ardenne
    if 4210105796 in current_affs:
        current_affs.append(96226040)

    # Soochow University
    if any(aff_id in current_affs for aff_id in [4210151382, 4210153519, 4210166543, 4210123502, 4210124971,
                                                 4210133251, 4210108364]):
        current_affs.append(3923682)

    # Virginia Tech
    if 4210166782 in current_affs:
        current_affs.append(859038795)
        current_affs.remove(4210166782)

    # University Canada West
    if 214977574 in current_affs:
        if 'University Canada West' not in aff_string:
            current_affs.remove(214977574)

    # Universidad Central de Chile
    if 4210156023 in current_affs:
        if any(word in aff_string for word in ['Universidad Central de Chile','UCEN', 'Central University of Chile']):
            pass
        elif any(word in aff_string for word in ['Universidad de Chile']):
            current_affs.remove(4210156023)
            current_affs.append(69737025)
        elif any(word in aff_string for word in ['Pontificia Universidad Católica de Chile']):
            current_affs.remove(4210156023)
            current_affs.append(162148367)
        else:
            current_affs.remove(4210156023)

    # Hamilton College (getting confused with Hamilton Institute at Maynooth University)
    if 188592606 in current_affs:
        if 'Hamilton Institute' in aff_string:
            current_affs.remove(188592606)

    # University of Quebec
    if any(inst in current_affs for inst in [159129438,63341726,104914703,182451676,33217400,190270569,
                                              39481719,31571312,9736820,200745827]):
        if 49663120 in current_affs:
            current_affs.remove(49663120)

    u_quebec_strings = ['Université du Québec','University of Quebec','University of Québec','Quebec University',
                             'Universite du Quebec','Univ Quebec','Univ. Quebec','Univ Québec','Univ. Québec',
                             'Quebec Univ.','Québec Univ.','Univ. of Quebec','Univ. of Québec','U de Québec',
                             'U de Quebec','Univ. du Quebec','Univ. du Québec']

    # Removing bad University of Quebec matches from Laval U. and U. of Montreal
    if 49663120 in current_affs:
        if any(word in aff_string for word in ['Laval University','University of Montreal']):
            if any(word in aff_string for word in u_quebec_strings):
                pass
            else:
                current_affs.remove(49663120)
                if 'Laval University' in aff_string:
                    current_affs.append(43406934)
                elif 'University of Montreal' in aff_string:
                    current_affs.append(70931966)

    # University of Maryland
    if 116545467 in current_affs:
        if 'University of Maryland' in aff_string:
            current_affs.remove(116545467)
            if 'Baltimore' in aff_string:
                if 'Baltimore County' in aff_string:
                    current_affs.append(79272384)
                else:
                    current_affs.append(126744593)
            elif any(word in aff_string for word in ['Princess Anne','Eastern Shore']):
                current_affs.append(22407884)
            elif 'College Park' in aff_string:
                current_affs.append(66946132)
    if any(aff_id in current_affs for aff_id in [4210132871,1315496137]):
        current_affs.append(126744593)

    # Comenius University Bratislava
    if 4210095125 in current_affs:
        current_affs.append(74788687)

    # University of North Texas
    if 165139151 in current_affs:
        current_affs.append(123534392)

    # University of Veterinary Medicine Hannover, Foundation
    if 114112103 in current_affs:
        if 'University of Veterinary Medicine' in aff_string:
            current_affs.append(189991)
            current_affs.remove(114112103)

    # University of South China
    if 4210143187 in current_affs:
        current_affs.append(91935597)

    # Wuhan University
    if any(aff_id in current_affs for aff_id in [4210131162, 4210125402, 4210140357,
                                                 4210126156, 4210120234]):
        current_affs.append(37461747)

    # SUNY Upstate Medical University
    if any(aff_id in current_affs for aff_id in [4210089004, 4210114695, 4210106223]):
        current_affs.append(20388574)

    # University College London
    if 2800173700 in current_affs:
        if 'University College London' in aff_string:
            current_affs.remove(2800173700)
        elif 'UCL' in aff_string:
            if 'London' in aff_string:
                current_affs.remove(2800173700)

    if any(aff_id in current_affs for aff_id in [2800129641, 4210150574, 4210141040, 4210151618,
                                                 2802751111, 4210118734, 2801138448, 1289784979,
                                                 4210119772, 2802844630, 2802576581]):
        current_affs.append(45129253)

    # Radboud University
    if any(aff_id in current_affs for aff_id in [4210109357, 2802934949]):
        current_affs.append(145872427)
        current_affs.append(2802934949)
    elif any(aff_id in current_affs for aff_id in [2801238018, 4210126394]):
        current_affs.append(145872427)

    # Shanghai University of Traditional Chinese Medicine
    if any(aff_id in current_affs for aff_id in [4210101984, 4210149132, 4210124018, 4210133418]):
        current_affs.append(4210098460)

    # Technical University Dortmund
    if 4210166399 in current_affs:
        current_affs.append(200332995)

    # University of Liverpool
    if any(aff_id in current_affs for aff_id in [2802775644, 4210089066, 4210165020, 2799272705, 4210157731,
                                                 2801018919, 4210134784, 4210086861, 4210093008, 4210157668,
                                                 2799451200, 4210111181, 2802833755, 4210086643]):
        current_affs.append(146655781)

    # Sichuan University
    if any(aff_id in current_affs for aff_id in [4210089228, 4210089761]):
        current_affs.append(24185976)

    # Johns Hopkins University
    if any(aff_id in current_affs for aff_id in [4210098865, 2802697821, 4210129832, 2799853436]):
        current_affs.append(145311948)

    # Shantou University
    if any(aff_id in current_affs for aff_id in [4210091098, 4210115239, 4210120522, 4210121200, 4210109945]):
        current_affs.append(32574673)

    # Johannes Gutenberg University Mainz
    if any(aff_id in current_affs for aff_id in [4210094062, 4387156336, 4210148626]):
        current_affs.append(197323543)

    # Yokohama City University
    if 2802180866 in current_affs:
        current_affs.append(89630735)
    elif 4210099218 in current_affs:
        current_affs.append(89630735)

    # Tampere University
    if 150589677 in current_affs:
        if 'Tampere University of Technology' in aff_string:
            if 'Tampere University of Applied Sciences' not in aff_string:
                current_affs.append(4210133110)
                current_affs.remove(150589677)

    # University of Ulster
    if any(aff_id in current_affs for aff_id in [2802259370,2802808109,4210130048]):
        current_affs.append(138801177)

    # Bloomberg
    if 1299907687 in current_affs:
        if 'Bloomberg School of' in aff_string:
            current_affs.remove(1299907687)
            current_affs.append(145311948)

    # Coventry (UK)
    if 4210127762 in current_affs:
        if 39555362 in current_affs:
            current_affs.remove(4210127762)

    # Hershey
    if 123457487 in current_affs:
        if any(aff_id in current_affs for aff_id in [130769515, 82783531]):
            current_affs.remove(123457487)

    # Södra Skogsägarna
    if 4210151240 in current_affs:
        if not any(word in aff_string for word in ['Södra Skogsägarna','Sodra Skogsägarna','Sodra Skogsagarna']):
            current_affs.remove(4210151240)

    # Bayer
    if 67348948 in current_affs:
        if 'Bayerisch' in aff_string:
            if 'Germany' in aff_string:
                current_affs.remove(67348948)

    # Eppendorf
    if 2801537753 in current_affs:
        if any(aff_id in current_affs for aff_id in [159176309, 4210108711, 4210125929]):
            current_affs.remove(2801537753)
        elif any(word in aff_string for word in ['University Medical Center',
                                                 'University Medical Centre']):
            if 'Hamburg' in aff_string:
                current_affs.remove(2801537753)
                current_affs.append(4210108711)
                current_affs.append(159176309)
        elif 'Hamburg-Eppendorf' in aff_string:
            current_affs.append(4210108711)
            current_affs.append(159176309)


    # Applied Mathematics
    if 4210131439 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Applied Mathematics Inst',
                                               'Applied Mathematics Dep',
                                               'Applied Mathematics Div']):
            current_affs.remove(4210131439)

    # Applied Materials (Germany)
    if 4210165146 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Applied Materials Inst',
                                               'Applied Materials Dep',
                                               'Applied Materials Div']):
            current_affs.remove(4210165146)
        elif len(current_affs) > 1:
            current_affs.remove(4210165146)

    # Applied Materials (United States)
    if 193427800 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Applied Materials Inst',
                                               'Applied Materials Dep',
                                               'Applied Materials Div']):
            current_affs.remove(193427800)
        elif len(current_affs) > 1:
            current_affs.remove(193427800)

    # Applied Materials (Israel)
    if 4210100008 in current_affs:
        if len(current_affs) > 1:
            current_affs.remove(4210100008)

    # Applied Materials (U.K.)
    if 4210087370 in current_affs:
        if len(current_affs) > 1:
            current_affs.remove(4210087370)

    # Applied Materials (Singapore)
    if 4210102879 in current_affs:
        if len(current_affs) > 1:
            current_affs.remove(4210102879)

    # Bioengineering (Switzerland)
    if 4210092485 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Bioengineering Inst',
                                               'Bioengineering Dep',
                                               'Bioengineering Div']):
            current_affs.remove(4210092485)

    # Visual Sciences (USA)
    if 4210107648 in current_affs:
        if 'Raleigh' not in aff_string:
            current_affs.remove(4210107648)

    # Quantum Group (USA)
    if 4210090401 in current_affs:
        if 'San Diego' not in aff_string:
            current_affs.remove(4210090401)

    # Engineering (Italy)
    if 4210127672 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Engineering Inst',
                                               'Engineering Dep',
                                               'Engineering Div']):
            current_affs.remove(4210127672)

    # Neurobehavioral Research (USA)
    if 4210157590 in current_affs:
        if 'Neurobehavioral Research' not in aff_string:
            current_affs.remove(4210157590)
        elif any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for']):
            current_affs.remove(4210157590)

    # Materials Research Institute (USA)
    if 4210148571 in current_affs:
        if 'Dayton' not in aff_string:
            current_affs.remove(4210148571)


    # Materials Sciences (USA)
    if 4210111788 in current_affs:
        if any(word in aff_string for word in ['Division of',
                                               'Department of',
                                               'Institute of',
                                               'Center for',
                                               'Engineering Inst',
                                               'Engineering Dep',
                                               'Engineering Div']):
            current_affs.remove(4210111788)
        elif 'Horsham' not in aff_string:
            current_affs.remove(4210111788)

    # Surgical Science (Sweden)
    if 4210147392 in current_affs:
        if 'Göteborg' not in aff_string:
            current_affs.remove(4210147392)

    # Computational Sciences (USA)
    if 4210129810 in current_affs:
        if 'Madison' not in aff_string:
            current_affs.remove(4210129810)

    # Translational Sciences (USA)
    if 4210163070 in current_affs:
        if 'Memphis' not in aff_string:
            current_affs.remove(4210163070)

    # Institut Henri Poincaré
    if ('henri' in aff_string.lower()) | (51178685 in current_affs):
        if any(word in aff_string.lower() for word in ['henri poincaré','henri poincare']) | (51178685 in current_affs):
            if 51178685 in current_affs:
                if 'IHP Group' in aff_string:
                    current_affs.remove(51178685)
                elif re.search('\\bIHP\\b', aff_string) and ('Paris' in aff_string):
                    pass
                elif not any(word in aff_string for word in ['Institut Henri Poincaré','Henri Poincaré Institut','Institut Henri Poincaré',
                                                        'Henri Poincaré Institut','Institute Henri Poincare']):
                    current_affs.remove(51178685)

                if 51178685 not in current_affs:
                    if any(word in aff_string for word in ['Henri Poincaré','Henri Poincare']) and ('Nancy' in aff_string):
                        current_affs.append(90183372)

            if any(word in aff_string for word in ['Henri Poincaré','Henri Poincare']):
                if 'Nancy' in aff_string:
                    current_affs.append(90183372)
                elif any(word in aff_string for word in ['Institut Henri Poincaré','Henri Poincaré Institut','Institut Henri Poincaré',
                                                        'Henri Poincaré Institut','Institute Henri Poincare']):
                    current_affs.append(51178685)

    # Roskilde University
    if 'Roskilde' in aff_string:
        if any(word in aff_string.lower() for
               word in ['Roskilde Uni','University of Roskilde', 'Universidade de Roskilde',
                        'Rosikilde University','Universities of Roskilde and Copenhagen']) & (107707843 in current_affs):
            current_affs.remove(107707843)

    # Hochschule Hannover
    if 140025399 in current_affs:
        if any(word in aff_string.lower() for word in ['medizinische','medical','med.','medische']):
            current_affs.remove(140025399)
            current_affs.append(34809795)
        elif any(word in aff_string.lower() for word in ['technische','techn.']):
            current_affs.remove(140025399)
            current_affs.append(114112103)

    # Twitter
    if 113979032 in current_affs:
        if '@' in aff_string:
            current_affs.remove(113979032)

    # BIOM
    if 4210131549 in current_affs:
        if any(word in aff_string for word in ["BOME", "Biologie des organismes marins et écosystèmes"]):
            current_affs.remove(4210131549)
            current_affs.append(4210110009)
        elif any(word in aff_string for word in ["ISOMER","Institut Des Substances et Organismes de la Mer"]):
            current_affs.remove(4210131549)
            current_affs.append(4210144488)
        elif any(word in aff_string for word in ["BOREA", "Biologie des Organismes et Ecosystèmes Aquatiques"]):
            current_affs.remove(4210131549)
            current_affs.append(4210110009)
        elif any(word in aff_string for word in ["EFNO", "Ecosystèmes forestiers","BioMEA","PFOM",
                                                 "Physiologie Fonctionnelle des Organismes Marins"]):
            current_affs.remove(4210131549)

    # Gateway
    if 4210139101 in current_affs:
        current_affs.remove(4210139101)

    # Australian College of Theology
    if 2800615496 in current_affs:
        if 'theology' not in aff_string.lower():
            current_affs.remove(2800615496)

    # Southern Institute of Technology
    if 2802042008 in current_affs:
        if not any(word in aff_string for word in ['Southern Institute of Technology','SIT']):
            current_affs.remove(2802042008)

    # Access e.V.
    if 4210121009 in current_affs:
        if not any(word in aff_string.lower() for word in ['access e.v.', 'access e. v.', 'accessmm e. v.', 'access ev ',
                                                'access ev.','access ev,']):
            current_affs.remove(4210121009)

    # Manipal University Jaipur
    if 164861460 in current_affs:
        if 'manipal university jaipur' in aff_string.lower():
            current_affs.remove(164861460)
            current_affs.append(73779912)

    # Westfälische Hochschule
    if 4210145899 in current_affs:
        if 887968799 in current_affs:
            current_affs.remove(4210145899)

    # École des Ponts ParisTech
    if 142631665 in current_affs:
        if any(word in aff_string.lower() for word in ['laboratoire central des ponts',
                                                       'laboratoire régional des ponts',
                                                       'laboratoire regional des ponts']) | ('LCPC' in aff_string):
            if not any(word in aff_string.lower() for word in ['ecole nationale des ponts','ecole des ponts paristech']):
                current_affs.remove(142631665)


    if not current_affs:
        current_affs.append(-1)

    return list(set(current_affs))