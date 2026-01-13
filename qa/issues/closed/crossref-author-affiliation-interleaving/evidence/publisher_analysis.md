# Publisher Analysis for Author/Affiliation Interleaving

Generated: 2026-01-12

## Summary

Analysis of major publishers (100k+ records) by their affected rate for the author/affiliation interleaving issue.

| Category | Count | Criteria |
|----------|-------|----------|
| SAFE | 63 | < 0.1% affected |
| LOW RISK | 51 | 0.1% - 1% affected |
| AFFECTED | 8 | >= 1% affected |

## SAFE Publishers (< 0.1% affected)

These publishers should NOT have their records modified by any fix:

| Publisher | Records | Affected | Rate |
|-----------|---------|----------|------|
| IEEE | 4,430,380 | 2,363 | 0.05% |
| Informa UK Limited | 4,831,113 | 4,095 | 0.08% |
| American Chemical Society (ACS) | 1,837,496 | 1,122 | 0.06% |
| Springer Berlin Heidelberg | 1,650,253 | 896 | 0.05% |
| Institute of Electrical and Electronics Engineers | 1,448,704 | 886 | 0.06% |
| Springer International Publishing | 1,305,318 | 801 | 0.06% |
| Routledge | 1,217,904 | 758 | 0.06% |
| Walter de Gruyter GmbH | 1,133,988 | 637 | 0.06% |
| Elsevier | 1,073,274 | 1,012 | 0.09% |
| Royal Society of Chemistry (RSC) | 797,382 | 500 | 0.06% |
| AIP Publishing | 789,386 | 518 | 0.07% |
| Georg Thieme Verlag KG | 721,145 | 440 | 0.06% |
| Oxford University Press | 700,213 | 256 | 0.04% |
| SPIE | 624,792 | 427 | 0.07% |
| ENCODE Data Coordination Center | 530,781 | 345 | 0.07% |
| ACM | 486,765 | 392 | 0.08% |
| Emerald | 454,348 | 348 | 0.08% |
| Project MUSE | 442,159 | 206 | 0.05% |
| BRILL | 435,134 | 172 | 0.04% |
| CRC Press | 434,948 | 189 | 0.04% |
| University of Chicago Press | 434,722 | 190 | 0.04% |
| Trans Tech Publications, Ltd. | 395,392 | 87 | 0.02% |
| CAIRN | 392,084 | 103 | 0.03% |
| PERSEE Program | 373,911 | 147 | 0.04% |
| Springer Nature Switzerland | 359,671 | 223 | 0.06% |
| OpenEdition | 342,185 | 68 | 0.02% |
| Springer Nature Singapore | 298,032 | 243 | 0.08% |
| De Gruyter | 296,859 | 40 | 0.01% |
| Egypts Presidential Specialized Council | 284,739 | 24 | 0.01% |
| Japan Society of Mechanical Engineers | 271,388 | 66 | 0.02% |
| American Geophysical Union (AGU) | 263,967 | 159 | 0.06% |
| IUCN | 246,650 | 7 | 0.003% |
| Cambridge University Press | 238,495 | 139 | 0.06% |
| H1 Connect | 238,295 | 61 | 0.03% |
| IGI Global | 230,944 | 101 | 0.04% |
| Inderscience Publishers | 222,017 | 52 | 0.02% |
| The Conversation | 206,259 | 108 | 0.05% |
| Institution of Engineering and Technology (IET) | 201,952 | 95 | 0.05% |
| American Institute of Aeronautics & Astronautics | 201,635 | 158 | 0.08% |
| The Electrochemical Society | 200,372 | 126 | 0.06% |
| Center for Open Science | 198,476 | 190 | 0.10% |
| Research Square Platform LLC | 239,612 | 231 | 0.10% |
| Duke University Press | 181,597 | 90 | 0.05% |
| SAE International | 173,025 | 74 | 0.04% |
| Universidade de Sao Paulo | 172,994 | 14 | 0.01% |
| Bentham Science Publishers Ltd. | 168,335 | 139 | 0.08% |
| Acoustical Society of America (ASA) | 167,613 | 51 | 0.03% |
| transcript Verlag | 164,366 | 22 | 0.01% |
| International Union of Crystallography (IUCr) | 159,239 | 118 | 0.07% |
| Edward Elgar Publishing | 156,098 | 105 | 0.07% |
| Association for Computing Machinery (ACM) | 156,076 | 132 | 0.08% |
| Atlantis Press | 154,386 | 3 | 0.002% |
| American Society of Civil Engineers (ASCE) | 149,390 | 62 | 0.04% |
| VS Verlag fur Sozialwissenschaften | 145,630 | 60 | 0.04% |
| World Scientific Pub Co Pte Lt | 137,937 | 68 | 0.05% |
| Scientific Research Publishing, Inc. | 136,316 | 43 | 0.03% |
| ASTM International | 131,471 | 18 | 0.01% |
| Hans Publishers | 122,906 | 0 | 0.00% |
| Egyptian Knowledge Bank | 121,064 | 89 | 0.07% |
| American Society of Mechanical Engineers | 120,678 | 77 | 0.06% |
| The Royal Society | 106,555 | 87 | 0.08% |
| Nomos Verlagsgesellschaft mbH & Co. KG | 100,768 | 16 | 0.02% |
| Sciencedomain International | 100,728 | 29 | 0.03% |

## AFFECTED Publishers (>= 1% affected)

These publishers have significant issues and should be prioritized for fixes:

| Publisher | Records | Affected | Rate |
|-----------|---------|----------|------|
| Qeios | 108,876 | 96,894 | 89.0% |
| Philosophy Documentation Center | 160,348 | 73,242 | 45.7% |
| Publishing House Helvetica | 106,395 | 10,131 | 9.5% |
| Office of Scientific and Technical Information | 312,700 | 11,917 | 3.8% |
| Worldwide Protein Data Bank | 173,151 | 4,619 | 2.7% |
| American Psychological Association (APA) | 636,234 | 14,412 | 2.3% |
| AIP | 116,325 | 2,215 | 1.9% |
| WORLD SCIENTIFIC | 135,805 | 2,055 | 1.5% |

## Recommended Fix Strategy

1. **Apply fix only to AFFECTED publishers** (>= 1% rate) initially
2. **Monitor LOW RISK publishers** (0.1-1%) for false positives
3. **Exclude SAFE publishers** (< 0.1%) from any automated fixes
4. Use publisher-specific logic where patterns differ

## SQL for Exclusion List

```sql
-- Publishers to EXCLUDE from fix (< 0.1% affected rate)
publisher NOT IN (
    'IEEE',
    'Informa UK Limited',
    'American Chemical Society (ACS)',
    'Springer Berlin Heidelberg',
    'Institute of Electrical and Electronics Engineers (IEEE)',
    'Springer International Publishing',
    'Routledge',
    'Walter de Gruyter GmbH',
    'Elsevier',
    'Royal Society of Chemistry (RSC)',
    'AIP Publishing',
    'Georg Thieme Verlag KG',
    'Oxford University Press',
    -- ... add all SAFE publishers
)
```