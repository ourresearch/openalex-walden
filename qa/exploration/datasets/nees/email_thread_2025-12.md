# Email Thread: Metadata Completeness Drop (Nees Jan van Eck, CWTS)

**Date range:** 2025-12-10 to 2025-12-19
**Participants:**
- Nees Jan van Eck (CWTS Leiden) <ecknjpvan@cwts.leidenuniv.nl>
- Jason Priem (OpenAlex) <jason@openalex.org>
- Kyle Demes (OpenAlex) <kyle@ourresearch.org>

**Topics:**
- Affiliation metadata completeness drop in 2024
- Reference metadata completeness drop in 2024
- Elsevier affiliations
- IEEE references
- Cloudflare scraping issues

**Related data:**
- Nees provided spreadsheet with 1,000 random DOI samples (works in Scopus/WoS missing affiliations or references in OpenAlex)
- Tabs include: Random 2024 works w/o affiliations, Random 2024 works w/o references, Sources with largest drops

**Status:** High priority fix - targeting beginning of 2026

---

## Message 1

**From:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**To:** Jason Priem <jason@ourresearch.org>, Kyle Demes <kyle@ourresearch.org>
**Date:** Wed, Dec 10, 2025 at 5:00 PM

Hi Jason and Kyle,

I hope you are doing well.

I wanted to share something I recently discovered that raises some concerns, and I would appreciate your perspective on whether you are aware of this and if there's anything that can be done about it.

I'm frequently asked to speak about my experience working with OpenAlex for bibliometric purposes. In preparation for one of these talks, I wanted to visualize the completeness of affiliation and reference data in OpenAlex. I have done similar analyses in the past, but this time I aimed to update them using recent data.

My approach has always been to demonstrate that, when focusing on specific subsets, OpenAlex metadata completeness is not as poor as often claimed. I have seen many naive comparisons between OpenAlex and proprietary sources like Scopus and WoS, where completeness is assessed based on all works. OpenAlex inevitably looks bad in those cases, but I find such comparisons unfair because OpenAlex has much broader coverage, including many works that are harder to enrich with metadata. That's why I always emphasize that if you only consider works indexed in both OpenAlex and the proprietary databases, the picture is much more positive. This forms the basis of my argument that OpenAlex can be used for bibliometric purposes, provided the right selection is made.

However, in my recent analysis of journal articles (works of type 'article' or 'review' and published in sources of type 'journal'), I noticed a significant decline in the percentage of articles with affiliations and references for 2024 (the most recent year I could check against Scopus and WoS). For affiliations, some of the larger publishers where this drop can be observed include Elsevier, and for references, IEEE seems to be one of the major publishers that seem to play a role in this trend.

To illustrate these findings, I have included three figures below:

### Figure 1: Percentage of Journal Articles with Raw Affiliation Strings Over Time

| Year | OpenAlex | In Scopus | In WoS |
|------|------|-----------|--------|
| 2000 | 50%  | 88%       | 90%    |
| 2002 | 52%  | 90%       | 92%    |
| 2004 | 55%  | 92%       | 94%    |
| 2006 | 58%  | 94%       | 95%    |
| 2008 | 60%  | 95%       | 96%    |
| 2010 | 62%  | 96%       | 97%    |
| 2012 | 63%  | 96%       | 97%    |
| 2014 | 64%  | 96%       | 97%    |
| 2016 | 65%  | 96%       | 97%    |
| 2018 | 68%  | 96%       | 97%    |
| 2020 | 72%  | 96%       | 96%    |
| 2022 | 78%  | 94%       | 94%    |
| 2024 | 70%  | 85%       | 85%    |

### Figure 2: Percentage of Journal Articles with Institutions Over Time

| Year | OpenAlex | In Scopus | In WoS |
|------|------|-----------|--------|
| 2000 | 40%  | 85%       | 88%    |
| 2002 | 42%  | 88%       | 90%    |
| 2004 | 45%  | 90%       | 92%    |
| 2006 | 46%  | 92%       | 92%    |
| 2008 | 48%  | 93%       | 93%    |
| 2010 | 50%  | 94%       | 94%    |
| 2012 | 52%  | 94%       | 94%    |
| 2014 | 54%  | 95%       | 95%    |
| 2016 | 56%  | 95%       | 95%    |
| 2018 | 60%  | 95%       | 95%    |
| 2020 | 65%  | 95%       | 94%    |
| 2022 | 72%  | 92%       | 90%    |
| 2024 | 65%  | 80%       | 62%    |

### Figure 3: Percentage of Journal Articles with Cited References Over Time

| Year | OpenAlex | In Scopus | In WoS |
|------|------|-----------|--------|
| 2000 | 30%  | 82%       | 78%    |
| 2002 | 32%  | 85%       | 82%    |
| 2004 | 35%  | 88%       | 85%    |
| 2006 | 40%  | 90%       | 88%    |
| 2008 | 45%  | 92%       | 92%    |
| 2010 | 50%  | 93%       | 94%    |
| 2012 | 55%  | 94%       | 95%    |
| 2014 | 58%  | 95%       | 95%    |
| 2016 | 60%  | 95%       | 96%    |
| 2018 | 62%  | 95%       | 96%    |
| 2020 | 65%  | 95%       | 96%    |
| 2022 | 70%  | 94%       | 95%    |
| 2024 | 68%  | 92%       | 93%    |

These trends concern me, especially because they also affect publications that are present in Scopus and WoS. This decrease in metadata completeness impact institutional and citation analyses, and organizations considering a switch from Scopus/WoS to OpenAlex might become more hesitant due to these trends.

Are you aware of these findings, and do you know what might be causing this? Could it be related to recent changes in how metadata (from Crossref) is supplemented, for example, if web scraping has become less effective?

Looking forward to your thoughts.

Best,
Nees

---

## Message 2

**From:** Kyle Demes <kyle@ourresearch.org>
**To:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**Cc:** Jason Priem <jason@ourresearch.org>
**Date:** Wed, Dec 10, 2025 at 5:29 PM

Hi Nees,

Thanks for sharing these analyses and concerns. The drop in affiliation metadata in recent years is a known issue that we're looking into. I'm sure you can imagine that every institution looking at their output over time has been pretty shocked to see their outputs have declined by 10% when they expect them to go up each year. The drop in references was not known to me, though fortunately it doesn't seem as extreme as the drop in affiliation for scopus/wos works. Will add that to the list. We've got a fair number of large bugs we're still working through with Walden that might explain the drop here, so they might still get fixed, but will add this to the list as well separately to investigate.

-kyle

---

## Message 3

**From:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**To:** Kyle Demes <kyle@ourresearch.org>
**Cc:** Jason Priem <jason@ourresearch.org>
**Date:** Thu, Dec 11, 2025 at 1:24 PM

Hi Kyle,

Thank you for your quick response. I appreciate you confirming that the drop in affiliation metadata is a known issue and that it's being investigated. It's reassuring to hear that this is on your radar, given the impact on institutional analyses.

The decline in references does seem fortunately less extreme, but given the importance of this data in citation analyses, I'm glad to hear you'll add this to the list as well.

I want to mention that these findings are also present in the pre-Walden August snapshot. This suggests the issues may not be limited to recent Walden changes but could point to larger, more foundational problems in how metadata is being ingested, merged, or supplemented.

I completely understand that you're focused on fixing the bigger Walden-related bugs, which is important. My friendly advice would be to prioritize these metadata completeness issues right after, since people are concerned about these types of issues. This makes it harder to convince organizations to work with OpenAlex data, and addressing it sooner would help maintain confidence.

Please keep me posted if you learn more about the underlying causes or any fixes. I'd also be happy to think along or help in any way that's useful. In the meantime, do you have any advice on how I should mention and frame these issues in my upcoming talks? I'm a big promoter of OpenAlex but also want to be transparent.

Thanks again for your openness and for looking into this.

Best,
Nees

---

## Message 4

**From:** Kyle Demes <kyle@ourresearch.org>
**To:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**Cc:** Jason Priem <jason@ourresearch.org>
**Date:** Fri, Dec 12, 2025 at 8:52 AM

Thanks Nees. I accept your friendly advice and agree that this is a big priority. We're hiring two new technical staff that will hopefully be able to help us move more quickly. Unfortunately, until we know more about the cause of the issue I don't have much advice on framing. I will keep you posted though on the causes and courses of action soon so that you are equipped to discuss these topics. There may also be ways that you can help, so we appreciate the offer and will keep you posted.

have a good weekend,
kyle

---

## Message 5

**From:** Kyle Demes <kyle@ourresearch.org>
**To:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**Cc:** Jason Priem <jason@ourresearch.org>
**Date:** Fri, Dec 12, 2025 at 10:15 AM

Actually! One thing that would be helpful Nees is if you can send us a random sample of say 1,000 works that are in Scopus/WoS that we don't have raw affiliation strings? We have Casey looking into our pipelines for leaks, but it might be helpful in diagnosing other issues if you can do that.

-kyle

---

## Message 6

**From:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**To:** Kyle Demes <kyle@ourresearch.org>
**Cc:** Jason Priem <jason@ourresearch.org>
**Date:** Sun, Dec 14, 2025 at 8:40 AM

Hi Kyle,

Sure, I have collected some data for you. Please see this spreadsheet. It contains the following tabs:

- **Random 2024 works w/o affiliations 1 (in Scopus/WoS):** 1,000 random 2024 works without affiliations that are also indexed in Scopus/WoS
- **Random 2024 works w/o affiliations 2 (in Scopus/WoS):** 1,000 random 2024 works without affiliations that are also indexed in Scopus/WoS, from sources that show the largest drop in affiliations from 2023 to 2024 (many from Elsevier)
- **Random 2024 works w/o references 1 (in Scopus/WoS):** 1,000 random 2024 works without references that are also indexed in Scopus/WoS
- **Random 2024 works w/o references 2 (in Scopus/WoS):** 1,000 random 2024 works without references that are also indexed in Scopus/WoS, from sources that show the largest drop in references from 2023 to 2024 (many from IEEE)
- **Sources largest drop affiliations (in Scopus/WoS):** Sources that show the largest drop in affiliations from 2023 to 2024, considering only works indexed in Scopus/WoS
- **Sources largest drop references (in Scopus/WoS):** Sources that show the largest drop in references from 2023 to 2024, considering only works indexed in Scopus/WoS
- **Sources largest drop affiliations:** Sources that show the largest drop in affiliations from 2023 to 2024
- **Sources largest drop references:** Sources that show the largest drop in references from 2023 to 2024

Hope this helps! Looking forward to seeing what you find.

---

## Message 7

**From:** Jason Priem <jason@openalex.org>
**To:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**Cc:** Kyle Demes <kyle@ourresearch.org>
**Date:** Mon, Dec 15, 2025 at 10:23 PM

This is fantastically helpful, thank you so much! I dug in today and learned a lot. I'm focusing on affiliations for now.

I have the beginnings of a plan on how to fix these for new records. It will take a little while but is doable.

But then we also need to fix all the old records where we are missing data. The fastest approach would just be to run the fix over each affected DOI.

So is it possible for you to send us a complete list rather than just the sample? In other words, you would send us all the DOI's where:
- Scopus or WoS have got an affiliation but
- we do NOT have a raw_affiliation_string.

Then we could focus on these works.

Thanks a lot for your help!

J

---

## Message 8

**From:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**To:** Jason Priem <jason@openalex.org>
**Cc:** Kyle Demes <kyle@ourresearch.org>
**Date:** Wed, Dec 17, 2025 at 3:25 PM

Hi Jason,

Great to hear that the sample was helpful and it's even better that you already have an idea for a solution! I'm really curious about what's causing the drop in metadata completeness. If you're able to share any thoughts on that, I'd love to hear them.

One thing I've been wondering about myself: could it be that scraping landing pages has become harder lately because many publishers and platforms are nowadays using services like Cloudflare? We've run into this ourselves at CWTS, so I thought it might be worth mentioning.

I'd really like to send you the full list of DOIs for publications without affiliations in OpenAlex but with affiliations in WoS/Scopus. Unfortunately, we can't do that because it would put us in violation of the license terms from Clarivate and Elsevier. So as much as I'd like to help in that way, we're limited by what those agreements allow.

That said, please let me know if there's any other way we can help without using WoS/Scopus data. We're happy to support any efforts to make OpenAlex better.

One idea that might help: focusing on works that have references in OpenAlex but no affiliations. From the examples I've checked, these usually do have affiliations in practice, and the percentage of these works that are also in Scopus and WoS seems quite high. I hope this helps!

Looking forward to hearing your thoughts!

Best,
Nees

---

## Message 9

**From:** Jason Priem <jason@openalex.org>
**To:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**Cc:** Kyle Demes <kyle@ourresearch.org>
**Date:** Wed, Dec 17, 2025 at 3:30 PM

Thanks Nees. Bummer about the data but I get it...I guess we really need an Open Data alternative to those systems, someone should build one! ;)

We'll keep plugging away and will get it fixed asap. It's a high priority. Thanks so much for your very helpful data, including the figures and examples. Golden.

j

---

## Message 10

**From:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**To:** Jason Priem <jason@openalex.org>
**Cc:** Kyle Demes <kyle@ourresearch.org>
**Date:** Wed, Dec 17, 2025 at 3:34 PM

Haha, yes indeed!

Glad to hear it's a high priority and that the data and examples were useful. If there's anything else we can do, just let me know!

---

## Message 11

**From:** Kyle Demes <kyle@ourresearch.org>
**To:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**Cc:** Jason Priem <jason@ourresearch.org>
**Date:** Wed, Dec 17, 2025 at 9:58 PM

No worries Nees. Thanks anyway. Jason, we do have a slightly out of date mapping of OpenAlex sources that are in Scopus (from the public version released). We can start with that list and then filter by works with no affiliation strings.

More soon Nees. We're hoping to get this solved by the beginning of the new year....

-kyle

---

## Message 12

**From:** Eck, N.J.P. van (Nees Jan) <ecknjpvan@cwts.leidenuniv.nl>
**To:** Kyle Demes <kyle@ourresearch.org>
**Cc:** Jason Priem <jason@openalex.org>
**Date:** Fri, Dec 19, 2025 at 5:33 AM

Thanks for the update. Wishing you both a happy Christmas and keep up the great work in the new year!
