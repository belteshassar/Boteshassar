# 1. query all supreme court decisions with link to fulltext
# 2. download fulltext
# 3. run the extract citations
# 4. for each found citation: query for legal citation
# 5. if exactly 1 legal citation is found: add statement
# 6. Else: print notice

from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from io import BytesIO
import json
import os
import random
import re
from wikidataintegrator import wdi_core, wdi_login, wdi_config
from pdfminer.high_level import extract_text
import requests
import sys


PROPS = {
    'instance_of': 'P31',
    'title': 'P1476',
    'date': 'P577',
    'jurisdiction': 'P1001',
    'subject': 'P921',
    'nja_ref': 'P1031',
    'court': '4884',
    'judge': 'P1594',
    'obj_role': 'P3831',
    'laws_applied': 'P3014',
    'published_in': 'P1433',
    'page': 'P304',
    'link': 'P953',
    'has_part': 'P527',
    'opinion_joined': 'P7122',
    'author': 'P50',
    'cites': 'P2860',
    'language': 'P407',
    'case_no': 'P8407',
    'ref_url': 'P854',
    'retrieved': 'P813',
}


def extract_citations(pdf_file):
    """Generator that yields citations of NJA cases, propositions, SOU,
    committee reports, or motions. Takes a pdf file path or a file-like object representing a pdf file.
    """
    text = extract_text(pdf_file)
    for m in re.finditer(r'NJA\s+\d{4}\s+s\.?\s+\d+(\s+I*V)?', text):
        yield re.sub('\s+', ' ', m.group(0)), None
    for m in re.finditer(r'[pP]rop(?:osition|\.)?\s+(\d{4}\/(?:\d{2}|2000):\d+)(?:\s+s\.\s+(\d+))?', text):
        yield 'Prop. ' + m.group(1), m.group(2)
    for m in re.finditer(r'(SOU\s+\d{4}:\d+)(?:\s+s\.\s+(\d+))?', text):
        yield re.sub('\s+', ' ', m.group(1)), m.group(2)
    for m in re.finditer(r'[bB]et(?:änkande|\.)?\s+(\d{4}\/(?:\d{2}|2000):\w+\d+)(?:\s+s\.\s+(\d+))?', text):
        yield 'bet. ' + m.group(1), m.group(2)
    for m in re.finditer(r'[mM]ot(?:ion|\.)?\s+(\d{4}\/(?:\d{2}|2000):\d{2})(?:\s+s\.\s+(\d+))?', text):
        yield 'Mot. ' + m.group(1), m.group(2)


@lru_cache(maxsize=2**15)
def query_legal_citation(citation):
    """Send question to WDQS to find the item with given legal citation.
    Returns item id only if a single unique item was found, otherwise
    return None. Cache results to avoid unnecessary load on WDQS.
    """
    q = 'SELECT * WHERE { ?item wdt:P1031 "' + citation + '" }'
    r = wdi_core.WDItemEngine.execute_sparql_query(q)
    return [row['item']['value'].rpartition('/')[2] for row in r['results']['bindings']]


q = """
SELECT * WHERE {
  ?item wdt:P31 wd:Q96482904 ;
        wdt:P953 ?url ;
        wdt:P577 ?date .
  OPTIONAL { ?item p:P527 [ps:P527 wd:Q6738447; prov:wasDerivedFrom/pr:P1476 ?title ] }
}
ORDER BY DESC( ?date )
"""

edit_group = '{:x}'.format(random.randrange(0, 2**48))
edit_summary = f'Add citation from Supreme Court cases ([[:toollabs:editgroups/b/CB/{edit_group}|details]])'
with open('credentials.json') as f:
    cred = json.load(f)
user_agent = f'{cred["user"]}/{sys.version_info[0]}.{sys.version_info[1]}'
login_instance = wdi_login.WDLogin(user=cred['user'], pwd=cred['password'])

r = wdi_core.WDItemEngine.execute_sparql_query(q)

for binding in r['results']['bindings']:
    item = binding['item']['value'].rpartition('/')[2]
    pdf = binding['url']['value']
    statements = []

    try:
        ref_title = binding['title']['value']
        ref_date = binding['date']['value']
        refs = [[
            wdi_core.WDUrl(pdf, PROPS['ref_url'], is_reference=True),
            wdi_core.WDMonolingualText(ref_title, PROPS['title'], language='sv', is_reference=True),
            wdi_core.WDTime(f'+{ref_date}', PROPS['date'], is_reference=True),
            wdi_core.WDTime(datetime.utcnow().strftime('+%Y-%m-%dT00:00:00Z'), PROPS['retrieved'], is_reference=True),
        ]]
    except Exception:
        refs = [[
            wdi_core.WDUrl(pdf, PROPS['ref_url'], is_reference=True),
            wdi_core.WDTime(f'+{ref_date}', PROPS['date'], is_reference=True),
            wdi_core.WDTime(datetime.utcnow().strftime('+%Y-%m-%dT00:00:00Z'), PROPS['retrieved'], is_reference=True),
        ]]
    try:
        resp = requests.get(pdf)
    except Exception:
        print(f'Error when fetching file for item {item}')
        continue
    citations = defaultdict(set)
    try:
        for citation, page in extract_citations(BytesIO(resp.content)):
            if page:
                citations[citation].add(page)
            else:
                citations[citation]
    except Exception:
        print(f'Error when parsing file for item {item}')
        continue
    for citation, pages in citations.items():
        targets = query_legal_citation(citation)
        if not targets:
            print(f'Item {item}: Found no item with legal citation {citation}.')
            continue
        if len(targets) > 1:
            print(f"Item {item}: Found multiple items with legal citation {citation}: {', '.join(bound['item']['value'].rpartition('/')[2] for bound in  r['results']['bindings'])}.")
            continue
        target = targets[0]
        statements.append(wdi_core.WDItemID(target, PROPS['cites'], references=refs))
        statements[-1].set_qualifiers([wdi_core.WDString(page, PROPS['page'], is_qualifier=True) for page in pages])
    if statements:
        wd_item = wdi_core.WDItemEngine(wd_item_id=item, data=statements, user_agent=user_agent, append_value=['P2860'], global_ref_mode='STRICT_KEEP')
        try:
            wd_item.write(login_instance, bot_account=False, edit_summary=edit_summary)
        except Exception:
            print(f'Error when editing item {item}')
            raise
