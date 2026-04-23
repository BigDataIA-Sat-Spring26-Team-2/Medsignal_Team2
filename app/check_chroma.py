import json
from utils.chromadb_client import get_client, get_collection
from collections import Counter

client = get_client()
col = get_collection(client)

all_docs, all_metas = [], []
offset = 0

while True:
    page = col.get(include=['documents', 'metadatas'], limit=300, offset=offset)
    fetched = len(page['documents'])
    if fetched == 0:
        break
    all_docs.extend(page['documents'])
    all_metas.extend(page['metadatas'])
    offset += fetched
    print(f'Fetched {offset} so far...')
    if fetched < 300:
        break

data = [
    {'pmid': m.get('pmid'), 'drug': m.get('drug_name'),
     'year': m.get('year'), 'journal': m.get('journal'), 'text': d}
    for d, m in zip(all_docs, all_metas)
]
data.sort(key=lambda x: (x['drug'] or '', x['pmid'] or ''))

with open('chromadb_abstracts.json', 'w') as f:
    json.dump(data, f, indent=2)

counts = Counter(x['drug'] for x in data)
print('Saved chromadb_abstracts.json')
print(f'Total abstracts: {len(data)}')
print('Per drug:')
for drug, count in sorted(counts.items()):
    print(f'  {drug}: {count}')