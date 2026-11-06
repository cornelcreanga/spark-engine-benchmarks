#!/usr/bin/env python3
"""Pin the official compiled OCSF schema and rank scalar paths by class coverage."""
import argparse, collections, csv, gzip, hashlib, json, re, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def compile_catalog(schema, source, digest):
    objects = schema['objects']
    types = schema['dictionary']['types']['attributes']
    def primitive(t):
        seen = set()
        while t in types and types[t].get('type') and t not in seen:
            seen.add(t); t = types[t]['type']
        return {'integer_t':'long', 'long_t':'long', 'float_t':'double',
                'boolean_t':'boolean'}.get(t, 'string')
    def field(name, a):
        return dict(path=name, logicalType=a['type'], sparkType=primitive(a['type']),
                    array=bool(a.get('is_array')), objectType=a.get('object_type'),
                    requirement=a.get('requirement', 'optional'),
                    enumValues={k:v['caption'] for k,v in a.get('enum', {}).items()},
                    sibling=a.get('sibling'))
    def flatten(attrs, prefix='', ancestors=(), depth=0, guards=()):
        out=[]
        for name,a in sorted(attrs.items()):
            path = prefix+'.'+name if prefix else name
            requirement = a.get('requirement','optional')
            if path == 'metadata.product': requirement = 'required'
            g = guards + ((path,requirement),) if requirement != 'required' else guards
            obj=a.get('object_type')
            if a['type']=='object_t' and not a.get('is_array') and obj in objects and obj not in ancestors and depth<3:
                out += flatten(objects[obj]['attributes'],path,ancestors+(obj,),depth+1,g)
            else:
                f=field(path,a); f['guards']=[dict(path=p,requirement=r) for p,r in g]
                out.append(f)
        bypath={f['path']:f for f in out}
        for f in out:
            if f['sibling'] and f['enumValues']:
                target=(f['path'].rsplit('.',1)[0]+'.' if '.' in f['path'] else '')+f['sibling']
                if target in bypath: bypath[target]['enumSource']=f['path']
        return out
    classes=[]; coverage=collections.Counter(); datatypes=collections.defaultdict(set)
    for name,c in sorted(schema['classes'].items(), key=lambda kv:(kv[1]['uid'],kv[0])):
        if c['uid'] <= 0: continue
        fields=flatten(c['attributes'])
        classes.append(dict(name=name,caption=c['caption'],uid=c['uid'],categoryUid=c['category_uid'],
                            categoryName=c['category_name'],deprecated='@deprecated' in c,profiles=c.get('profiles',[]),fields=fields))
        for f in fields:
            if not f['array'] and f['logicalType'] not in ('object_t','json_t'):
                coverage[f['path']]+=1;datatypes[f['path']].add(f['sparkType'])
    promoted=[];used=set()
    for path,count in sorted(coverage.items(),key=lambda x:(-x[1],x[0].count('.'),x[0])):
        column=path.replace('.','__').replace('/','_')
        if not re.fullmatch('[a-zA-Z_][a-zA-Z0-9_]*',column):raise ValueError(path)
        if column in used: raise ValueError('Column collision: '+column)
        # Numeric widening is safe; incompatible scalar types are kept as strings.
        ts=datatypes[path]; dtype=next(iter(ts)) if len(ts)==1 else ('double' if ts<={'long','double'} else 'string')
        promoted.append(dict(path=path,column=column,sparkType=dtype,classCount=count));used.add(column)
    object_fields={}
    for k,v in sorted(objects.items()):
        fields=[field(n,a) for n,a in sorted(v['attributes'].items())];byname={f['path']:f for f in fields}
        for f in fields:
            if f['enumValues'] and f['sibling'] in byname:byname[f['sibling']]['enumSource']=f['path']
        object_fields[k]=fields
    return dict(version=schema['version'],source=source,sourceSha256=digest,
                flattenObjectDepth=3,extensions=sorted(schema['extensions']),
                types=types,objects=object_fields,
                classes=classes,rankedFields=promoted)

def main():
    p=argparse.ArgumentParser();p.add_argument('--version',default='1.9.0');p.add_argument('--source-file',type=Path);a=p.parse_args()
    url=f'https://schema.ocsf.io/{a.version}/export/v2/schema'
    raw=a.source_file.read_bytes() if a.source_file else urllib.request.urlopen(url,timeout=90).read()
    schema=json.loads(raw)
    if schema['version']!=a.version:raise ValueError('Unexpected schema version')
    digest=hashlib.sha256(raw).hexdigest();model=compile_catalog(schema,url,digest)
    dest=ROOT/'dataset-generator/src/main/resources/ocsf';dest.mkdir(parents=True,exist_ok=True)
    data=json.dumps(model,sort_keys=True,separators=(',',':')).encode()
    (dest/f'catalog-{a.version}.json.gz').write_bytes(gzip.compress(data,mtime=0))
    (dest/f'schema-{a.version}.json.gz').write_bytes(gzip.compress(raw,mtime=0))
    provenance=dict(version=a.version,source=url,sourceSha256=digest,catalogSha256=hashlib.sha256(data).hexdigest(),
                    classes=len(model['classes']),extensions=model['extensions'],flattenObjectDepth=3,
                    selection='Class presence descending, then shallower paths, then path name; arrays/objects stay in map.')
    (dest/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
    with (ROOT/'config/ocsf-top-fields.csv').open('w') as f:
        w=csv.DictWriter(f,lineterminator='\n',fieldnames=['rank','path','column','sparkType','classCount']);w.writeheader()
        for i,item in enumerate(model['rankedFields'][:200],1):w.writerow(dict(rank=i,**item))
    weights=['# Relative class frequencies for all concrete official classes (including deprecated/extensions).',
             '# Zero disables a class; omitted classes use dataset.ocsf.defaultEventWeight (1).',
             '# Process Activity is 10 times as likely as Script Activity.']
    for c in model['classes']:
        weights += ['# '+c['caption']+f" [{c['uid']}]"+(' (deprecated)' if c['deprecated'] else ''),
                    'dataset.eventWeight.'+c['name'].replace('/','.')+'='+('10' if c['name']=='process_activity' else '1')]
    (ROOT/'config/ocsf-event-weights.properties').write_text('\n'.join(weights)+'\n')
    print(json.dumps(provenance,indent=2));print('Catalog uncompressed bytes:',len(data))

if __name__=='__main__':main()
