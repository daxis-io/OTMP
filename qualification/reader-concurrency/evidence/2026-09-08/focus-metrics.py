import argparse, importlib.util, itertools, json, pathlib, random, sys
root=pathlib.Path('/private/tmp/otmp-reader-concurrency')
sys.path.insert(0,str(root/'qualification/reader-scale'))
import run
import concurrency
p=argparse.ArgumentParser();p.add_argument('--out',type=pathlib.Path,required=True);a=p.parse_args();a.out.mkdir(exist_ok=False)
e=pathlib.Path('/private/tmp/otmp-reader-concurrency-evidence')
results={};gates={}
for name,fixture,config,_ in concurrency.cases():
 if name not in ('mixed-broad-small','mixed-homogeneous-2','mixed-homogeneous-4','mixed-homogeneous-8'):continue
 orders=list(itertools.permutations(('baseline','v6','v7')))*3+[('baseline','v6','v7'),('v7','v6','baseline')]
 random.Random(74415).shuffle(orders);run.write_json(a.out/(name+'-order.json'),orders)
 for number,order in enumerate(orders,1):
  for label in order:
   binary=e/('reader_scale-reservation-control' if label=='baseline' and name.endswith('-8') else 'reader_scale-instrumentation' if label=='baseline' else 'reader_scale-candidate-'+label)
   cfg=a.out/(name+'-'+label+'.json');run.write_json(cfg,dict(config,preflight_concurrency=1 if label=='baseline' else 8))
   result=run.run_samples(binary,e/'concurrency-fixtures'/fixture,cfg,a.out/name/label/f'round-{number:04d}',1,300)
   print(name,number,label,result['samples'],flush=True)
 for label in ('baseline','v6','v7'):
  s=run.build_summary((a.out/name/label).glob('round-*/sample-*'));results[name+'/'+label]=s
  gates[name+'/'+label+'/completion']=s['samples']['failed']==0
 b=results[name+'/baseline']['overlapping'];c=results[name+'/v7']['overlapping']
 for key,before in b['query_latency_ms'].items():
  if config['overlapping_survivors'][int(key.split(':')[1])] is not None:
   gates[name+'/'+key+'/small']=concurrency.latency_gate(before['complete_ms'],c['query_latency_ms'].get(key,{}).get('complete_ms',{}),1.1,percentiles=('p95',))
 gates[name+'/throughput']=c['throughput_queries_per_second']['0']['p50'] >= b['throughput_queries_per_second']['0']['p50']*.9
 run.write_json(a.out/'results.json',results);run.write_json(a.out/'gates.json',gates)
raise SystemExit(not all(gates.values()))
