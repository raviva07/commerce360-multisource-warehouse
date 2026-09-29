from __future__ import annotations
import json, logging, shutil, sqlite3, urllib.request
from pathlib import Path
import pandas as pd
from src.orchestrator import DAGRunner, Task

ROOT=Path(__file__).resolve().parents[1]

def setup_logging():
    (ROOT/'logs').mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',handlers=[logging.FileHandler(ROOT/'logs/pipeline.log',encoding='utf-8'),logging.StreamHandler()])

def load_contract(): return json.loads((ROOT/'data_contract.json').read_text())
def load_state():
    p=ROOT/'state/checkpoints.json'
    return json.loads(p.read_text()) if p.exists() else {'orders_updated_at':None,'customers_updated_at':None}
def save_state(state):
    (ROOT/'state').mkdir(exist_ok=True); (ROOT/'state/checkpoints.json').write_text(json.dumps(state,indent=2))

def require(df,cols,name):
    missing=[c for c in cols if c not in df.columns]
    if missing: raise ValueError(f'{name} missing columns {missing}')
    if df[cols].isna().any().any(): raise ValueError(f'{name} contains null required fields')

def ingest_orders(ctx):
    src=ROOT/'data/input/orders.csv'; (ROOT/'data/bronze').mkdir(parents=True,exist_ok=True); shutil.copy2(src,ROOT/'data/bronze/orders.csv')
    df=pd.read_csv(src); require(df,ctx['contract']['orders_required'],'orders')
    if df['order_id'].duplicated().any(): raise ValueError('duplicate order_id')
    if not set(df['channel']).issubset(ctx['contract']['allowed_channels']): raise ValueError('invalid channel')
    if (df['quantity']<=0).any() or (df['unit_price']<=0).any(): raise ValueError('non-positive order values')
    df['updated_at']=pd.to_datetime(df['updated_at']); cp=ctx['state']['orders_updated_at']
    ctx['orders']=df if not cp else df[df['updated_at']>pd.Timestamp(cp)].copy()
    return len(ctx['orders'])

def ingest_customers(ctx,api_url):
    with urllib.request.urlopen(api_url,timeout=3) as r: payload=r.read()
    (ROOT/'data/bronze').mkdir(parents=True,exist_ok=True); (ROOT/'data/bronze/customers.json').write_bytes(payload)
    df=pd.DataFrame(json.loads(payload)); require(df,ctx['contract']['customer_required'],'customers'); df['updated_at']=pd.to_datetime(df['updated_at'])
    cp=ctx['state']['customers_updated_at']; ctx['customers']=df if not cp else df[df['updated_at']>pd.Timestamp(cp)].copy(); return len(ctx['customers'])

def ingest_products(ctx):
    src=ROOT/'data/input/products.json'; shutil.copy2(src,ROOT/'data/bronze/products.json'); df=pd.DataFrame(json.loads(src.read_text())); require(df,ctx['contract']['product_required'],'products'); ctx['products']=df; return len(df)

def transform(ctx):
    s=ROOT/'data/silver'; s.mkdir(parents=True,exist_ok=True)
    o=ctx['orders'].copy();
    if not o.empty:
        o['order_ts']=pd.to_datetime(o['order_ts']); o['revenue']=(o.quantity*o.unit_price).round(2); o.to_csv(s/'orders_increment.csv',index=False)
    c=ctx['customers'].copy();
    if not c.empty:
        for col in ['customer_name','city','segment']: c[col]=c[col].astype(str).str.strip().str.title()
        c.to_csv(s/'customers_increment.csv',index=False)
    ctx['orders_clean']=o; ctx['customers_clean']=c

def schema(conn):
    conn.executescript('''
    CREATE TABLE IF NOT EXISTS dim_customer(customer_sk INTEGER PRIMARY KEY AUTOINCREMENT,customer_id TEXT,customer_name TEXT,city TEXT,segment TEXT,valid_from TEXT,valid_to TEXT,is_current INTEGER,UNIQUE(customer_id,valid_from));
    CREATE TABLE IF NOT EXISTS dim_product(product_sk INTEGER PRIMARY KEY AUTOINCREMENT,product_id TEXT UNIQUE,product_name TEXT,category TEXT);
    CREATE TABLE IF NOT EXISTS fact_order(order_id TEXT PRIMARY KEY,order_ts TEXT,customer_sk INTEGER,product_sk INTEGER,quantity INTEGER,unit_price REAL,revenue REAL,channel TEXT);
    CREATE TABLE IF NOT EXISTS pipeline_metrics(metric_ts TEXT DEFAULT CURRENT_TIMESTAMP,name TEXT,value REAL);
    ''')

def load_customer_scd2(conn,c):
    for r in c.sort_values('updated_at').itertuples(index=False):
        current=conn.execute('SELECT customer_sk,customer_name,city,segment,valid_from FROM dim_customer WHERE customer_id=? AND is_current=1',(r.customer_id,)).fetchone()
        incoming=(r.customer_name,r.city,r.segment)
        if current and current[1:4]==incoming: continue
        if current: conn.execute('UPDATE dim_customer SET valid_to=?,is_current=0 WHERE customer_sk=?',(r.updated_at.isoformat(),current[0]))
        conn.execute('INSERT OR IGNORE INTO dim_customer(customer_id,customer_name,city,segment,valid_from,valid_to,is_current) VALUES(?,?,?,?,?,?,1)',(r.customer_id,*incoming,r.updated_at.isoformat(),None))

def customer_sk_at(conn,cid,ts):
    row=conn.execute('''SELECT customer_sk FROM dim_customer WHERE customer_id=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?) ORDER BY valid_from DESC LIMIT 1''',(cid,ts,ts)).fetchone()
    if row: return row[0]
    row=conn.execute('SELECT customer_sk FROM dim_customer WHERE customer_id=? ORDER BY valid_from LIMIT 1',(cid,)).fetchone(); return row[0] if row else None

def load(ctx):
    db=ROOT/'warehouse/commerce360.db'; db.parent.mkdir(exist_ok=True); conn=sqlite3.connect(db); schema(conn)
    with conn:
        for r in ctx['products'].itertuples(index=False): conn.execute('INSERT INTO dim_product(product_id,product_name,category) VALUES(?,?,?) ON CONFLICT(product_id) DO UPDATE SET product_name=excluded.product_name,category=excluded.category',(r.product_id,r.product_name,r.category))
        load_customer_scd2(conn,ctx['customers_clean'])
        for r in ctx['orders_clean'].itertuples(index=False):
            ts=pd.Timestamp(r.order_ts).isoformat(); csk=customer_sk_at(conn,r.customer_id,ts); psk=conn.execute('SELECT product_sk FROM dim_product WHERE product_id=?',(r.product_id,)).fetchone()[0]
            if csk is None: raise ValueError(f'No customer dimension for {r.customer_id}')
            conn.execute('INSERT INTO fact_order VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET order_ts=excluded.order_ts,customer_sk=excluded.customer_sk,product_sk=excluded.product_sk,quantity=excluded.quantity,unit_price=excluded.unit_price,revenue=excluded.revenue,channel=excluded.channel',(r.order_id,ts,csk,psk,int(r.quantity),float(r.unit_price),float(r.revenue),r.channel))
        conn.execute('INSERT INTO pipeline_metrics(name,value) VALUES(?,?)',('increment_orders',len(ctx['orders_clean'])))
    ctx['conn']=conn

def publish(ctx):
    g=ROOT/'data/gold'; g.mkdir(parents=True,exist_ok=True); conn=ctx['conn']
    queries={'revenue_by_channel.csv':'SELECT channel,COUNT(*) orders,ROUND(SUM(revenue),2) revenue FROM fact_order GROUP BY channel ORDER BY revenue DESC','revenue_by_city.csv':'SELECT c.city,ROUND(SUM(f.revenue),2) revenue FROM fact_order f JOIN dim_customer c ON f.customer_sk=c.customer_sk GROUP BY c.city ORDER BY revenue DESC','revenue_by_category.csv':'SELECT p.category,ROUND(SUM(f.revenue),2) revenue FROM fact_order f JOIN dim_product p ON f.product_sk=p.product_sk GROUP BY p.category ORDER BY revenue DESC'}
    for n,q in queries.items(): pd.read_sql_query(q,conn).to_csv(g/n,index=False)
    ctx['fact_rows']=conn.execute('SELECT COUNT(*) FROM fact_order').fetchone()[0]; ctx['revenue']=conn.execute('SELECT ROUND(SUM(revenue),2) FROM fact_order').fetchone()[0]

def finalize(ctx,events):
    if not ctx['orders'].empty: ctx['state']['orders_updated_at']=ctx['orders']['updated_at'].max().isoformat()
    if not ctx['customers'].empty: ctx['state']['customers_updated_at']=ctx['customers']['updated_at'].max().isoformat()
    save_state(ctx['state']); (ROOT/'logs/dag_events.json').write_text(json.dumps(events,indent=2));
    lineage={'sources':['orders.csv','customers REST API','products.json'],'bronze':['orders.csv','customers.json','products.json'],'silver':['orders_increment.csv','customers_increment.csv'],'warehouse':['dim_customer(SCD2)','dim_product','fact_order'],'gold':['revenue_by_channel','revenue_by_city','revenue_by_category']}; (ROOT/'logs/lineage.json').write_text(json.dumps(lineage,indent=2))
    ctx['conn'].close()

def run(api_url):
    setup_logging(); ctx={'contract':load_contract(),'state':load_state()}
    runner=DAGRunner([
      Task('ingest_orders',lambda:ingest_orders(ctx)),
      Task('ingest_customers_api',lambda:ingest_customers(ctx,api_url),retries=2),
      Task('ingest_products',lambda:ingest_products(ctx)),
      Task('transform',lambda:transform(ctx),('ingest_orders','ingest_customers_api','ingest_products')),
      Task('load_warehouse',lambda:load(ctx),('transform',)),
      Task('publish_marts',lambda:publish(ctx),('load_warehouse',))])
    events=runner.run(); finalize(ctx,events); return {'increment_orders':len(ctx['orders']),'increment_customers':len(ctx['customers']),'fact_rows':ctx['fact_rows'],'revenue':ctx['revenue'],'dag_tasks':len(events)}
