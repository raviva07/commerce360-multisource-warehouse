import shutil, sqlite3
from threading import Thread
from src.mock_api import start_server
from src.pipeline import ROOT, run

def reset():
    for rel in ['data/bronze','data/silver','data/gold','warehouse','state','logs']:
        p=ROOT/rel
        if p.exists(): shutil.rmtree(p)
    for rel in ['warehouse','state','logs']:(ROOT/rel).mkdir()

def with_api():
    s,p=start_server(0); Thread(target=s.serve_forever,daemon=True).start(); return s,f'http://127.0.0.1:{p}/customers'

def test_multisource_pipeline_and_scd2():
    reset(); s,url=with_api()
    try: r=run(url)
    finally: s.shutdown(); s.server_close()
    assert r['increment_orders']==6 and r['fact_rows']==6
    assert r['revenue']==254000.0
    conn=sqlite3.connect(ROOT/'warehouse/commerce360.db')
    assert conn.execute("select count(*) from dim_customer where customer_id='C002'").fetchone()[0]==2
    assert conn.execute("select count(*) from dim_customer where customer_id='C002' and is_current=1 and city='Bengaluru'").fetchone()[0]==1
    conn.close()

def test_checkpoint_makes_second_run_incremental():
    test_multisource_pipeline_and_scd2()
    s,url=with_api()
    try:r=run(url)
    finally:s.shutdown();s.server_close()
    assert r['increment_orders']==0 and r['increment_customers']==0 and r['fact_rows']==6

def test_changed_order_updates_existing_fact():
    import pandas as pd
    test_multisource_pipeline_and_scd2()
    path=ROOT/'data/input/orders.csv'; df=pd.read_csv(path)
    oid=df.loc[0,'order_id']; df.loc[0,'quantity']=2; df.loc[0,'updated_at']='2027-01-01T00:00:00'
    expected=2*float(df.loc[0,'unit_price']); df.to_csv(path,index=False)
    s,url=with_api()
    try:r=run(url)
    finally:s.shutdown();s.server_close()
    assert r['fact_rows']==6 and r['increment_orders']==1
    with sqlite3.connect(ROOT/'warehouse/commerce360.db') as conn:
        assert conn.execute('SELECT revenue FROM fact_order WHERE order_id=?',(oid,)).fetchone()[0]==expected
