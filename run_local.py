from threading import Thread
from src.mock_api import start_server
from src.pipeline import run
s,port=start_server(0); t=Thread(target=s.serve_forever,daemon=True); t.start()
try: print(run(f'http://127.0.0.1:{port}/customers'))
finally: s.shutdown(); s.server_close()
