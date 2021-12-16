#!/usr/bin/env python3

import os, sys

dr = os.getenv('DISBATCH_ROOT')
if dr and dr not in sys.path:
    sys.path.append(dr)
        
try:
    import disbatch
except:
    print(f'disBatch environment is incomplete. Check:\n\tDISBATCH_ROOT {dr!r}.', file=sys.stderr)
    sys.exit(1)

dbExec = os.path.join(os.path.dirname(disbatch.__file__), 'disBatch.py')
os.execv(sys.executable, [sys.executable, dbExec] + sys.argv[1:])
