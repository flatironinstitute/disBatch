#!/usr/bin/env python3

import os, sys

dr = None
if 'DISBATCH_ROOT' in os.environ:
    dr = os.environ['DISBATCH_ROOT']
    if dr not in sys.path:
        sys.path.append(dr)
        
try:
    import disbatch
except:
    print(f'disBatch environment is incomplete. Check:\n\tDISBATCH_ROOT {dr!r}.', file=sys.stderr)
    sys.exit(1)

dbExec = os.path.join(os.path.sep.join(disbatch.__file__.split(os.path.sep)[:-1]), 'disBatch.py')
os.execv(sys.executable, [sys.executable, dbExec] + sys.argv[1:])
