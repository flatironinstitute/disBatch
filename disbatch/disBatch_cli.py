#!/usr/bin/env python3

try:
    import disBatch
except:
    import os, sys
    pp = os.environ.get('PYTHONPATH', '<Not Set>')
    print(f'disBatch environment is incomplete. Check:\n\tPYTHONPATH {pp!r}.', file=sys.stderr)
    sys.exit(1)

disBatch.main()
