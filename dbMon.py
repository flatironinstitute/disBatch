#!/usr/bin/python3
import curses, json, socket, sys, time

try:
    from queue import Queue
except ImportError:
    from Queue import Queue
from threading import Thread

from kvsstcp import KVSClient

kvsc = KVSClient(sys.argv[1])
uniqueId = sys.argv[2]

# TODO: For the moment, we assume the screen is "big enough".

def waitGetch(stdscr, q):
    while True:
        k = stdscr.getch()
        q.put(('key', k))

def dbStatus(q):
    while True:
        try:
            j = kvsc.view('DisBatch status')
            q.put(('status', j))
            time.sleep(1)
        except:
            q.put(('stop', None))
            break

def popYNC(msg, parent, q, title='Confirm'):
    ph, pw = parent.getmaxyx()
    h = int(ph * .75)
    w = int(pw * .85)
    ro, co = int((ph - h)*.5), int((pw - w)*.5)
    l, msgw = '', []
    for word in msg.split():
        if len(word) > w:
            word = word[:w-3] + '...'
        if len(l) + 1 + len(word) > w:
            msgw.append(l)
            l = word
        else:
            l = l +  (' ' if l else '') + word
    msgw.append(l)
    if len(msgw) > h:
        missing = 1 + len(msgw) - h
        msgw = msgw[:h-1]
        msgw.append('%d lines elided.'%missing)

    nw = curses.newwin(h+2, w+2, ro, co)
    nw.border()
    nw.addstr(0, int((w - len(title))*.5), title)
    for r, l in enumerate(msgw):
        nw.addstr(r+1, 1, l)
    nw.addstr(r+2, int((w - len(title))*.5), '[Y]es/[N]o/[C]ancel', curses.A_REVERSE)
    nw.refresh()

    resp = {'y': 'Y', 'Y':  'Y', 'n': 'N', 'N': 'N', 'c': 'C', 'C': 'C'}
    while True:
        tag, o = q.get()
        if tag != 'key': continue
        k = chr(o)
        if k in resp:
            break

    parent.redrawwin()
    parent.refresh()
    return resp[k]

FirstEngineRow = 3

def statusWindow(stdscr):
    curses.start_color()
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_RED)
    curses.init_pair(4, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(5, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(6, curses.COLOR_BLACK, curses.COLOR_BLACK)
    curses.init_pair(7, curses.COLOR_WHITE, curses.COLOR_WHITE)

    CPCB, CPGB, CPBR, CPYB, CPRB, CPBB, CPWW = [curses.color_pair(x) for x in range(1, 8)]

    curses.curs_set(False)

    stdscr.bkgdset(CPBB)
    stdscr.clear()
    stdscr.refresh()

    q = Queue()
    gc = Thread(target=waitGetch, args=(stdscr, q))
    gc.daemon = True
    gc.start()
    db = Thread(target=dbStatus, args=(q,))
    db.daemon = True
    db.start()

    HelpMe = 'For help, press\'?\''
    
    col, row, done, r2k, statusLine = 0, 0, False, {}, HelpMe
    cursorLimits = None
    localEngineStatus = {}
    statusd, statusj = {}, 'No status data received.'
    while True:
        lRow, lCol = row, col
        tag, o = q.get()

        height, width = stdscr.getmaxyx()

        if tag in ['resize', 'status']:
            now = time.time()
            stdscr.clear()
            try:
                if tag == 'status':
                    statusj = o
                statusd = json.loads(statusj)
                # convert keys back to ints after json transform.
                engines = {int(k): v for k, v in statusd['engines'].items()}
                contexts = {int(k): v for k, v in statusd['contexts'].items()}
                ee = engines.values()
                statusd['slots'] = sum([len(e['cylinders']) for e in ee if e['status'] == 'running'])
                statusd['finished'] = sum([e['finished'] for e in ee])
                statusd['failed'] = sum([e['failed'] for e in ee])
                stdscr.addstr(0, 0, uniqueId + (': {more:15s}   Total slots{slots:4d}   Run{finished:7d}   Failed{failed:5d}   Barriers{barriers:3d}'.format(**statusd)), CPCB)
                #                   '01234 012345678901 01234567890123456789 0123456 0123456 0123456789 0123456789 01234567'
                stdscr.addstr(2, 0, ' Rank    Context           Host          Last    Avail   Assigned   Finished   Failed ', CPCB | curses.A_UNDERLINE)
                r, r2k = FirstEngineRow, {}
                ee = sorted(engines.items())
                for rank, engine in ee:
                    if engine['status'] == 'stopped': continue
                    r2k[r] = rank
                    engine['slots'] = len(engine['cylinders'])
                    engine['delay'] = now - engine['last']
                    engine['cLabel'] = contexts[engine['cRank']]['label']
                    cp = CPGB
                    if engine['status'] == 'stopping':
                        cp = CPRB
                    elif localEngineStatus.get(rank, '') == 'requesting shutdown':
                        cp = CPYB
                    stdscr.addstr(r, 0, '{rank:5d} {cLabel:12.12s} {hostname:20.20s} {delay:7.0f}s {slots:7d} {assigned:10d} {finished:10d} {failed:8d}'.format(**engine), cp)
                    r += 1
                cursorLimits = (FirstEngineRow, r if r == FirstEngineRow else r-1)
            except ValueError as e:
                stdscr.addstr(0, 0, o[:width-3], CPCB)
                print('Exception while processing status:', str(e), file=sys.stderr)
        elif tag == 'key':
            k = o
            if k == curses.KEY_RESIZE:
                q.put(('resize', None))
                continue
            statusLine = HelpMe
            if   k == ord('q'):
                break
            if   k == curses.KEY_DOWN:
                row = row + 1
            elif k == curses.KEY_UP:
                row = row - 1
            elif k == curses.KEY_RIGHT:
                pass
            elif k == curses.KEY_LEFT:
                pass
            elif k in [ord('h'), ord('?')]:
                statusLine = 'C: Shutdown context; E: Shutdown engine; q: quit'
            elif k in [ord('C'), ord('E')]:
                if not done:
                    target = r2k.get(row)
                    if target is not None:
                        if k == ord('C'):
                            cRank = engines[target]['cRank']
                            r = popYNC('Stopping context {cLabel:s} ({cRank:d})'.format(**engines[target]) + ' %d'%row, stdscr, q)
                            if r == 'Y':
                                try:
                                    kvsc.put('.controller', ('stop context', cRank))
                                    for rank, e in engines.items():
                                        if e['cRank'] == cRank:
                                            localEngineStatus[rank] = 'requesting shutdown'
                                except socket.error:
                                    pass
                        elif k == ord('E'):
                            r = popYNC('Stopping engine {rank:d} ({hostname:s}, {pid:d})'.format(**engines[target]), stdscr, q)
                            if r == 'Y':
                                try:
                                    kvsc.put('.controller', ('stop engine', target))
                                    localEngineStatus[target] = 'requesting shutdown'
                                except socket.error:
                                    pass
            else:
                statusLine = 'Last unrecognized key code %d. %s'%(k, HelpMe)
        elif tag == 'stop':
            done = True
            statusLine = 'Run ended.'
        else:
            raise Exception('Unknown tag: '+tag)

        if cursorLimits is not None:
            row = max(cursorLimits[0], row)
            row = min(cursorLimits[1], row)
        else:
            row = FirstEngineRow

        stdscr.move(height-1, 0)
        stdscr.clrtoeol()
        stdscr.addstr(statusLine, CPBR)

        stdscr.chgat(lRow, lCol, 1, CPBB)
        stdscr.chgat(row, col, 1, CPWW)

        # Refresh the screen
        stdscr.refresh()

def main():
    curses.wrapper(statusWindow)

if __name__ == "__main__":
    main()
