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
    stdscr.clear()
    stdscr.refresh()

    curses.start_color()
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_RED)
    curses.init_pair(4, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(5, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.curs_set(False)

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
    while True:
        lRow, lCol = row, col
        tag, o = q.get()

        height, width = stdscr.getmaxyx()

        if tag == 'status':
            now = time.time()
            stdscr.clear()
            try:
                d = json.loads(o)
                # convert keys back to ints after json transform.
                engines = {int(k): v for k, v in d['engines'].items()}
                contexts = {int(k): v for k, v in d['contexts'].items()}
                ee = engines.values()
                d['slots'] = sum([len(e['cylinders']) for e in ee if e['status'] == 'running'])
                d['finished'] = sum([e['finished'] for e in ee])
                d['failed'] = sum([e['failed'] for e in ee])
                stdscr.addstr(0, 0, uniqueId + (': {more:15s}  Run{finished:7d}  Failed{failed:7d}  Barriers{barriers:4d}  Total slots{slots:4d}'.format(**d)), curses.color_pair(1))
                #                   '01234 012345678901 01234567890123456789 0123456789 01234 012345678 0123456 0123456789 0123456789 0123456789'
                stdscr.addstr(2, 0, ' Rank    Context           Host            PID      Age     Last    Avail   Assigned   Finished    Failed  ', curses.color_pair(1) | curses.A_UNDERLINE)
                r, r2k = FirstEngineRow, {}
                ee = sorted(engines.items())
                for rank, engine in ee:
                    if engine['status'] == 'stopped': continue
                    r2k[r] = rank
                    engine['slots'] = len(engine['cylinders'])
                    engine['delay'] = now - engine['last']
                    engine['cLabel'] = contexts[engine['cRank']]['label']
                    cp = curses.color_pair(2)
                    if engine['status'] == 'stopping':
                        cp = curses.color_pair(5)
                    elif localEngineStatus.get(rank, '') == 'requesting shutdown':
                        cp = curses.color_pair(4)
                    stdscr.addstr(r, 0, '{rank:5d} {cLabel:12.12s} {hostname:20.20s} {pid:10d} {age:5d} {delay:8.1f}s {slots:7d} {assigned:10d} {finished:10d} {failed:10d}'.format(**engine), cp)
                    r += 1
                cursorLimits = (FirstEngineRow, r if r == FirstEngineRow else r-1)
            except ValueError:
                stdscr.addstr(0, 0, o, curses.color_pair(1))
        elif tag == 'key':
            statusLine = HelpMe
            k = o
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
        stdscr.addstr(statusLine, curses.color_pair(3))

        stdscr.chgat(lRow, lCol, 1, curses.A_NORMAL)
        stdscr.chgat(row, col, 1, curses.A_REVERSE)

        # Refresh the screen
        stdscr.refresh()

def main():
    curses.wrapper(statusWindow)

if __name__ == "__main__":
    main()
